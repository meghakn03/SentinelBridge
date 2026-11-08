import json
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import httpx
import pytest
from fastapi.testclient import TestClient

from sentinelbridge.app import create_app
from sentinelbridge.config import Settings
from sentinelbridge.events import normalize
from sentinelbridge.pipeline import retry_after
from sentinelbridge.simulator import create_simulator, seed_events

ACTION = {"X-SentinelBridge-Action": "ingest"}


@pytest.fixture
def settings(tmp_path):
    return Settings(
        source_url="http://simulator/events",
        hec_url="http://simulator/services/collector/event",
        source_token="demo-source-token",
        hec_token="demo-hec-token",
        db_path=tmp_path / "connector.sqlite3",
    )


def start(client):
    response = client.post("/api/runs", headers=ACTION)
    assert response.status_code == 202
    return response.json()["id"]


def finished(client, run_id):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        latest = client.get("/api/status").json()["last_run"]
        if latest["id"] == run_id and latest["status"] != "running":
            return latest
        time.sleep(0.02)
    pytest.fail("Connector run did not finish")


def test_simulated_round_trip_with_faults_and_restart(settings):
    simulator = create_simulator(faults=True)
    transport = httpx.ASGITransport(app=simulator)
    with TestClient(create_app(settings, transport=transport)) as client:
        assert "Connector overview" in client.get("/").text
        assert client.get("/healthz").json() == {"status": "ok"}
        assert client.post("/api/runs").status_code == 403
        run_id = start(client)
        assert client.post("/api/runs", headers=ACTION).status_code == 409
        run = finished(client, run_id)
        assert (run["status"], run["pages"], run["delivered"]) == ("succeeded", 4, 12)
        assert (run["duplicates"], run["retries"], run["rate_limits"]) == (1, 2, 1)
        assert client.get("/api/status").json()["checkpoint_at"] is not None
        assert "sentinelbridge_delivered_total 12" in client.get("/metrics").text

    with TestClient(simulator) as mock:
        payloads = mock.get("/received", headers={"Authorization": "Splunk demo-hec-token"}).json()["events"]
        assert len(payloads) == len({item["event"]["event_id"] for item in payloads}) == 12
        assert all(item["sourcetype"] == "sentinelbridge:security:json" and item["time"] > 0 for item in payloads)
        assert "[REDACTED]" in json.dumps(payloads)
        assert "example-credential" not in json.dumps(payloads)
        assert "fixture-secret-outside-allowlist" not in json.dumps(payloads)

    with TestClient(create_app(settings, transport=transport)) as client:
        run = finished(client, start(client))
        assert (run["status"], run["pages"], run["delivered"]) == ("succeeded", 0, 0)
        assert len(client.get("/api/events").json()["items"]) == 12


def test_partial_delivery_resumes_without_resending_committed_event(settings, caplog):
    raw = seed_events()[:2]
    attempts = []
    requested_cursors = []
    fail = True

    def upstream(request):
        if request.method == "GET":
            cursor = request.url.params.get("cursor")
            requested_cursors.append(cursor)
            return httpx.Response(200, json={"events": [] if cursor else raw, "next_cursor": None if cursor else "p2"})
        event = json.loads(request.content)["event"]
        attempts.append(event["source_event_id"])
        if event["source_event_id"] == "evt-0002" and fail:
            return httpx.Response(403, json={"detail": settings.hec_token})
        return httpx.Response(200, json={"code": 0})

    transport = httpx.MockTransport(upstream)
    with TestClient(create_app(settings, transport=transport)) as client:
        failed = finished(client, start(client))
        assert (failed["status"], failed["delivered"], failed["pages"], failed["retries"]) == ("failed", 1, 0, 0)
        assert failed["error"] == "Splunk HEC returned HTTP 403"
        assert client.get("/api/status").json()["checkpoint_at"] is None

    fail = False
    with TestClient(create_app(settings, transport=transport)) as client:
        succeeded = finished(client, start(client))
        assert (succeeded["status"], succeeded["delivered"], succeeded["duplicates"]) == ("succeeded", 1, 1)
        assert client.get("/api/status").json()["checkpoint_at"] is not None
    assert requested_cursors == [None, None, "p2"]
    assert attempts == ["evt-0001", "evt-0002", "evt-0002"]
    assert settings.hec_token not in caplog.text


def test_server_cooldown_survives_restart(settings):
    calls = 0

    def upstream(_):
        nonlocal calls
        calls += 1
        return httpx.Response(429, headers={"Retry-After": "90"})

    transport = httpx.MockTransport(upstream)
    for _ in range(2):
        with TestClient(create_app(settings, transport=transport)) as client:
            run = finished(client, start(client))
            assert run["status"] == "deferred"
            assert run["retries"] == run["pages"] == 0
            assert client.get("/api/status").json()["cooldowns"]["source"] > time.time()
    assert calls == 1


@pytest.mark.parametrize("reply", [{"code": 6}, {"code": False}, {"code": 0, "ackId": 0}])
def test_hec_rejection_leaves_event_pending(settings, reply):
    def upstream(request):
        if request.method == "GET":
            return httpx.Response(200, json={"events": seed_events()[:1], "next_cursor": "p1"})
        return httpx.Response(200, json=reply)

    with TestClient(create_app(settings, transport=httpx.MockTransport(upstream))) as client:
        run = finished(client, start(client))
        assert (run["status"], run["delivered"], run["pages"]) == ("failed", 0, 0)
        assert client.get("/api/events").json() == {"items": []}


def test_invalid_page_fails_before_delivering_any_event(settings):
    def upstream(request):
        assert request.method == "GET"
        events = seed_events()[:1] + [{"id": "broken", "occurred_at": "no-timezone"}]
        return httpx.Response(200, json={"events": events, "next_cursor": "p2"})

    with TestClient(create_app(settings, transport=httpx.MockTransport(upstream))) as client:
        run = finished(client, start(client))
        assert run["error"] == "Event timestamp must be ISO 8601 with a timezone"
        assert run["delivered"] == run["pages"] == 0


def test_repeated_cursor_does_not_loop(settings):
    def upstream(request):
        assert request.method == "GET"
        return httpx.Response(200, json={"events": [], "next_cursor": "same"})

    with TestClient(create_app(settings, transport=httpx.MockTransport(upstream))) as client:
        run = finished(client, start(client))
        assert run["pages"] == 1
        assert run["error"] == "Source API repeated a continuation cursor"


def test_normalization_scrubs_headers_urls_and_configured_values():
    raw = {
        "id": "security-1",
        "occurred_at": "2026-11-08T09:30:00+02:00",
        "severity": " WARNING ",
        "device": "server-with-sink-secret",
        "ip": "2001:0db8::1",
        "authorization": "raw-field-secret",
        "message": 'Authorization: Bearer jwt-value password="space value" https://user:pass@example.org',
    }
    event = normalize(raw, ("sink-secret",))
    assert event.severity == "medium"
    assert event.timestamp.isoformat() == "2026-11-08T07:30:00+00:00"
    assert event.source_ip == "2001:db8::1"
    assert event.asset == "server-with-[REDACTED]"
    assert event.message == "Authorization: [REDACTED] password=[REDACTED] https://[REDACTED]@example.org"
    assert "raw-field-secret" not in event.model_dump_json()


def test_dashboard_authentication(settings):
    config = replace(settings, demo=False, dashboard_password="local-test-password")
    with TestClient(create_app(config)) as client:
        assert client.get("/healthz").status_code == 200
        for path in ("/", "/api/status", "/api/events", "/api/runs", "/metrics"):
            assert client.get(path).status_code == 401
            assert client.get(path, auth=("operator", config.dashboard_password)).status_code == 200
        assert client.post("/api/runs", headers=ACTION).status_code == 401


def test_retry_after_accepts_http_dates():
    date = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=60), usegmt=True)
    assert 58 <= retry_after(date) <= 60
    assert retry_after("30") == 30
    assert retry_after("malformed") == 0
