"""Local HTTP fixtures for the source API and Splunk HEC."""

import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse

FIXTURES = (
    ("info", "authentication", "session_start", "success", "sso-gateway-01", "SSO session established for an enrolled device."),
    ("low", "network", "dns_query", "allowed", "dns-edge-02", "Recursive lookup completed for an approved resolver."),
    ("warning", "identity", "policy_change", "success", "iam-prod-01", "Conditional access policy updated by a service account."),
    ("high", "endpoint", "process_launch", "blocked", "wrk-fin-08", "Unsigned executable launch blocked by endpoint policy."),
    ("low", "cloud", "object_read", "allowed", "storage-eu-02", "Read operation matched the storage access policy."),
    ("critical", "authentication", "login_attempt", "blocked", "vpn-edge-01", "Multiple failed sign-ins from a new network."),
    ("info", "identity", "credential_rotation", "success", "iam-prod-02", "Service credential rotated. api_key=example-credential"),
    ("high", "network", "connection_attempt", "blocked", "fw-edge-03", "Outbound connection to a restricted destination blocked."),
    ("warning", "endpoint", "script_execution", "observed", "wrk-eng-12", "Script executed from a temporary directory."),
    ("low", "cloud", "permission_audit", "success", "cloud-audit-01", "Scheduled review of storage permissions completed."),
    ("high", "cloud", "role_assumption", "denied", "aws-prod-02", "Privileged role assumption denied outside the approved window."),
    ("critical", "endpoint", "malware_detection", "quarantined", "wrk-fin-14", "Endpoint agent quarantined a suspicious PowerShell payload."),
)


def seed_events():
    # Fixed source identifiers and timestamps keep demo replays deterministic.
    start = datetime(2026, 11, 8, 11, 51, tzinfo=timezone.utc)
    events = []
    for number, (severity, category, action, result, device, message) in enumerate(FIXTURES, 1):
        events.append({
            "id": f"evt-{number:04}",
            "occurred_at": (start + timedelta(seconds=number * 21)).isoformat(),
            "severity": severity,
            "type": category,
            "action": action,
            "result": result,
            "user": "svc-audit" if category in {"cloud", "identity"} else f"operator-{number:02}",
            "device": device,
            "ip": f"192.0.2.{number + 10}",
            "message": message,
            "access_token": "fixture-secret-outside-allowlist",
        })
    events.insert(4, events[3].copy())
    return events


def create_simulator(faults: Optional[bool] = None) -> FastAPI:
    app = FastAPI(title="SentinelBridge simulator", docs_url=None, redoc_url=None, openapi_url=None)
    events = seed_events()
    accepted = []
    inject_faults = faults if faults is not None else os.getenv("SIMULATE_FAILURES", "1") == "1"
    rate_limited = False
    hec_failed = False

    @app.get("/healthz")
    async def health():
        return {"status": "ok"}

    @app.get("/events")
    async def page(
        limit: int = Query(4, ge=1, le=500),
        cursor: Optional[str] = Query(None, pattern=r"^p[0-9]{1,6}$"),
        authorization: str = Header(""),
    ):
        nonlocal rate_limited
        if not secrets.compare_digest(authorization.encode(), b"Bearer demo-source-token"):
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
        offset = int(cursor[1:]) if cursor is not None else 0
        if offset > len(events):
            return JSONResponse({"error": "Cursor out of range"}, status_code=400)
        if inject_faults and offset > 0 and not rate_limited:
            rate_limited = True
            return JSONResponse({"error": "Rate limited"}, status_code=429, headers={"Retry-After": "1"})
        items = events[offset:offset + limit]
        return {"events": items, "next_cursor": f"p{offset + len(items)}" if items else None}

    @app.post("/services/collector/event")
    async def hec(request: Request, authorization: str = Header("")):
        nonlocal hec_failed
        if not secrets.compare_digest(authorization.encode(), b"Splunk demo-hec-token"):
            return JSONResponse({"code": 4, "text": "Invalid token"}, status_code=403)
        if inject_faults and not hec_failed:
            hec_failed = True
            return JSONResponse({"code": 9, "text": "Server is busy"}, status_code=503, headers={"Retry-After": "1"})
        try:
            payload = await request.json()
        except ValueError:
            payload = None
        if not isinstance(payload, dict) or not isinstance(payload.get("event"), dict):
            return JSONResponse({"code": 6, "text": "Invalid data format"}, status_code=400)
        accepted.append(payload)
        return {"code": 0, "text": "Success"}

    @app.get("/received")
    async def received(authorization: str = Header("")):
        if not secrets.compare_digest(authorization.encode(), b"Splunk demo-hec-token"):
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
        return {"count": len(accepted), "events": accepted}

    return app


app = create_simulator()
