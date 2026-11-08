# SentinelBridge

SentinelBridge uses Python 3.12, FastAPI, SQLite, and a vanilla HTML, CSS, and JavaScript dashboard, with Splunk HEC integration, Prometheus metrics, and Docker Compose for local deployment.

The dashboard shows delivery totals, recent events, severity filters, checkpoint timestamps, and run history. Both upstreams have local simulators, so the demo runs without a Splunk account.

## Run the demo

```sh
docker compose up --build
```

Open [the local dashboard](http://127.0.0.1:8000/) and select **Run ingestion**. A fresh demo delivers 12 unique events across four pages, skips one duplicate, and retries twice. The source sends a 429 and the mock HEC sends a 503 on their first eligible requests. Both ask the connector to retry after one second.

Inspect the actual HEC envelopes received by the simulator:

```sh
curl -sS -H 'Authorization: Splunk demo-hec-token' http://127.0.0.1:8001/received
```

The named Compose volume holds connector state. The mock receiver keeps its receipts in memory. Repeating ingestion on an unchanged source returns zero new events. To run another independent demo without touching existing state, choose a different Compose project with `docker compose -p sentinelbridge-replay up --build` after stopping the original services.

### Run with Python

Use Python 3.12 on macOS or Linux, or use the container on other hosts.

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
```

Start the dashboard and simulator together in one terminal:

```sh
.venv/bin/python -m sentinelbridge
```

Open [the local dashboard](http://127.0.0.1:8000/) and select **Run ingestion**. Press **Ctrl+C** to stop both services. Set `SIMULATE_FAILURES=0` before launching to disable injected faults. If the default ports are occupied, append `--port 8080 --simulator-port 8081`. If you change the simulator port after a run, set `CONNECTOR_DB` to a new path because the saved state is tied to the original URLs.

## Send to a Splunk instance

Set the following environment variables before starting the connector. The sample uses the local simulated source with a real HEC destination.

| Variable | Example or purpose |
| --- | --- |
| `CONNECTOR_MODE` | `live` enables the required dashboard password. |
| `SOURCE_URL` | `http://127.0.0.1:8001/events` for the demo source, or an HTTPS source implementing the contract below. |
| `SOURCE_TOKEN` | `demo-source-token` for the demo source. A real source uses its bearer token. |
| `SPLUNK_HEC_URL` | Full HTTPS event URL, such as `https://splunk.example:8088/services/collector/event`. |
| `SPLUNK_HEC_TOKEN` | HEC token allowed to write to the destination index. Use a token with indexer acknowledgement disabled. |
| `SPLUNK_INDEX` | Optional index. Omit it to use the token's default. |
| `DASHBOARD_PASSWORD` | Password supplied by your deployment secret store. The dashboard and monitoring APIs use HTTP Basic with username `operator`. |
| `CONNECTOR_DB` | Dedicated path such as `data/live.sqlite3`. Use persistent local storage. |
| `POLL_INTERVAL_SECONDS` | `0` for manual runs, or a positive interval to poll immediately at startup and then on that interval. |

To start only the connector when the source is already running, use `.venv/bin/python -m uvicorn sentinelbridge.app:app --port 8000 --no-access-log`.

TLS verification uses HTTPX's normal trust settings. For a private CA, supply its bundle through `SSL_CERT_FILE`. Put the dashboard behind TLS when exposing it beyond localhost. Use a single Uvicorn worker per database. The process takes an exclusive lock on the state file's companion lock file.

Request tuning defaults to four attempts, a 10-second timeout, a 20-second maximum inline retry delay, and 100 pages per run. Override with `MAX_ATTEMPTS`, `REQUEST_TIMEOUT_SECONDS`, `MAX_RETRY_WAIT_SECONDS`, and `MAX_PAGES_PER_RUN`. Set `PAGE_SIZE` to change the source page size, which defaults to 4 in demo mode and 100 in live mode.

In Splunk, search for `sourcetype="sentinelbridge:security:json"` in the configured index. If needed, use `| spath` to extract fields from the event JSON. Use an all-time window for the fixed demo fixture dates. See [Splunk HEC event formatting](https://help.splunk.com/en/splunk-cloud-platform/get-started/get-data-in/10.6/get-data-with-http-event-collector/format-events-for-http-event-collector) and [HEC response codes](https://help.splunk.com/en/splunk-cloud-platform/get-started/get-data-in/10.7/get-data-with-http-event-collector/troubleshoot-http-event-collector).

## Source and event contracts

The connector sends `GET SOURCE_URL?limit=N&cursor=...` with bearer authorization. It omits `cursor` on the first request. Each response contains an `events` array and an opaque `next_cursor`. Each data page must advance the cursor. When caught up, return `{"events":[],"next_cursor":null}`. A saved cursor must be reusable across restarts and must resume after the last completed page, including when new events arrive. IDs must be stable across replays and unique within the source.

Every input event requires `id` and timezone-aware ISO 8601 `occurred_at`. The allowlist also reads `severity`, `type`, `action`, `result`, `user`, `device`, `ip`, and `message`. Normalized schema version 1.0 emits `event_id`, `source_event_id`, UTC `timestamp`, `severity`, `category`, `action`, `outcome`, `actor`, `asset`, `source_ip`, and `message`. The HEC envelope adds epoch `time`, `source`, `sourcetype`, and the optional index.

Unknown input fields are dropped. Retained text is scrubbed for configured credential values, credential assignments, authorization schemes, and URL user info. Run history and logs record fixed diagnostic messages and HTTP status codes. Arbitrary unlabelled secrets in source text can evade pattern matching, so review the source fields before sending live data.

## Delivery and recovery

- HTTP 408, 429, 5xx, and transport failures retry with bounded exponential backoff and jitter. The connector honors [`Retry-After` delay seconds and HTTP dates](https://www.rfc-editor.org/rfc/rfc9110.html#section-10.2.3). Longer cooldowns survive restarts and defer the run.
- A page is validated before any of its events are sent. Malformed data, a repeated cursor, or a permanent HEC rejection fails the run. Correct the source event or configuration and rerun to resume from the saved position.
- Each event is recorded in SQLite after HEC returns success code 0. The page cursor advances only after every event on that page is accepted or already present in the delivery ledger. A run reaching the page cap can resume on the next poll or manual run.
- HEC acceptance is the delivery boundary. If HEC accepts an event and the HTTP response or subsequent SQLite commit is lost, replay may send the event again. Use the stable `event_id` for Splunk-side deduplication when needed.
- The SQLite database holds the cursor, cooldowns, run history, and normalized delivery ledger. The database is bound to its source and destination configuration. Back it up together with its state and plan disk capacity for the retained ledger and run history.

## Monitoring and tests

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | Local process liveness. |
| `GET /api/status` | Run state, cumulative totals, checkpoint timestamp, and active cooldowns. |
| `GET /api/events` | Recent accepted events. |
| `GET /api/runs` | Recent run results and safe error messages. |
| `POST /api/runs` | Starts a run with `X-SentinelBridge-Action: ingest`. An overlapping request returns 409. |
| `GET /metrics` | Prometheus counters and gauges for deliveries, retries, rate limits, duplicate skips, checkpoints, run outcomes, and cooldowns. |

```sh
curl -sS -X POST -H 'X-SentinelBridge-Action: ingest' http://127.0.0.1:8000/api/runs
.venv/bin/python -m pytest -q
```
