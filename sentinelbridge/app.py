import asyncio
import logging
import secrets
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles

from .config import Settings
from .pipeline import Connector, UPSTREAMS
from .store import COUNTERS, open_store

STATIC = Path(__file__).parent / "static"
STATUSES = ("running", "succeeded", "failed", "deferred", "page_limit", "interrupted")
BASIC = HTTPBasic(auto_error=False)


async def schedule(connector: Connector):
    while True:
        connector.start("scheduled")
        await asyncio.sleep(connector.config.poll_interval)


def create_app(settings: Optional[Settings] = None, *, transport=None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        config = settings or Settings.from_env()
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        with open_store(config.db_path, config.fingerprint) as store:
            async with httpx.AsyncClient(
                timeout=config.timeout, follow_redirects=False, transport=transport,
            ) as client:
                connector = Connector(config, store, client)
                app.state.connector = connector
                scheduler = asyncio.create_task(schedule(connector)) if config.poll_interval else None
                try:
                    yield
                finally:
                    if scheduler:
                        scheduler.cancel()
                        await asyncio.gather(scheduler, return_exceptions=True)
                    if connector.running:
                        connector.task.cancel()
                        await asyncio.gather(connector.task, return_exceptions=True)

    app = FastAPI(title="SentinelBridge", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.mount("/assets", StaticFiles(directory=STATIC), name="assets")

    async def authorized(
        request: Request, credentials: Optional[HTTPBasicCredentials] = Depends(BASIC),
    ) -> Connector:
        connector = request.app.state.connector
        password = connector.config.dashboard_password
        if password:
            supplied = credentials.password if credentials else ""
            user = credentials.username if credentials else ""
            password_ok = secrets.compare_digest(supplied, password)
            user_ok = secrets.compare_digest(user, "operator")
            if not (password_ok and user_ok):
                raise HTTPException(401, "Authentication required", headers={"WWW-Authenticate": "Basic"})
        return connector

    @app.middleware("http")
    async def response_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"
        )
        return response

    @app.get("/")
    async def dashboard(_=Depends(authorized)):
        return FileResponse(STATIC / "index.html")

    @app.get("/healthz")
    async def health():
        return {"status": "ok"}

    @app.get("/api/status")
    async def status(connector: Connector = Depends(authorized)):
        store = connector.store
        runs = store.runs(1)
        checkpoint = store.get("checkpoint_time")
        now = time.time()
        cooldowns = {upstream: store.cooldown(upstream) for upstream in UPSTREAMS}
        return {
            "mode": "demo" if connector.config.demo else "live",
            "running": connector.running,
            "poll_interval_seconds": connector.config.poll_interval,
            "checkpoint_at": float(checkpoint) if checkpoint else None,
            "cooldowns": {key: value for key, value in cooldowns.items() if value > now},
            "last_run": runs[0] if runs else None,
            "totals": store.totals(),
        }

    @app.post("/api/runs", status_code=202)
    async def start(request: Request, connector: Connector = Depends(authorized)):
        # A cross-origin form cannot add this header, and the API does not allow CORS.
        if request.headers.get("x-sentinelbridge-action") != "ingest":
            raise HTTPException(403, "Set the X-SentinelBridge-Action header to ingest")
        run_id = connector.start("manual")
        if run_id is None:
            raise HTTPException(409, "An ingestion run is already in progress")
        return {"id": run_id, "status": "running"}

    @app.get("/api/runs")
    async def runs(limit: int = Query(10, ge=1, le=100), connector: Connector = Depends(authorized)):
        return {"items": connector.store.runs(limit)}

    @app.get("/api/events")
    async def events(limit: int = Query(50, ge=1, le=200), connector: Connector = Depends(authorized)):
        return {"items": connector.store.events(limit)}

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics(connector: Connector = Depends(authorized)):
        totals = connector.store.totals()
        lines = []
        for counter in COUNTERS:
            metric = f"sentinelbridge_{counter}_total"
            lines.extend((f"# TYPE {metric} counter", f"{metric} {totals[counter]}"))
        lines.append("# TYPE sentinelbridge_runs_total counter")
        lines.extend(
            f'sentinelbridge_runs_total{{status="{status}"}} {totals["runs"].get(status, 0)}'
            for status in STATUSES if status != "running"
        )
        lines.extend((
            "# TYPE sentinelbridge_running gauge", f"sentinelbridge_running {int(connector.running)}",
            "# TYPE sentinelbridge_checkpoint_timestamp_seconds gauge",
            f"sentinelbridge_checkpoint_timestamp_seconds {connector.store.get('checkpoint_time') or 0}",
            "# TYPE sentinelbridge_cooldown_until_seconds gauge",
        ))
        for name in UPSTREAMS:
            until = connector.store.cooldown(name)
            lines.append(f'sentinelbridge_cooldown_until_seconds{{upstream="{name}"}} {until if until > time.time() else 0}')
        return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")

    return app


app = create_app()
