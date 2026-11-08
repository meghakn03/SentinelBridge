import asyncio
import logging
import math
import random
import time
from datetime import timezone
from email.utils import parsedate_to_datetime

import httpx

from .config import Settings
from .events import InvalidEvent, SecurityEvent, normalize
from .store import Store

LOG = logging.getLogger("sentinelbridge.pipeline")
UPSTREAMS = {"source": "Source API", "hec": "Splunk HEC"}


class PipelineError(Exception):
    pass


class Deferred(PipelineError):
    pass


def retry_after(value: str) -> float:
    if value.isascii() and value.isdecimal():
        try:
            return min(float(value), 253402300799 - time.time())
        except ValueError:
            return 0
    try:
        date = parsedate_to_datetime(value)
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        return max(0, date.timestamp() - time.time())
    except (ValueError, OverflowError, TypeError):
        return 0


class Connector:
    def __init__(self, config: Settings, store: Store, client: httpx.AsyncClient):
        self.config = config
        self.store = store
        self.client = client
        self.task = None

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    def start(self, trigger: str):
        if self.running:
            return None
        run_id = self.store.start(trigger)
        self.task = asyncio.create_task(self.run(run_id))
        return run_id

    async def run(self, run_id: int):
        LOG.info("run_started id=%s", run_id)
        status, error = "succeeded", None
        try:
            status = await self.ingest(run_id)
        except asyncio.CancelledError:
            status, error = "interrupted", "Connector stopped before this run finished"
            raise
        except Deferred as exc:
            status, error = "deferred", str(exc)
        except (PipelineError, InvalidEvent) as exc:
            status, error = "failed", str(exc)
        except Exception as exc:
            status, error = "failed", "Unexpected connector error"
            LOG.error("run_internal_error id=%s type=%s", run_id, type(exc).__name__)
        finally:
            self.store.finish(run_id, status, error)
            LOG.log(logging.WARNING if error else logging.INFO, "run_finished id=%s status=%s reason=%s",
                    run_id, status, error or "none")

    async def request(self, run_id: int, upstream: str, method: str, url: str, **kwargs):
        name = UPSTREAMS[upstream]
        for attempt in range(self.config.max_attempts):
            remaining = self.store.cooldown(upstream) - time.time()
            if remaining > self.config.max_retry_wait:
                raise Deferred(f"{name} cooldown is active for {math.ceil(remaining)} more seconds")
            if remaining > 0:
                await asyncio.sleep(remaining)
            if attempt:
                self.store.increment(run_id, "retries")
            delay = min(self.config.max_retry_wait, 0.25 * 2 ** attempt + random.uniform(0, 0.2))
            try:
                response = await self.client.request(method, url, **kwargs)
            except (httpx.TransportError, httpx.DecodingError):
                reason = f"{name} transport failure"
            else:
                status = response.status_code
                if 200 <= status < 300:
                    try:
                        body = response.json()
                    except ValueError:
                        raise PipelineError(f"{name} returned invalid JSON") from None
                    if isinstance(body, dict):
                        return body
                    raise PipelineError(f"{name} returned an invalid response")
                reason = f"{name} returned HTTP {status}"
                if status not in {408, 429} and not 500 <= status < 600:
                    raise PipelineError(reason)
                if status == 429:
                    self.store.increment(run_id, "rate_limits")
                hint = retry_after(response.headers.get("Retry-After", ""))
                if hint or status == 429:
                    self.store.defer(upstream, time.time() + max(hint, delay))
                if hint > self.config.max_retry_wait:
                    raise Deferred(f"{name} requested a {math.ceil(hint)} second cooldown")
            if attempt + 1 < self.config.max_attempts and self.store.cooldown(upstream) <= time.time():
                await asyncio.sleep(delay)
        raise PipelineError(f"{reason} after {self.config.max_attempts} attempts")

    async def send(self, run_id: int, event: SecurityEvent):
        payload = {
            "time": event.timestamp.timestamp(),
            "source": "sentinelbridge",
            "sourcetype": "sentinelbridge:security:json",
            "event": event.model_dump(mode="json"),
        }
        if self.config.index:
            payload["index"] = self.config.index
        body = await self.request(
            run_id, "hec", "POST", self.config.hec_url,
            headers={"Authorization": f"Splunk {self.config.hec_token}"}, json=payload,
        )
        if type(body.get("code")) is not int or body["code"] != 0:
            raise PipelineError("Splunk HEC rejected the event or omitted its success code")
        if "ackId" in body or "ackID" in body:
            raise PipelineError("Splunk HEC returned an indexer acknowledgement ID. Use a token with indexer ACK disabled.")

    async def ingest(self, run_id: int) -> str:
        cursor = self.store.get("cursor")
        visited = {cursor}
        secrets = (self.config.source_token, self.config.hec_token, self.config.dashboard_password)
        for _ in range(self.config.max_pages):
            params = {"limit": self.config.page_size}
            if cursor is not None:
                params["cursor"] = cursor
            body = await self.request(
                run_id, "source", "GET", self.config.source_url,
                headers={"Authorization": f"Bearer {self.config.source_token}"}, params=params,
            )
            events, next_cursor = body.get("events"), body.get("next_cursor")
            if not isinstance(events, list) or len(events) > self.config.page_size or "next_cursor" not in body:
                raise PipelineError("Source API returned an invalid page")
            if next_cursor is None and not events:
                return "succeeded"
            if not isinstance(next_cursor, str) or not 1 <= len(next_cursor) <= 2048:
                raise PipelineError("Source API returned an invalid continuation cursor")
            if next_cursor in visited:
                raise PipelineError("Source API repeated a continuation cursor")

            # Validate the full page before sending any events on it.
            for event in [normalize(raw, secrets) for raw in events]:
                if self.store.seen(event.event_id):
                    self.store.increment(run_id, "duplicates")
                    continue
                await self.send(run_id, event)
                self.store.delivered(run_id, event)
            self.store.checkpoint(run_id, next_cursor)
            cursor = next_cursor
            visited.add(cursor)
        return "page_limit"
