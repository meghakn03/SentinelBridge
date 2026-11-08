import hashlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit


def endpoint(value: str, setting: str) -> str:
    url = urlsplit(value)
    if (
        url.scheme not in {"http", "https"}
        or not url.hostname
        or url.username is not None
        or url.password is not None
        or url.query
        or url.fragment
    ):
        raise ValueError(f"{setting} must be an HTTP(S) URL without credentials, query or fragment")
    return value


def integer(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        raise ValueError(f"{name} must be an integer") from None
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


@dataclass(frozen=True)
class Settings:
    source_url: str
    hec_url: str
    source_token: str = field(repr=False)
    hec_token: str = field(repr=False)
    db_path: Path = Path("data/connector.sqlite3")
    demo: bool = True
    dashboard_password: str = field(default="", repr=False)
    index: str = ""
    page_size: int = 4
    max_pages: int = 100
    max_attempts: int = 4
    max_retry_wait: int = 20
    timeout: int = 10
    poll_interval: int = 0

    @property
    def fingerprint(self) -> str:
        route = "\0".join([str(self.demo), self.source_url, self.hec_url, self.index])
        return hashlib.sha256(route.encode()).hexdigest()

    @classmethod
    def from_env(cls):
        mode = os.getenv("CONNECTOR_MODE", "demo")
        if mode not in {"demo", "live"}:
            raise ValueError("CONNECTOR_MODE must be demo or live")
        demo = mode == "demo"
        source_url = os.getenv("SOURCE_URL", "http://127.0.0.1:8001/events" if demo else "")
        hec_url = os.getenv("SPLUNK_HEC_URL", "http://127.0.0.1:8001/services/collector/event" if demo else "")
        source_token = os.getenv("SOURCE_TOKEN", "demo-source-token" if demo else "")
        hec_token = os.getenv("SPLUNK_HEC_TOKEN", "demo-hec-token" if demo else "")
        password = os.getenv("DASHBOARD_PASSWORD", "")
        if not all((source_token, hec_token)) or (not demo and not password):
            raise ValueError("Set SOURCE_TOKEN, SPLUNK_HEC_TOKEN and, in live mode, DASHBOARD_PASSWORD")
        if not all(token.isascii() and token.isprintable() and not any(c.isspace() for c in token)
                   for token in (source_token, hec_token)):
            raise ValueError("Source and HEC tokens must use printable ASCII without spaces")
        if password and not (password.isascii() and password.isprintable()):
            raise ValueError("DASHBOARD_PASSWORD must use printable ASCII")
        return cls(
            source_url=endpoint(source_url, "SOURCE_URL"),
            hec_url=endpoint(hec_url, "SPLUNK_HEC_URL"),
            source_token=source_token,
            hec_token=hec_token,
            db_path=Path(os.getenv("CONNECTOR_DB", "data/connector.sqlite3")),
            demo=demo,
            dashboard_password=password,
            index=os.getenv("SPLUNK_INDEX", ""),
            page_size=integer("PAGE_SIZE", 4 if demo else 100, 1, 500),
            max_pages=integer("MAX_PAGES_PER_RUN", 100, 1, 1000),
            max_attempts=integer("MAX_ATTEMPTS", 4, 1, 10),
            max_retry_wait=integer("MAX_RETRY_WAIT_SECONDS", 20, 1, 300),
            timeout=integer("REQUEST_TIMEOUT_SECONDS", 10, 1, 120),
            poll_interval=integer("POLL_INTERVAL_SECONDS", 0, 0, 86400),
        )
