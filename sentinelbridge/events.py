import hashlib
import re
from datetime import datetime, timezone
from ipaddress import ip_address
from typing import Optional

from pydantic import BaseModel


class InvalidEvent(ValueError):
    pass


class SecurityEvent(BaseModel):
    schema_version: str = "1.0"
    event_id: str
    source_event_id: str
    timestamp: datetime
    severity: str
    category: str
    action: str
    outcome: str
    actor: Optional[str]
    asset: Optional[str]
    source_ip: Optional[str]
    message: str


SEVERITY = {
    "critical": "critical", "crit": "critical", "high": "high",
    "medium": "medium", "moderate": "medium", "warn": "medium", "warning": "medium",
    "low": "low", "info": "info", "informational": "info",
}
AUTH_VALUE = re.compile(r"(?i)(\b(?:Bearer|Basic|Splunk)\s+)[A-Za-z0-9_./+=~%-]+")
CREDENTIAL = re.compile(
    r'(?i)(\b(?:access[_ -]?token|refresh[_ -]?token|api[_ -]?key|token|password|passwd|'
    r'client[_ -]?secret|secret|credential|authorization|cookie)["\x27]?\s*[:=]\s*)'
    r'(?:"[^"]*"|\x27[^\x27]*\x27|(?:(?:Bearer|Basic|Splunk)\s+)?[^\s,;&]+)'
)
URL_CREDENTIAL = re.compile(r"(https?://)[^\s/@]+@", re.IGNORECASE)
REDACTED = "[REDACTED]"


def redact(value: str, secrets: tuple) -> str:
    value = CREDENTIAL.sub(lambda match: match[1] + REDACTED, value)
    value = AUTH_VALUE.sub(lambda match: match[1] + REDACTED, value)
    for secret in sorted(secrets, key=len, reverse=True):
        if secret:
            value = value.replace(secret, REDACTED)
    return URL_CREDENTIAL.sub(r"\g<1>[REDACTED]@", value)


def normalize(raw: object, secrets: tuple) -> SecurityEvent:
    if not isinstance(raw, dict):
        raise InvalidEvent("Event must be a JSON object")
    source_id = raw.get("id")
    if not isinstance(source_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}", source_id):
        raise InvalidEvent("Event id is missing or invalid")
    try:
        timestamp = datetime.fromisoformat(raw["occurred_at"].replace("Z", "+00:00"))
        if timestamp.utcoffset() is None:
            raise ValueError
        timestamp = timestamp.astimezone(timezone.utc)
    except (KeyError, ValueError, AttributeError, OverflowError):
        raise InvalidEvent("Event timestamp must be ISO 8601 with a timezone") from None

    def clean(name: str, fallback: str = "") -> str:
        value = raw.get(name)
        if not isinstance(value, str):
            return fallback
        return redact(value, secrets).strip()[:1024] or fallback

    # Normalize enums independently, then redact fields that can contain free text.
    level = raw.get("severity")
    severity = SEVERITY.get(level.strip().lower(), "unknown") if isinstance(level, str) else "unknown"
    ip = clean("ip")
    try:
        source_ip = str(ip_address(ip)) if "%" not in ip else None
    except ValueError:
        source_ip = None

    return SecurityEvent(
        event_id=hashlib.sha256(source_id.encode()).hexdigest(),
        source_event_id=redact(source_id, secrets),
        timestamp=timestamp,
        severity=severity,
        category=clean("type", "unknown"),
        action=clean("action", "unknown"),
        outcome=clean("result", "unknown"),
        actor=clean("user") or None,
        asset=clean("device") or None,
        source_ip=source_ip,
        message=clean("message", "Security event"),
    )
