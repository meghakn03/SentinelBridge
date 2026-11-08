import fcntl
import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from .events import SecurityEvent

COUNTERS = ("pages", "delivered", "duplicates", "retries", "rate_limits")


@contextmanager
def open_store(path: Path, fingerprint: str):
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix(path.suffix + ".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Connector database is already owned by another process") from None
        connection = sqlite3.connect(path)
        try:
            yield Store(connection, fingerprint)
        finally:
            connection.close()


class Store:
    def __init__(self, db: sqlite3.Connection, fingerprint: str):
        self.db = db
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = FULL;
            CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY,
                trigger TEXT NOT NULL,
                status TEXT NOT NULL,
                started REAL NOT NULL,
                finished REAL,
                pages INTEGER NOT NULL DEFAULT 0,
                delivered INTEGER NOT NULL DEFAULT 0,
                duplicates INTEGER NOT NULL DEFAULT 0,
                retries INTEGER NOT NULL DEFAULT 0,
                rate_limits INTEGER NOT NULL DEFAULT 0,
                error TEXT
            );
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                received REAL NOT NULL,
                payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS events_received ON events(received DESC);
        """)
        existing = self.get("route")
        if existing is not None and existing != fingerprint:
            raise RuntimeError("Database belongs to another source or destination. Set CONNECTOR_DB to a new path.")
        with self.db:
            self.set("route", fingerprint)
            self.db.execute(
                "UPDATE runs SET status='interrupted', finished=?, error=? WHERE status='running'",
                (time.time(), "Connector stopped before this run finished"),
            )

    def get(self, key: str):
        row = self.db.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

    def set(self, key: str, value: str):
        self.db.execute("INSERT OR REPLACE INTO state VALUES (?, ?)", (key, value))

    def start(self, trigger: str) -> int:
        with self.db:
            return self.db.execute(
                "INSERT INTO runs (trigger, status, started) VALUES (?, 'running', ?)", (trigger, time.time())
            ).lastrowid

    def finish(self, run_id: int, status: str, error=None):
        with self.db:
            self.db.execute(
                "UPDATE runs SET status=?, finished=?, error=? WHERE id=?", (status, time.time(), error, run_id)
            )

    def increment(self, run_id: int, counter: str):
        if counter not in COUNTERS:
            raise ValueError("Unknown counter")
        with self.db:
            self.db.execute(f"UPDATE runs SET {counter}={counter}+1 WHERE id=?", (run_id,))

    def checkpoint(self, run_id: int, cursor: str):
        with self.db:
            self.set("cursor", cursor)
            self.set("checkpoint_time", str(time.time()))
            self.db.execute("UPDATE runs SET pages=pages+1 WHERE id=?", (run_id,))

    def seen(self, event_id: str) -> bool:
        return self.db.execute("SELECT 1 FROM events WHERE event_id=?", (event_id,)).fetchone() is not None

    def delivered(self, run_id: int, event: SecurityEvent):
        with self.db:
            self.db.execute(
                "INSERT INTO events VALUES (?, ?, ?)", (event.event_id, time.time(), event.model_dump_json())
            )
            self.db.execute("UPDATE runs SET delivered=delivered+1 WHERE id=?", (run_id,))

    def defer(self, upstream: str, until: float):
        with self.db:
            self.set(f"cooldown_{upstream}", str(until))

    def cooldown(self, upstream: str) -> float:
        return float(self.get(f"cooldown_{upstream}") or 0)

    def runs(self, limit: int):
        return [dict(row) for row in self.db.execute("SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,))]

    def events(self, limit: int):
        return [
            json.loads(row["payload"])
            for row in self.db.execute("SELECT payload FROM events ORDER BY received DESC, rowid DESC LIMIT ?", (limit,))
        ]

    def totals(self):
        sums = ", ".join(f"COALESCE(SUM({counter}), 0) AS {counter}" for counter in COUNTERS)
        totals = dict(self.db.execute(f"SELECT {sums} FROM runs").fetchone())
        totals["runs"] = {
            row["status"]: row["total"]
            for row in self.db.execute("SELECT status, COUNT(*) AS total FROM runs GROUP BY status")
        }
        return totals
