"""Regression test for the SSE persisted-event-id fix (2026-09-14 follow-up
to F02/F03): live-delivered events must carry the SAME durable id the
events table assigns on insert, so a client can resume cleanly with
since_id=N after a reconnect or a queue-full drop instead of silently
losing track of where it left off.
"""
from __future__ import annotations

import asyncio
import uuid

import aiosqlite
import pytest
from fastapi.testclient import TestClient

from mnemos.hive_mind import service
from mnemos.hive_mind.repository import SqliteHiveMindRepository


def _configure(monkeypatch: pytest.MonkeyPatch, tmp_path, name: str) -> str:
    db_path = str(tmp_path / name)
    monkeypatch.setattr(service, "DB_PATH", db_path)
    monkeypatch.setattr(service, "_REPO", SqliteHiveMindRepository(db_path))
    return db_path


def test_live_broadcast_event_carries_the_persisted_events_table_id(
    monkeypatch, tmp_path
) -> None:
    db_path = _configure(monkeypatch, tmp_path, "sse-persisted-id.sqlite3")
    # Drive the app lifespan once so full schema creation (jobs, events, ...)
    # runs the same way it does for every other test in this suite, before
    # we talk to the events table directly.
    with TestClient(service.app):
        pass

    async def run() -> tuple[dict, int]:
        async with aiosqlite.connect(db_path) as db:
            sub_id = str(uuid.uuid4())
            queue: asyncio.Queue = asyncio.Queue(maxsize=8)
            service.EVENT_QUEUE[sub_id] = queue
            try:
                await service.emit_event(db, "agent.online", {"urn": "test:agent:1"})

                delivered = await asyncio.wait_for(queue.get(), timeout=2)

                async with db.execute(
                    "SELECT id FROM events WHERE kind = ? ORDER BY id DESC LIMIT 1",
                    ("agent.online",),
                ) as cur:
                    row = await cur.fetchone()
                persisted_id = row[0]

                return delivered, persisted_id
            finally:
                service.EVENT_QUEUE.pop(sub_id, None)

    delivered, persisted_id = asyncio.run(run())

    assert "id" in delivered, (
        "live-broadcast event is missing an 'id' field entirely — a client "
        "has no way to resume via since_id after a drop or reconnect"
    )
    assert delivered["id"] == persisted_id, (
        f"live event id {delivered['id']!r} does not match the events table's "
        f"own row id {persisted_id!r} for the same event — catch-up (since_id) "
        f"and live delivery would disagree about event identity"
    )
