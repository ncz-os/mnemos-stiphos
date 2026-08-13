from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any

import pytest

from mnemos.hive_mind import service
from mnemos.hive_mind.oracle_repository import OracleHiveMindRepository
from mnemos.hive_mind.repository import SqliteHiveMindRepository


@pytest.mark.asyncio
async def test_sqlite_claim_pages_past_ineligible_prefix(tmp_path) -> None:
    repo = SqliteHiveMindRepository(str(tmp_path / "hive.sqlite3"), claim_candidate_page_size=2)
    await repo.init()
    now = time.time()
    with sqlite3.connect(repo.db_path) as db:
        for index in range(5):
            db.execute(
                "INSERT INTO jobs "
                "(id, submitter_urn, kind, description, priority, required_capabilities, "
                "status, started_at) VALUES (?, 'submitter', 'work', ?, ?, ?, 'queued', ?)",
                (
                    f"job-{index}",
                    "x" * 100_000,
                    10 - index,
                    json.dumps(["cpu"] if index == 4 else ["gpu"]),
                    now + index,
                ),
            )
        db.commit()

    claimed = await repo.find_and_claim_job(
        agent_urn="urn:agent:test:host:1",
        agent_kind="worker",
        agent_caps={"cpu"},
        agent_runtime="test",
        agent_model="test",
        agent_provider="local",
        agent_tier="A",
        cost_tier_order=["A", "B", "C"],
        sub_throttled=False,
        now=now + 10,
    )

    assert claimed is not None
    assert claimed["id"] == "job-4"
    assert claimed["description"] == "x" * 100_000


@pytest.mark.asyncio
async def test_public_json_lists_are_clamped_and_cost_index_exists(monkeypatch, tmp_path) -> None:
    db_path = str(tmp_path / "hive.sqlite3")
    repo = SqliteHiveMindRepository(db_path)
    await repo.init()
    with sqlite3.connect(db_path) as db:
        db.executescript(service.SCHEMA)
        for index in range(4):
            db.execute(
                "INSERT INTO jobs (id, submitter_urn, kind, description, status, started_at) "
                "VALUES (?, 'submitter', 'work', 'payload', 'queued', ?)",
                (f"job-{index}", float(index)),
            )
            db.execute(
                "INSERT INTO messages (id, from_urn, topic, payload, ts) "
                "VALUES (?, 'sender', 'topic', '{}', ?)",
                (f"message-{index}", float(index)),
            )
            db.execute(
                "INSERT INTO events (ts, kind, payload) VALUES (?, 'event', '{}')",
                (float(index),),
            )
        indexes = {row[1] for row in db.execute("PRAGMA index_list('jobs')")}
        db.commit()

    monkeypatch.setattr(service, "DB_PATH", db_path)
    monkeypatch.setattr(service, "MAX_RESULT_PAGE_SIZE", 2)

    assert (await service.list_jobs(limit=10_000))["count"] == 2
    assert (await service.list_messages(limit=10_000))["count"] == 2
    assert (await service.events_log(limit=10_000))["count"] == 2
    assert "idx_jobs_costs" in indexes


class _DependencyCursor:
    def __init__(self, conn: "_DependencyConnection") -> None:
        self.conn = conn
        self.rows: list[tuple[Any, ...]] = []

    def execute(self, sql: str, params: dict[str, Any]) -> None:
        self.conn.statements.append((sql, params))
        if "SELECT id, status FROM jobs" in sql:
            self.rows = [(raw_id, "done") for raw_id in params.values()]

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


class _DependencyConnection:
    def __init__(self) -> None:
        self.statements: list[tuple[str, dict[str, Any]]] = []

    def cursor(self) -> _DependencyCursor:
        return _DependencyCursor(self)


class _UnusedPool:
    pass


def test_oracle_dependency_eligibility_uses_one_batched_query() -> None:
    repo = OracleHiveMindRepository(user="u", password="p", dsn="d", pool=_UnusedPool())
    conn = _DependencyConnection()
    dependencies = [str(uuid.uuid4()) for _ in range(4)]
    row: list[Any] = [None] * 33
    row[7] = "[]"
    row[10] = "C"
    row[14] = json.dumps(dependencies)
    row[32] = "{}"

    assert repo._service_job_is_claimable(
        conn,
        row=row,
        agent_urn="urn:agent:test:host:1",
        agent_kind="worker",
        agent_caps=set(),
        agent_model="test",
        agent_provider="local",
        agent_tier="A",
        cost_tier_order=["A", "B", "C"],
        sub_throttled=False,
        agent_pools=set(),
    )
    assert len(conn.statements) == 1
    assert len(conn.statements[0][1]) == len(dependencies)
