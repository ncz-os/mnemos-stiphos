from __future__ import annotations

import datetime as dt
import json
import re
import sqlite3
import uuid
from pathlib import Path
from typing import Any

import pytest

from mnemos.hive_mind.oracle_repository import (
    ORACLE_SCHEMA_VERSION,
    OracleHiveMindRepository,
    OracleSchemaCompatibilityError,
)
from mnemos.hive_mind.repository import SqliteHiveMindRepository


class _FakeCursor:
    def __init__(self, conn: "_FakeConnection") -> None:
        self.conn = conn
        self._rows: list[tuple[Any, ...]] = []
        self.rowcount = 0

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        self.conn.statements.append((sql, params or {}))
        p = params or {}
        sql_norm = " ".join(sql.lower().split())
        if "from user_tables" in sql_norm:
            self._rows = [(name,) for name in sorted(self.conn.tables)]
        elif "from hive_schema_metadata" in sql_norm:
            if self.conn.schema_version is not None:
                self._rows = [(self.conn.schema_version,)]
        elif "select max_cost_tier, routing_metadata from jobs" in sql_norm:
            if self.conn.routing_existing is not None:
                self._rows = [self.conn.routing_existing]
        elif sql_norm.startswith("select id, submitter_urn") and "from jobs" in sql_norm:
            self._rows = list(self.conn.job_rows)
        elif sql_norm.startswith("update jobs set"):
            self.rowcount = 1 if self.conn.update_succeeds else 0
        elif sql_norm.startswith("insert into hive_messages"):
            self.conn.messages.append(
                {
                    "id": p["id"],
                    "from_urn": p["from_urn"],
                    "to_urn": p["to_urn"],
                    "in_reply_to": p["in_reply_to"],
                    "topic": p["topic"],
                    "payload": json.loads(p["payload"]),
                    "ts": p["ts"],
                }
            )
        elif sql_norm.startswith("insert into hive_events"):
            self.conn.events.append(
                {
                    "ts": p["ts"],
                    "kind": p["kind"],
                    "payload": json.loads(p["payload"]),
                    "agent_urn": p["agent_urn"],
                }
            )
        elif "from hive_cache" in sql_norm:
            cache = self.conn.cache.get(p["cache_key"])
            if cache:
                self._rows = [
                    (
                        json.dumps(cache["result"]),
                        cache["source_job_id"],
                        cache["result_mnemos_id"],
                        cache["hit_count"],
                        cache["cost_saved_usd"],
                        cache["model"],
                        cache["provider"],
                        cache["cached_at"],
                    )
                ]
        elif sql_norm.startswith("merge into hive_cache"):
            existing = self.conn.cache.get(p["cache_key"], {})
            self.conn.cache[p["cache_key"]] = {
                "result": json.loads(p["result_json"]),
                "source_job_id": p["source_job_id"],
                "result_mnemos_id": p["result_mnemos_id"],
                "hit_count": existing.get("hit_count", 0),
                "cost_saved_usd": existing.get("cost_saved_usd", 0),
                "model": p["model"],
                "provider": p["provider"],
                "cached_at": p["cached_at"],
            }
        elif sql_norm.startswith("merge into hive_worker_kind_stats"):
            key = (p["urn"], p["kind"])
            row = self.conn.worker_stats.setdefault(
                key,
                {
                    "success_count": 0,
                    "fail_count": 0,
                    "cancelled_count": 0,
                    "total_tokens_in": 0,
                    "total_tokens_out": 0,
                    "total_cost_usd": 0.0,
                    "total_duration_sec": 0.0,
                    "last_run": None,
                },
            )
            row["success_count"] += p["success_delta"]
            row["fail_count"] += p["fail_delta"]
            row["cancelled_count"] += p["cancelled_delta"]
            row["total_tokens_in"] += p["tokens_in"]
            row["total_tokens_out"] += p["tokens_out"]
            row["total_cost_usd"] += p["cost_usd"]
            row["total_duration_sec"] += p["duration_sec"]
            row["last_run"] = p["last_run"]

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def __iter__(self):
        return iter(self._rows)


class _FakeConnection:
    def __init__(self, *, tables: set[str] | None = None, schema_version: int | None = None) -> None:
        self.messages: list[dict[str, Any]] = []
        self.events: list[dict[str, Any]] = []
        self.cache: dict[str, dict[str, Any]] = {}
        self.worker_stats: dict[tuple[str, str], dict[str, Any]] = {}
        self.statements: list[tuple[str, dict[str, Any]]] = []
        self.commits = 0
        self.tables = tables or set()
        self.schema_version = schema_version
        self.routing_existing: tuple[Any, ...] | None = None
        self.job_rows: list[tuple[Any, ...]] = []
        self.update_succeeds = True

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        return None

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None


class _FakePool:
    def __init__(self, conn: _FakeConnection) -> None:
        self.conn = conn

    def acquire(self) -> _FakeConnection:
        return self.conn


@pytest.mark.asyncio
async def test_messages_events_cache_and_worker_stats_match_sqlite(tmp_path) -> None:
    sqlite_repo = SqliteHiveMindRepository(str(tmp_path / "hive.sqlite3"))
    await sqlite_repo.init()
    oracle_conn = _FakeConnection()
    oracle_repo = OracleHiveMindRepository(user="u", password="p", dsn="d", pool=_FakePool(oracle_conn))

    payload = {"text": "hello", "n": 7}
    ts = 1_779_999_123.25
    sqlite_msg_id = await sqlite_repo.insert_message(
        from_urn="urn:agent:codex:host:1",
        to_urn="urn:agent:worker:host:2",
        in_reply_to=None,
        topic="topic.a",
        payload=payload,
        ts=ts,
    )
    oracle_msg_id = await oracle_repo.insert_message(
        from_urn="urn:agent:codex:host:1",
        to_urn="urn:agent:worker:host:2",
        in_reply_to=None,
        topic="topic.a",
        payload=payload,
        ts=ts,
    )

    uuid.UUID(sqlite_msg_id)
    assert len(uuid.UUID(oracle_msg_id).bytes) == 16
    assert oracle_conn.messages[0] == {
        "id": oracle_msg_id,
        "from_urn": "urn:agent:codex:host:1",
        "to_urn": "urn:agent:worker:host:2",
        "in_reply_to": None,
        "topic": "topic.a",
        "payload": payload,
        "ts": dt.datetime.fromtimestamp(ts, dt.timezone.utc),
    }

    await sqlite_repo.emit_event(kind="message", payload={"urn": "urn:agent:codex:host:1"}, ts=ts)
    await oracle_repo.emit_event(kind="message", payload={"urn": "urn:agent:codex:host:1"}, ts=ts)
    assert oracle_conn.events[0]["kind"] == "message"
    assert oracle_conn.events[0]["agent_urn"] == "urn:agent:codex:host:1"
    assert oracle_conn.events[0]["ts"] == dt.datetime.fromtimestamp(ts, dt.timezone.utc)

    result = {"ok": True, "items": [1, 2, 3]}
    source_job_id = str(uuid.uuid4())
    await sqlite_repo.cache_store(
        cache_key="cache-key",
        result_json=result,
        source_job_id=source_job_id,
        result_mnemos_id="mem-1",
        model="gpt-5.5",
        provider="openai",
        cached_at=ts,
    )
    await oracle_repo.cache_store(
        cache_key="cache-key",
        result_json=result,
        source_job_id=source_job_id,
        result_mnemos_id="mem-1",
        model="gpt-5.5",
        provider="openai",
        cached_at=ts,
    )
    assert await oracle_repo.cache_get("cache-key") == await sqlite_repo.cache_get("cache-key")

    await sqlite_repo.record_worker_kind_stats(
        urn="urn:agent:worker:host:2",
        kind="code-edit",
        success_delta=1,
        fail_delta=2,
        cancelled_delta=3,
        tokens_in=100,
        tokens_out=40,
        cost_usd=0.125,
        duration_sec=9.5,
        last_run=ts,
    )
    await sqlite_repo.record_worker_kind_stats(
        urn="urn:agent:worker:host:2",
        kind="code-edit",
        success_delta=2,
        fail_delta=0,
        cancelled_delta=1,
        tokens_in=50,
        tokens_out=10,
        cost_usd=0.025,
        duration_sec=0.5,
        last_run=ts + 1,
    )
    await oracle_repo.record_worker_kind_stats(
        urn="urn:agent:worker:host:2",
        kind="code-edit",
        success_delta=1,
        fail_delta=2,
        cancelled_delta=3,
        tokens_in=100,
        tokens_out=40,
        cost_usd=0.125,
        duration_sec=9.5,
        last_run=ts,
    )
    await oracle_repo.record_worker_kind_stats(
        urn="urn:agent:worker:host:2",
        kind="code-edit",
        success_delta=2,
        fail_delta=0,
        cancelled_delta=1,
        tokens_in=50,
        tokens_out=10,
        cost_usd=0.025,
        duration_sec=0.5,
        last_run=ts + 1,
    )

    with sqlite3.connect(str(tmp_path / "hive.sqlite3")) as db:
        sqlite_row = db.execute(
            "SELECT success_count, fail_count, cancelled_count, total_tokens_in, "
            "total_tokens_out, total_cost_usd, total_duration_sec, last_run "
            "FROM hive_worker_kind_stats WHERE urn=? AND kind=?",
            ("urn:agent:worker:host:2", "code-edit"),
        ).fetchone()
    oracle_row = oracle_conn.worker_stats[("urn:agent:worker:host:2", "code-edit")]
    assert sqlite_row[:-1] == (
        oracle_row["success_count"],
        oracle_row["fail_count"],
        oracle_row["cancelled_count"],
        oracle_row["total_tokens_in"],
        oracle_row["total_tokens_out"],
        oracle_row["total_cost_usd"],
        oracle_row["total_duration_sec"],
    )
    assert sqlite_row[-1] == oracle_row["last_run"].timestamp()

    sql_text = "\n".join(sql for sql, _ in oracle_conn.statements)
    assert "MERGE INTO hive_cache" in sql_text
    assert "MERGE INTO hive_worker_kind_stats" in sql_text
    assert "hive_events" in sql_text


@pytest.mark.asyncio
async def test_oracle_init_accepts_canonical_versioned_schema() -> None:
    canonical = {
        "AGENTS",
        "JOBS",
        "HIVE_MESSAGES",
        "HIVE_EVENTS",
        "HIVE_CACHE",
        "HIVE_WORKER_KIND_STATS",
        "HIVE_SCHEMA_METADATA",
    }
    conn = _FakeConnection(tables=canonical, schema_version=ORACLE_SCHEMA_VERSION)
    repo = OracleHiveMindRepository(user="u", password="p", dsn="d", pool=_FakePool(conn))

    await repo.init()


@pytest.mark.asyncio
async def test_oracle_init_rejects_deprecated_memory_schema_actionably() -> None:
    conn = _FakeConnection(tables={"MEMORY_AGENTS", "MEMORY_JOBS", "MEMORY_MESSAGES", "MEMORY_EVENTS"})
    repo = OracleHiveMindRepository(user="u", password="p", dsn="d", pool=_FakePool(conn))

    with pytest.raises(OracleSchemaCompatibilityError) as exc_info:
        await repo.init()

    message = str(exc_info.value)
    assert "deprecated Oracle MEMORY_* schema" in message
    assert "AGENTS/JOBS and HIVE_*" in message
    assert "002_memory_to_canonical.sql" in message


@pytest.mark.asyncio
async def test_oracle_routing_update_uses_canonical_jobs_contract() -> None:
    conn = _FakeConnection()
    conn.routing_existing = ("A", json.dumps({"submitter_max_cost_tier_explicit": True}))
    repo = OracleHiveMindRepository(user="u", password="p", dsn="d", pool=_FakePool(conn))
    job_id = str(uuid.uuid4())

    updated = await repo.update_job_routing(
        job_id=job_id,
        routed_at=1_779_999_123.25,
        max_cost_tier="C",
        required_capabilities=["gpu"],
        routing_metadata={"router": "knemon"},
    )

    assert updated is True
    update_sql, params = next(
        (sql, params) for sql, params in conn.statements if "UPDATE jobs SET" in sql
    )
    assert "required_capabilities=:required_capabilities" in update_sql
    assert params["max_cost_tier"] == "A"


@pytest.mark.asyncio
async def test_oracle_queued_insert_accepts_complete_service_contract() -> None:
    conn = _FakeConnection()
    repo = OracleHiveMindRepository(user="u", password="p", dsn="d", pool=_FakePool(conn))

    await repo.insert_job_queued(
        job_id=str(uuid.uuid4()),
        submitter_urn="urn:submitter",
        parent_job_id=None,
        kind="code-edit",
        description="fix it",
        priority=4,
        deadline=None,
        required_capabilities=["code"],
        eligible_kinds=["codex"],
        project="mnemos",
        max_cost_tier="A",
        preferred_providers=["local"],
        preferred_models=["native"],
        mnemos_refs=["mem-1"],
        depends_on=None,
        max_retries=2,
        started_at=1_779_999_123.25,
        routing_metadata={"submitter_max_cost_tier_explicit": True},
    )

    insert_sql, params = next((sql, params) for sql, params in conn.statements if "INSERT INTO jobs" in sql)
    assert "routing_metadata" in insert_sql
    assert json.loads(params["routing_metadata"])["submitter_max_cost_tier_explicit"] is True


@pytest.mark.asyncio
async def test_oracle_direct_claim_is_atomic_and_returns_service_shape() -> None:
    conn = _FakeConnection()
    job_id = uuid.uuid4()
    started = dt.datetime(2026, 5, 28, tzinfo=dt.timezone.utc)
    conn.job_rows = [
        (
            job_id.bytes,
            "urn:submitter",
            None,
            "code-edit",
            "fix it",
            10,
            None,
            json.dumps(["code"]),
            json.dumps(["codex"]),
            "mnemos",
            "A",
            json.dumps(["local"]),
            json.dumps(["native"]),
            None,
            None,
            "queued",
            None,
            None,
            None,
            None,
            None,
            None,
            started,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            0,
            2,
            json.dumps({}),
        )
    ]
    repo = OracleHiveMindRepository(user="u", password="p", dsn="d", pool=_FakePool(conn))

    status, claimed = await repo.claim_job_by_id(
        job_id=str(job_id),
        agent_urn="urn:worker",
        agent_kind="codex",
        agent_caps={"code"},
        agent_runtime="codex",
        agent_model="native",
        agent_provider="local",
        agent_tier="A",
        cost_tier_order=["A", "B", "C"],
        sub_throttled=False,
        now=1_779_999_123.25,
    )

    assert status == "claimed"
    assert claimed is not None
    assert claimed["id"] == str(job_id)
    assert claimed["claimed_by"] == "urn:worker"
    sql_text = "\n".join(sql for sql, _ in conn.statements)
    assert "FOR UPDATE SKIP LOCKED" in sql_text
    assert "claimed_provider=:claimed_provider" in sql_text


def test_oracle_ddl_and_forward_migration_declare_canonical_schema() -> None:
    ddl = (Path(__file__).parents[1] / "mnemos/hive_mind/oracle_ddl.sql").read_text()
    migration = (Path(__file__).parents[1] / "migrations/oracle/002_memory_to_canonical.sql").read_text()

    for table in ("agents", "jobs", "hive_messages", "hive_events", "hive_cache", "hive_worker_kind_stats"):
        assert f"CREATE TABLE {table}" in ddl
    assert "VALUES ('stiphos-oracle', 2)" in ddl
    assert "SELECT agent_urn urn" in migration
    assert "ELSIF column_exists('MEMORY_AGENTS','URN')" in migration
    assert "CAST(NVL(created_at,SYSTIMESTAMP) AS TIMESTAMP WITH TIME ZONE)" in migration
    assert "ELSIF column_exists('MEMORY_JOBS','REQUIRED_CAPABILITIES')" in migration
    assert "MERGE INTO hive_messages" in migration
    assert "MERGE INTO hive_events" in migration
    assert "MERGE INTO hive_scheduled_jobs" in migration
    assert "require_columns('AGENTS'" in migration
    assert "require_columns('JOBS'" in migration
    assert "archive('MEMORY_JOBS','LEGACY_MEMORY_JOBS')" in migration
    assert "MERGE INTO hive_schema_metadata" in migration
    assert migration.index("require_columns('JOBS'") < migration.index("MERGE INTO hive_schema_metadata")


def test_oracle_job_descriptions_remain_lossless_clobs() -> None:
    root = Path(__file__).parents[1]
    ddl = (root / "mnemos/hive_mind/oracle_ddl.sql").read_text()
    migration = (root / "migrations/oracle/002_memory_to_canonical.sql").read_text()

    assert "description           CLOB" in ddl
    assert "kind VARCHAR2(64) NOT NULL, description CLOB" in migration
    assert "DBMS_LOB.SUBSTR(description,4000,1)" not in migration
    assert "Job description length mismatch after migration" in migration


@pytest.mark.asyncio
async def test_fresh_canonical_ddl_produces_schema_accepted_on_init() -> None:
    ddl = (Path(__file__).parents[1] / "mnemos/hive_mind/oracle_ddl.sql").read_text()
    created_tables = {
        match.group(1).upper()
        for match in re.finditer(r"^CREATE TABLE\s+([a-z_]+)", ddl, flags=re.MULTILINE | re.IGNORECASE)
    }
    version_match = re.search(r"VALUES \('stiphos-oracle',\s*(\d+)\)", ddl)
    assert version_match is not None
    conn = _FakeConnection(tables=created_tables, schema_version=int(version_match.group(1)))
    repo = OracleHiveMindRepository(user="u", password="p", dsn="d", pool=_FakePool(conn))

    await repo.init()
