"""Hive Mind repository contracts and SQLite parity helpers."""

from __future__ import annotations

import asyncio
import json
import os
import random
import sqlite3
import time
import uuid
from typing import Any, Optional, Protocol, runtime_checkable


def _uuid7() -> str:
    try:
        return str(uuid.uuid7())  # type: ignore[attr-defined]
    except AttributeError:
        unix_ms = int(time.time() * 1000) & ((1 << 48) - 1)
        rand_a = random.getrandbits(12)
        rand_b = random.getrandbits(62)
        value = (unix_ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
        return str(uuid.UUID(int=value))


def _attempt_lease_seconds() -> float:
    """STIPHOS heartbeat-lease (2026-09-14): default lease duration in
    seconds. Mirrors ``service.ATTEMPT_LEASE_SECONDS`` but does not import
    ``service`` (which would create a circular import) — both paths read
    the env var independently so they agree."""
    try:
        return float(os.environ.get("HIVE_ATTEMPT_LEASE_SECONDS", "300"))
    except ValueError:
        return 300.0


@runtime_checkable
class HiveMindRepository(Protocol):
    async def init(self) -> None: ...

    async def close(self) -> None: ...

    async def insert_agent(
        self,
        *,
        urn: str,
        kind: str,
        runtime: str,
        model: str,
        provider: str,
        cost_tier: str,
        autonomy_level: str,
        auth_method: str,
        plan_cap_usd: float | None,
        subscription_pools: Optional[list[str]],
        host: str,
        session_id: str,
        pid: int | None,
        capabilities: Optional[list[str]],
        version: Optional[str],
        started_at: float,
        last_heartbeat: float,
        metadata: Optional[dict[str, Any]],
    ) -> None: ...

    async def insert_job_queued(self, **kwargs: Any) -> None: ...

    async def insert_job_cache_hit(self, **kwargs: Any) -> None: ...

    async def update_job_routing(self, *, job_id: str, routed_at: float, **kwargs: Any) -> bool: ...

    async def find_and_claim_job(
        self,
        *,
        agent_urn: str,
        agent_kind: str,
        agent_caps: set[str],
        agent_runtime: str,
        agent_model: str,
        agent_provider: str,
        agent_tier: str,
        cost_tier_order: list[str],
        sub_throttled: bool,
        now: float,
    ) -> Optional[dict[str, Any]]: ...

    async def claim_job_by_id(
        self,
        *,
        job_id: str,
        agent_urn: str,
        agent_kind: str,
        agent_caps: set[str],
        agent_runtime: str,
        agent_model: str,
        agent_provider: str,
        agent_tier: str,
        cost_tier_order: list[str],
        sub_throttled: bool,
        now: float,
    ) -> tuple[str, Optional[dict[str, Any]]]: ...

    async def requeue_job(self, *, job_id: str) -> bool: ...
    async def fetch_jobs_metrics(self) -> dict[str, Any]: ...
    async def fetch_hosts(self) -> list[dict[str, Any]]: ...
    async def fetch_agents_stats(self) -> dict[str, Any]: ...
    async def mark_attempt_reaped(
        self,
        *,
        job_id: str,
        prior_claimer: str,
        reason: str,
        now: float,
    ) -> bool: ...
    async def renew_attempt_lease(
        self,
        *,
        job_id: str,
        claim_token: str,
        now: float,
        extension_seconds: float | None = None,
    ) -> dict[str, Any]: ...
    async def cancel_attempt(
        self,
        *,
        job_id: str,
        claim_token: str,
        now: float,
        reason: str,
    ) -> dict[str, Any]: ...


_SERVICE_JOB_SELECT_COLUMNS = (
    "id, submitter_urn, parent_job_id, kind, description, "
    "priority, deadline, required_capabilities, eligible_kinds, "
    "project, max_cost_tier, preferred_providers, preferred_models, "
    "mnemos_refs, depends_on, status, claimed_by, claimed_at, "
    "claimed_runtime, claimed_model, claimed_provider, "
    "claimed_cost_tier, started_at, retry_backoff_until, ended_at, "
    "result, result_mnemos_id, tokens_in, tokens_out, "
    "estimated_cost_usd, retry_count, max_retries, routing_metadata, "
    "claim_token, attempt_generation"
)

_SERVICE_JOB_CANDIDATE_COLUMNS = (
    "id, required_capabilities, eligible_kinds, max_cost_tier, depends_on, "
    "routing_metadata, priority, started_at"
)


class SqliteHiveMindRepository:
    def __init__(
        self,
        db_path: str,
        *,
        claim_candidate_page_size: int | None = None,
        max_result_page_size: int | None = None,
    ) -> None:
        self.db_path = db_path
        if claim_candidate_page_size is None:
            try:
                claim_candidate_page_size = int(os.environ.get("HIVE_CLAIM_CANDIDATE_PAGE_SIZE", "256"))
            except ValueError:
                claim_candidate_page_size = 256
        self.claim_candidate_page_size = max(1, claim_candidate_page_size)
        if max_result_page_size is None:
            try:
                max_result_page_size = int(os.environ.get("HIVE_MAX_RESULT_PAGE_SIZE", "1000"))
            except ValueError:
                max_result_page_size = 1000
        self.max_result_page_size = max(1, max_result_page_size)

    @staticmethod
    def _json(value: Any) -> str | None:
        if value is None:
            return None
        return json.dumps(value, separators=(",", ":"), default=str)

    async def init(self) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS hive_messages (
                  id TEXT PRIMARY KEY,
                  from_urn TEXT NOT NULL,
                  to_urn TEXT,
                  in_reply_to TEXT,
                  topic TEXT NOT NULL,
                  payload TEXT NOT NULL,
                  ts REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS hive_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts REAL NOT NULL,
                  kind TEXT NOT NULL,
                  payload TEXT NOT NULL,
                  agent_urn TEXT
                );
                CREATE TABLE IF NOT EXISTS hive_cache (
                  cache_key TEXT PRIMARY KEY,
                  result_json TEXT NOT NULL,
                  source_job_id TEXT,
                  result_mnemos_id TEXT,
                  hit_count INTEGER NOT NULL DEFAULT 0,
                  cost_saved_usd REAL NOT NULL DEFAULT 0,
                  model TEXT,
                  provider TEXT,
                  cached_at REAL NOT NULL,
                  last_hit_at REAL
                );
                CREATE TABLE IF NOT EXISTS hive_worker_kind_stats (
                  urn TEXT NOT NULL,
                  kind TEXT NOT NULL,
                  success_count INTEGER NOT NULL DEFAULT 0,
                  fail_count INTEGER NOT NULL DEFAULT 0,
                  cancelled_count INTEGER NOT NULL DEFAULT 0,
                  total_tokens_in INTEGER NOT NULL DEFAULT 0,
                  total_tokens_out INTEGER NOT NULL DEFAULT 0,
                  total_cost_usd REAL NOT NULL DEFAULT 0,
                  total_duration_sec REAL NOT NULL DEFAULT 0,
                  last_run REAL,
                  PRIMARY KEY (urn, kind)
                );
                CREATE TABLE IF NOT EXISTS memory_jobs (
                  id TEXT PRIMARY KEY,
                  status TEXT NOT NULL,
                  priority INTEGER NOT NULL DEFAULT 0,
                  kind TEXT NOT NULL,
                  description TEXT,
                  submitter_urn TEXT NOT NULL,
                  claimed_by TEXT,
                  parent_job_id TEXT,
                  created_at REAL NOT NULL,
                  started_at REAL,
                  ended_at REAL,
                  result TEXT,
                  eligible_kinds TEXT,
                  project TEXT,
                  tags TEXT,
                  retry_count INTEGER NOT NULL DEFAULT 0,
                  max_retries INTEGER NOT NULL DEFAULT 3,
                  CHECK (status IN ('queued','offered','claimed','running',
                                    'done','failed','cancelled'))
                );
                CREATE INDEX IF NOT EXISTS ix_memory_jobs_queue
                  ON memory_jobs(status, priority DESC, created_at ASC);
                CREATE INDEX IF NOT EXISTS ix_memory_jobs_project
                  ON memory_jobs(project);
                CREATE INDEX IF NOT EXISTS ix_memory_jobs_claimed_by
                  ON memory_jobs(claimed_by);
                CREATE TABLE IF NOT EXISTS agents (
                  urn TEXT PRIMARY KEY,
                  kind TEXT NOT NULL,
                  host TEXT NOT NULL,
                  session_id TEXT NOT NULL,
                  pid INTEGER,
                  runtime TEXT,
                  model TEXT,
                  provider TEXT,
                  cost_tier TEXT,
                  autonomy_level TEXT,
                  auth_method TEXT,
                  plan_cap_usd REAL,
                  plan_period_used_usd REAL NOT NULL DEFAULT 0,
                  subscription_pools TEXT,
                  capabilities TEXT,
                  version TEXT,
                  started_at REAL NOT NULL,
                  last_heartbeat REAL NOT NULL,
                  status TEXT NOT NULL CHECK(status IN ('online','idle','offline','error')),
                  metadata TEXT
                );
                CREATE TABLE IF NOT EXISTS agent_credentials (
                  urn TEXT PRIMARY KEY REFERENCES agents(urn) ON DELETE CASCADE,
                  token_hash TEXT NOT NULL UNIQUE,
                  issued_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS jobs (
                  id TEXT PRIMARY KEY,
                  submitter_urn TEXT NOT NULL,
                  parent_job_id TEXT,
                  kind TEXT NOT NULL,
                  description TEXT,
                  priority INTEGER NOT NULL DEFAULT 0,
                  deadline REAL,
                  required_capabilities TEXT,
                  eligible_kinds TEXT,
                  project TEXT,
                  max_cost_tier TEXT NOT NULL DEFAULT 'A',
                  preferred_providers TEXT,
                  preferred_models TEXT,
                  mnemos_refs TEXT,
                  depends_on TEXT,
                  status TEXT NOT NULL CHECK(status IN ('queued','offered','claimed','running','done','failed','cancelled')),
                  claimed_by TEXT,
                  claimed_at REAL,
                  claimed_runtime TEXT,
                  claimed_model TEXT,
                  claimed_provider TEXT,
                  claimed_cost_tier TEXT,
                  started_at REAL NOT NULL,
                  retry_backoff_until REAL,
                  routed_at REAL,
                  routing_metadata TEXT,
                  ended_at REAL,
                  result TEXT,
                  result_mnemos_id TEXT,
                  tokens_in INTEGER,
                  tokens_out INTEGER,
                  estimated_cost_usd REAL,
                  retry_count INTEGER NOT NULL DEFAULT 0,
                  max_retries INTEGER NOT NULL DEFAULT 2,
                  -- F03 (2026-09-14): per-claim fencing token + attempt generation.
                  -- A successful claim mints a fresh UUID token AND bumps attempt_generation
                  -- atomically; the SAME agent (or any other) cannot complete a later claim
                  -- with the stale token from a previous attempt. Reaper -> requeue resets
                  -- attempt_generation to a new value (the new claim bumps it again).
                  claim_token TEXT,
                  attempt_generation INTEGER NOT NULL DEFAULT 0,
                  -- STIPHOS heartbeat-lease (2026-09-14): per-attempt progress lease,
                  -- distinct from the worker's general heartbeat. Set on a successful
                  -- claim to (now + ATTEMPT_LEASE_SECONDS); renewed by the worker
                  -- via POST /v1/jobs/{id}/lease/renew with the live claim_token.
                  -- The reaper treats a lapsed lease as the authoritative reap
                  -- signal — even if the worker process is still heartbeating
                  -- normally. A bounded number of lease expiries is allowed
                  -- before the job is finally marked failed.
                  attempt_lease_deadline REAL,
                  attempt_lease_renewed_at REAL,
                  attempt_lease_renewals INTEGER NOT NULL DEFAULT 0,
                  attempt_lease_max_renewals INTEGER NOT NULL DEFAULT 0,
                  attempt_lease_expired_at REAL,
                  attempt_lease_expiry_count INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_jobs_queue
                  ON jobs(status, priority DESC, started_at ASC);
                CREATE INDEX IF NOT EXISTS idx_jobs_costs
                  ON jobs(ended_at, claimed_provider, claimed_model);
                -- Immutable per-claim audit record. One row per claim attempt (including
                -- reaped/requeued/cancelled attempts), so a stale completion can be
                -- diagnosed after the fact, not just rejected at the CAS boundary.
                -- F03 (2026-09-14).
                CREATE TABLE IF NOT EXISTS job_attempts (
                  attempt_id TEXT PRIMARY KEY,
                  job_id TEXT NOT NULL,
                  attempt_generation INTEGER NOT NULL,
                  claim_token TEXT NOT NULL,
                  claimed_by TEXT,
                  claimed_at REAL,
                  outcome TEXT NOT NULL DEFAULT 'in_flight'
                    CHECK(outcome IN ('in_flight','completed','failed','cancelled','reaped')),
                  outcome_at REAL,
                  result TEXT,
                  released_by TEXT,
                  released_at REAL,
                  release_reason TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_job_attempts_job_id
                  ON job_attempts(job_id, attempt_generation);
                CREATE INDEX IF NOT EXISTS idx_job_attempts_outcome
                  ON job_attempts(outcome);
                CREATE TABLE IF NOT EXISTS worker_kind_stats (
                  urn TEXT NOT NULL,
                  kind TEXT NOT NULL,
                  success_count INTEGER NOT NULL DEFAULT 0,
                  fail_count INTEGER NOT NULL DEFAULT 0,
                  cancelled_count INTEGER NOT NULL DEFAULT 0,
                  total_tokens_in INTEGER NOT NULL DEFAULT 0,
                  total_tokens_out INTEGER NOT NULL DEFAULT 0,
                  total_cost_usd REAL NOT NULL DEFAULT 0,
                  total_duration_sec REAL NOT NULL DEFAULT 0,
                  last_run REAL,
                  PRIMARY KEY (urn, kind)
                );
                """
            )
            db.commit()

    async def close(self) -> None:
        return None

    async def insert_agent(
        self,
        *,
        urn: str,
        kind: str,
        runtime: str,
        model: str,
        provider: str,
        cost_tier: str,
        autonomy_level: str,
        auth_method: str,
        plan_cap_usd: float | None,
        subscription_pools: Optional[list[str]],
        host: str,
        session_id: str,
        pid: int | None,
        capabilities: Optional[list[str]],
        version: Optional[str],
        started_at: float,
        last_heartbeat: float,
        metadata: Optional[dict[str, Any]],
    ) -> None:
        await asyncio.to_thread(
            self._insert_agent_sync,
            urn=urn,
            kind=kind,
            runtime=runtime,
            model=model,
            provider=provider,
            cost_tier=cost_tier,
            autonomy_level=autonomy_level,
            auth_method=auth_method,
            plan_cap_usd=plan_cap_usd,
            subscription_pools=subscription_pools,
            host=host,
            session_id=session_id,
            pid=pid,
            capabilities=capabilities,
            version=version,
            started_at=started_at,
            last_heartbeat=last_heartbeat,
            metadata=metadata,
        )

    def _insert_agent_sync(
        self,
        *,
        urn: str,
        kind: str,
        runtime: str,
        model: str,
        provider: str,
        cost_tier: str,
        autonomy_level: str,
        auth_method: str,
        plan_cap_usd: float | None,
        subscription_pools: Optional[list[str]],
        host: str,
        session_id: str,
        pid: int | None,
        capabilities: Optional[list[str]],
        version: Optional[str],
        started_at: float,
        last_heartbeat: float,
        metadata: Optional[dict[str, Any]],
    ) -> None:
        with sqlite3.connect(self.db_path, timeout=30.0) as db:
            db.execute(
                "INSERT INTO agents "
                "(urn, kind, host, session_id, pid, runtime, model, provider, "
                "cost_tier, autonomy_level, auth_method, plan_cap_usd, "
                "plan_period_used_usd, subscription_pools, capabilities, version, started_at, "
                "last_heartbeat, status, metadata) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "COALESCE((SELECT plan_period_used_usd FROM agents WHERE urn=?), 0), "
                "?, ?, ?, ?, ?, 'online', ?) "
                "ON CONFLICT(urn) DO UPDATE SET "
                "kind=excluded.kind, host=excluded.host, session_id=excluded.session_id, "
                "pid=excluded.pid, runtime=excluded.runtime, model=excluded.model, "
                "provider=excluded.provider, cost_tier=excluded.cost_tier, "
                "autonomy_level=excluded.autonomy_level, auth_method=excluded.auth_method, "
                "plan_cap_usd=excluded.plan_cap_usd, subscription_pools=excluded.subscription_pools, "
                "capabilities=excluded.capabilities, "
                "version=excluded.version, last_heartbeat=excluded.last_heartbeat, "
                "status='online', metadata=excluded.metadata",
                (
                    urn,
                    kind,
                    host,
                    session_id,
                    pid,
                    runtime,
                    model,
                    provider,
                    cost_tier,
                    autonomy_level,
                    auth_method,
                    plan_cap_usd,
                    urn,
                    json.dumps(subscription_pools, separators=(",", ":")) if subscription_pools is not None else None,
                    json.dumps(capabilities, separators=(",", ":")) if capabilities is not None else None,
                    version,
                    float(started_at),
                    float(last_heartbeat),
                    json.dumps(metadata, separators=(",", ":"), default=str) if metadata is not None else None,
                ),
            )
            db.commit()

    async def insert_message(
        self,
        *,
        from_urn: str,
        to_urn: Optional[str],
        in_reply_to: Optional[str],
        topic: str,
        payload: dict[str, Any],
        ts: float,
    ) -> str:
        msg_id = _uuid7()
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                "INSERT INTO hive_messages "
                "(id, from_urn, to_urn, in_reply_to, topic, payload, ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (msg_id, from_urn, to_urn, in_reply_to, topic, json.dumps(payload), ts),
            )
            db.commit()
        return msg_id

    async def insert_job(
        self,
        *,
        job_id: str,
        submitter_urn: str,
        parent_job_id: Optional[str],
        kind: str,
        description: Optional[str],
        priority: int = 0,
        eligible_kinds: Optional[list[str]] = None,
        project: Optional[str] = None,
        tags: Optional[dict[str, Any] | list[str]] = None,
        max_retries: int = 3,
        created_at: Optional[float] = None,
    ) -> None:
        await asyncio.to_thread(
            self._insert_job_sync,
            job_id=job_id,
            submitter_urn=submitter_urn,
            parent_job_id=parent_job_id,
            kind=kind,
            description=description,
            priority=priority,
            eligible_kinds=eligible_kinds,
            project=project,
            tags=tags,
            max_retries=max_retries,
            created_at=created_at,
        )

    async def insert_job_queued(self, **kwargs: Any) -> None:
        await asyncio.to_thread(self._insert_job_queued_sync, **kwargs)

    def _insert_job_queued_sync(self, **kwargs: Any) -> None:
        started = float(kwargs.get("started_at") or kwargs.get("created_at") or time.time())
        with sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None) as db:
            db.execute("PRAGMA busy_timeout=30000")
            db.execute("BEGIN IMMEDIATE")
            try:
                db.execute(
                    "INSERT INTO jobs "
                    "(id, submitter_urn, parent_job_id, kind, description, priority, "
                    "deadline, required_capabilities, eligible_kinds, project, "
                    "max_cost_tier, preferred_providers, preferred_models, mnemos_refs, "
                    "depends_on, status, claimed_by, claimed_at, claimed_runtime, "
                    "claimed_model, claimed_provider, claimed_cost_tier, started_at, "
                    "retry_backoff_until, routed_at, routing_metadata, ended_at, result, result_mnemos_id, "
                    "tokens_in, tokens_out, estimated_cost_usd, retry_count, max_retries) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "'queued', NULL, NULL, NULL, NULL, NULL, NULL, ?, "
                    "NULL, NULL, ?, NULL, NULL, NULL, NULL, NULL, NULL, 0, ?)",
                    (
                        kwargs["job_id"],
                        kwargs["submitter_urn"],
                        kwargs.get("parent_job_id"),
                        kwargs["kind"],
                        kwargs.get("description"),
                        int(kwargs.get("priority") or 0),
                        kwargs.get("deadline"),
                        self._json(kwargs.get("required_capabilities")),
                        self._json(kwargs.get("eligible_kinds")),
                        kwargs.get("project"),
                        (kwargs.get("max_cost_tier") or "A").upper(),
                        self._json(kwargs.get("preferred_providers")),
                        self._json(kwargs.get("preferred_models")),
                        self._json(kwargs.get("mnemos_refs")),
                        self._json(kwargs.get("depends_on")),
                        started,
                        self._json(kwargs.get("routing_metadata")),
                        int(kwargs.get("max_retries") if kwargs.get("max_retries") is not None else 2),
                    ),
                )
                db.commit()
            except Exception:
                db.rollback()
                raise

    async def insert_job_cache_hit(self, **kwargs: Any) -> None:
        await asyncio.to_thread(self._insert_job_cache_hit_sync, **kwargs)

    async def update_job_routing(self, *, job_id: str, routed_at: float, **kwargs: Any) -> bool:
        return await asyncio.to_thread(self._update_job_routing_sync, job_id=job_id, routed_at=routed_at, **kwargs)

    def _update_job_routing_sync(self, *, job_id: str, routed_at: float, **kwargs: Any) -> bool:
        requested_max_cost_tier = kwargs.get("max_cost_tier")
        with sqlite3.connect(self.db_path, timeout=30.0) as db:
            existing = db.execute(
                "SELECT max_cost_tier, routing_metadata FROM jobs WHERE id=? AND status='queued' AND claimed_by IS NULL",
                (job_id,),
            ).fetchone()
            if not existing:
                return False
            existing_metadata = self._loads(existing[1], {}) or {}
            if (
                requested_max_cost_tier is not None
                and isinstance(existing_metadata, dict)
                and existing_metadata.get("submitter_max_cost_tier_explicit")
            ):
                order = ["A", "B", "C"]
                current_tier = str(existing[0] or "A").upper()
                requested_tier = str(requested_max_cost_tier).upper()
                try:
                    if order.index(requested_tier) > order.index(current_tier):
                        requested_max_cost_tier = current_tier
                except ValueError:
                    requested_max_cost_tier = current_tier
        merged_metadata = dict(existing_metadata) if isinstance(existing_metadata, dict) else {}
        merged_metadata.update(kwargs.get("routing_metadata") or {})
        fields = ["routed_at=?", "routing_metadata=?"]
        args: list[Any] = [float(routed_at), self._json(merged_metadata)]
        for column in (
            "required_capabilities",
            "eligible_kinds",
            "preferred_providers",
            "preferred_models",
            "mnemos_refs",
            "depends_on",
        ):
            if column in kwargs and kwargs[column] is not None:
                fields.append(f"{column}=?")
                args.append(self._json(kwargs[column]))
        if requested_max_cost_tier is not None:
            fields.append("max_cost_tier=?")
            args.append(str(requested_max_cost_tier).upper())
        args.append(job_id)
        with sqlite3.connect(self.db_path, timeout=30.0) as db:
            cur = db.execute(
                f"UPDATE jobs SET {', '.join(fields)} WHERE id=? AND status='queued' AND claimed_by IS NULL",
                args,
            )
            db.commit()
            return cur.rowcount == 1

    def _insert_job_cache_hit_sync(self, **kwargs: Any) -> None:
        started = float(kwargs.get("started_at") or time.time())
        ended = float(kwargs.get("ended_at") or started)
        with sqlite3.connect(self.db_path, timeout=30.0) as db:
            db.execute(
                "INSERT INTO jobs "
                "(id, submitter_urn, parent_job_id, kind, description, priority, "
                "deadline, required_capabilities, eligible_kinds, project, "
                "max_cost_tier, preferred_providers, preferred_models, mnemos_refs, "
                "depends_on, status, claimed_by, claimed_at, claimed_runtime, "
                "claimed_model, claimed_provider, claimed_cost_tier, started_at, "
                "retry_backoff_until, ended_at, result, result_mnemos_id, "
                "tokens_in, tokens_out, estimated_cost_usd, retry_count, max_retries) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "'done', NULL, NULL, NULL, ?, ?, NULL, ?, NULL, ?, ?, ?, "
                "NULL, NULL, NULL, 0, ?)",
                (
                    kwargs["job_id"],
                    kwargs["submitter_urn"],
                    kwargs.get("parent_job_id"),
                    kwargs["kind"],
                    kwargs.get("description"),
                    int(kwargs.get("priority") or 0),
                    kwargs.get("deadline"),
                    self._json(kwargs.get("required_capabilities")),
                    self._json(kwargs.get("eligible_kinds")),
                    kwargs.get("project"),
                    (kwargs.get("max_cost_tier") or "A").upper(),
                    self._json(kwargs.get("preferred_providers")),
                    self._json(kwargs.get("preferred_models")),
                    self._json(kwargs.get("mnemos_refs")),
                    self._json(kwargs.get("depends_on")),
                    kwargs.get("model"),
                    kwargs.get("provider"),
                    started,
                    ended,
                    self._json(kwargs.get("result") or {}),
                    kwargs.get("result_mnemos_id"),
                    int(kwargs.get("max_retries") if kwargs.get("max_retries") is not None else 2),
                ),
            )
            db.commit()

    def _insert_job_sync(
        self,
        *,
        job_id: str,
        submitter_urn: str,
        parent_job_id: Optional[str],
        kind: str,
        description: Optional[str],
        priority: int,
        eligible_kinds: Optional[list[str]],
        project: Optional[str],
        tags: Optional[dict[str, Any] | list[str]],
        max_retries: int,
        created_at: Optional[float],
    ) -> None:
        created = float(created_at if created_at is not None else time.time())
        with sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None) as db:
            db.execute("PRAGMA busy_timeout=30000")
            db.execute("BEGIN IMMEDIATE")
            try:
                db.execute(
                    "INSERT INTO memory_jobs "
                    "(id, status, priority, kind, description, submitter_urn, "
                    "claimed_by, parent_job_id, created_at, started_at, ended_at, "
                    "result, eligible_kinds, project, tags, retry_count, max_retries) "
                    "VALUES (?, 'queued', ?, ?, ?, ?, NULL, ?, ?, NULL, NULL, "
                    "NULL, ?, ?, ?, 0, ?)",
                    (
                        job_id,
                        int(priority),
                        kind,
                        description,
                        submitter_urn,
                        parent_job_id,
                        created,
                        json.dumps(eligible_kinds, separators=(",", ":")) if eligible_kinds is not None else None,
                        project,
                        json.dumps(tags, separators=(",", ":"), default=str) if tags is not None else None,
                        int(max_retries),
                    ),
                )
                db.commit()
            except Exception:
                db.rollback()
                raise

    async def claim_next_job(
        self,
        *,
        agent_urn: str,
        agent_kind: str,
        agent_capabilities: Optional[list[str]] = None,
        agent_cost_tier: str = "A",
    ) -> Optional[dict[str, Any]]:
        return await asyncio.to_thread(
            self._claim_next_job_sync,
            agent_urn=agent_urn,
            agent_kind=agent_kind,
        )

    async def find_and_claim_job(
        self,
        *,
        agent_urn: str,
        agent_kind: str,
        agent_caps: set[str],
        agent_runtime: str,
        agent_model: str,
        agent_provider: str,
        agent_tier: str,
        cost_tier_order: list[str],
        sub_throttled: bool,
        now: float,
    ) -> Optional[dict[str, Any]]:
        return await asyncio.to_thread(
            self._find_and_claim_job_sync,
            agent_urn=agent_urn,
            agent_kind=agent_kind,
            agent_caps=set(agent_caps),
            agent_runtime=agent_runtime,
            agent_model=agent_model,
            agent_provider=agent_provider,
            agent_tier=agent_tier,
            cost_tier_order=list(cost_tier_order),
            sub_throttled=sub_throttled,
            now=float(now),
        )

    def _find_and_claim_job_sync(
        self,
        *,
        agent_urn: str,
        agent_kind: str,
        agent_caps: set[str],
        agent_runtime: str,
        agent_model: str,
        agent_provider: str,
        agent_tier: str,
        cost_tier_order: list[str],
        sub_throttled: bool,
        now: float,
    ) -> Optional[dict[str, Any]]:
        with sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None) as db:
            if not self._table_exists(db, "jobs"):
                return self._claim_next_job_sync(agent_urn=agent_urn, agent_kind=agent_kind)
            db.execute("PRAGMA busy_timeout=30000")
            try:
                agent_pools_row = db.execute(
                    "SELECT subscription_pools FROM agents WHERE urn=? AND status IN ('online','idle')",
                    (agent_urn,),
                ).fetchone()
                agent_pools = self._normalized_strings(agent_pools_row[0] if agent_pools_row else None)
                cursor: tuple[int, float, str] | None = None

                while True:
                    where = (
                        "status='queued' AND claimed_by IS NULL "
                        "AND (retry_backoff_until IS NULL OR retry_backoff_until <= ?)"
                    )
                    params: list[Any] = [now]
                    if cursor is not None:
                        priority, started_at, job_id = cursor
                        where += (
                            " AND (priority < ? OR (priority = ? AND "
                            "(started_at > ? OR (started_at = ? AND id > ?))))"
                        )
                        params.extend((priority, priority, started_at, started_at, job_id))
                    params.append(self.claim_candidate_page_size)
                    candidates = db.execute(
                        f"SELECT {_SERVICE_JOB_CANDIDATE_COLUMNS} FROM jobs WHERE {where} "
                        "ORDER BY priority DESC, started_at ASC, id ASC LIMIT ?",
                        params,
                    ).fetchall()
                    if not candidates:
                        return None

                    dependency_ids = {
                        dependency
                        for candidate in candidates
                        for dependency in (self._loads(candidate[4], []) or [])
                    }
                    dependency_statuses: dict[str, str] = {}
                    if dependency_ids:
                        dependency_list = list(dependency_ids)
                        for offset in range(0, len(dependency_list), 500):
                            dependency_batch = dependency_list[offset : offset + 500]
                            placeholders = ",".join("?" for _ in dependency_batch)
                            dependency_statuses.update(
                                db.execute(
                                    f"SELECT id, status FROM jobs WHERE id IN ({placeholders})",
                                    tuple(dependency_batch),
                                ).fetchall()
                            )

                    for candidate in candidates:
                        eligibility_row: list[Any] = [None] * 33
                        for source_index, target_index in enumerate((0, 7, 8, 10, 14, 32)):
                            eligibility_row[target_index] = candidate[source_index]
                        if not self._service_job_is_claimable(
                            db,
                            row=eligibility_row,
                            agent_urn=agent_urn,
                            agent_kind=agent_kind,
                            agent_caps=agent_caps,
                            agent_model=agent_model,
                            agent_provider=agent_provider,
                            agent_tier=agent_tier,
                            cost_tier_order=cost_tier_order,
                            sub_throttled=sub_throttled,
                            agent_pools=agent_pools,
                            dependency_statuses=dependency_statuses,
                        ):
                            continue

                        db.execute("BEGIN IMMEDIATE")
                        current_candidate = db.execute(
                            f"SELECT {_SERVICE_JOB_CANDIDATE_COLUMNS} FROM jobs "
                            "WHERE id=? AND status='queued' AND claimed_by IS NULL "
                            "AND (retry_backoff_until IS NULL OR retry_backoff_until <= ?)",
                            (candidate[0], now),
                        ).fetchone()
                        current_eligibility_row: list[Any] = [None] * 33
                        if current_candidate:
                            for source_index, target_index in enumerate((0, 7, 8, 10, 14, 32)):
                                current_eligibility_row[target_index] = current_candidate[source_index]
                        if not current_candidate or not self._service_job_is_claimable(
                            db,
                            row=current_eligibility_row,
                            agent_urn=agent_urn,
                            agent_kind=agent_kind,
                            agent_caps=agent_caps,
                            agent_model=agent_model,
                            agent_provider=agent_provider,
                            agent_tier=agent_tier,
                            cost_tier_order=cost_tier_order,
                            sub_throttled=sub_throttled,
                        ):
                            db.rollback()
                            continue
                        cur = db.execute(
                            # STIPHOS heartbeat-lease (2026-09-14): set
                            # attempt_lease_deadline = now + lease on every
                            # successful claim. The lease is per-attempt and
                            # distinct from the worker's general heartbeat,
                            # so a hung attempt on a healthy worker is
                            # detectable. renewed_at is set to now (the
                            # initial "renewal" at claim time); renewals is
                            # initialized to 0 and bumps on each explicit
                            # /v1/jobs/{id}/lease/renew call.
                            "UPDATE jobs SET status='claimed', claimed_by=?, claimed_at=?, "
                            "claimed_runtime=?, claimed_model=?, claimed_provider=?, "
                            "claimed_cost_tier=?, claim_token=?, "
                            "attempt_generation = attempt_generation + 1, "
                            "attempt_lease_deadline=?, attempt_lease_renewed_at=?, "
                            "attempt_lease_renewals=0 "
                            "WHERE id=? AND status='queued' AND claimed_by IS NULL",
                            (
                                agent_urn,
                                now,
                                agent_runtime,
                                agent_model,
                                agent_provider,
                                agent_tier,
                                _uuid7(),
                                now + _attempt_lease_seconds(),
                                now,
                                candidate[0],
                            ),
                        )
                        if cur.rowcount != 1:
                            db.rollback()
                            continue
                        claimed_row = db.execute(
                            f"SELECT {_SERVICE_JOB_SELECT_COLUMNS} FROM jobs WHERE id=?",
                            (candidate[0],),
                        ).fetchone()
                        # F03 (2026-09-14): persist the immutable attempt record so
                        # a stale completion from a future reaped/reclaimed attempt
                        # can be diagnosed after the fact, not just rejected.
                        db.execute(
                            "INSERT INTO job_attempts "
                            "(attempt_id, job_id, attempt_generation, claim_token, "
                            "claimed_by, claimed_at, outcome) "
                            "VALUES (?, ?, ?, ?, ?, ?, 'in_flight')",
                            (
                                _uuid7(),
                                claimed_row[0],
                                int(claimed_row[34]),
                                claimed_row[33],
                                claimed_row[16],
                                float(claimed_row[17] or now),
                            ),
                        )
                        db.commit()
                        claimed = self._service_job_from_row(claimed_row)
                        claimed["claimed_resources"] = {
                            "runtime": agent_runtime,
                            "model": agent_model,
                            "provider": agent_provider,
                            "cost_tier": agent_tier,
                        }
                        return claimed

                    last = candidates[-1]
                    cursor = (int(last[6]), float(last[7]), str(last[0]))
            except Exception:
                if db.in_transaction:
                    db.rollback()
                raise

    async def claim_job_by_id(
        self,
        *,
        job_id: str,
        agent_urn: str,
        agent_kind: str,
        agent_caps: set[str],
        agent_runtime: str,
        agent_model: str,
        agent_provider: str,
        agent_tier: str,
        cost_tier_order: list[str],
        sub_throttled: bool,
        now: float,
    ) -> tuple[str, Optional[dict[str, Any]]]:
        return await asyncio.to_thread(
            self._claim_job_by_id_sync,
            job_id=job_id,
            agent_urn=agent_urn,
            agent_kind=agent_kind,
            agent_caps=set(agent_caps),
            agent_runtime=agent_runtime,
            agent_model=agent_model,
            agent_provider=agent_provider,
            agent_tier=agent_tier,
            cost_tier_order=list(cost_tier_order),
            sub_throttled=sub_throttled,
            now=float(now),
        )

    def _claim_job_by_id_sync(
        self,
        *,
        job_id: str,
        agent_urn: str,
        agent_kind: str,
        agent_caps: set[str],
        agent_runtime: str,
        agent_model: str,
        agent_provider: str,
        agent_tier: str,
        cost_tier_order: list[str],
        sub_throttled: bool,
        now: float,
    ) -> tuple[str, Optional[dict[str, Any]]]:
        with sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None) as db:
            if not self._table_exists(db, "jobs"):
                return ("unavailable", None)
            db.execute("PRAGMA busy_timeout=30000")
            db.execute("BEGIN IMMEDIATE")
            try:
                row = db.execute(
                    f"SELECT {_SERVICE_JOB_SELECT_COLUMNS} "
                    "FROM jobs "
                    "WHERE id=? AND status IN ('queued','offered') AND claimed_by IS NULL "
                    "AND (retry_backoff_until IS NULL OR retry_backoff_until <= ?)",
                    (job_id, now),
                ).fetchone()
                if not row:
                    db.rollback()
                    return ("unavailable", None)
                if not self._service_job_is_claimable(
                    db,
                    row=row,
                    agent_urn=agent_urn,
                    agent_kind=agent_kind,
                    agent_caps=agent_caps,
                    agent_model=agent_model,
                    agent_provider=agent_provider,
                    agent_tier=agent_tier,
                    cost_tier_order=cost_tier_order,
                    sub_throttled=sub_throttled,
                ):
                    db.rollback()
                    return ("ineligible", None)
                cur = db.execute(
                    # STIPHOS heartbeat-lease (2026-09-14): set
                    # attempt_lease_deadline on every successful claim
                    # (mirrors the /v1/jobs/next path).
                    "UPDATE jobs SET status='claimed', claimed_by=?, claimed_at=?, "
                    "claimed_runtime=?, claimed_model=?, claimed_provider=?, "
                    "claimed_cost_tier=?, claim_token=?, "
                    "attempt_generation = attempt_generation + 1, "
                    "attempt_lease_deadline=?, attempt_lease_renewed_at=?, "
                    "attempt_lease_renewals=0 "
                    "WHERE id=? AND status IN ('queued','offered') AND claimed_by IS NULL",
                    (
                        agent_urn,
                        now,
                        agent_runtime,
                        agent_model,
                        agent_provider,
                        agent_tier,
                        _uuid7(),
                        now + _attempt_lease_seconds(),
                        now,
                        job_id,
                    ),
                )
                if cur.rowcount != 1:
                    db.rollback()
                    return ("unavailable", None)
                # Re-read the row so we pick up the freshly minted claim_token
                # and bumped attempt_generation from the UPDATE above.
                fresh_row = db.execute(
                    f"SELECT {_SERVICE_JOB_SELECT_COLUMNS} FROM jobs WHERE id=?",
                    (job_id,),
                ).fetchone()
                claimed_row = list(fresh_row)
                # F03 (2026-09-14): persist the immutable attempt record so a
                # stale completion from a future reaped/reclaimed attempt can be
                # diagnosed after the fact, not just rejected at the CAS layer.
                db.execute(
                    "INSERT INTO job_attempts "
                    "(attempt_id, job_id, attempt_generation, claim_token, "
                    "claimed_by, claimed_at, outcome) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'in_flight')",
                    (
                        _uuid7(),
                        job_id,
                        int(claimed_row[34]),
                        claimed_row[33],
                        claimed_row[16],
                        float(claimed_row[17] or now),
                    ),
                )
                db.commit()
                claimed = self._service_job_from_row(claimed_row)
                claimed["claimed_resources"] = {
                    "runtime": agent_runtime,
                    "model": agent_model,
                    "provider": agent_provider,
                    "cost_tier": agent_tier,
                }
                return ("claimed", claimed)
            except Exception:
                db.rollback()
                raise

    @staticmethod
    def _table_exists(db: sqlite3.Connection, table: str) -> bool:
        row = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        return row is not None

    @staticmethod
    def _loads(value: Any, default: Any = None) -> Any:
        if value in (None, ""):
            return default
        return json.loads(value)

    @classmethod
    def _normalized_strings(cls, value: Any) -> set[str]:
        if isinstance(value, str):
            try:
                loaded = cls._loads(value, value)
            except json.JSONDecodeError:
                loaded = value
        else:
            loaded = value
        if not loaded:
            return set()
        if isinstance(loaded, dict):
            items = loaded.keys()
        elif isinstance(loaded, (list, tuple, set)):
            items = loaded
        else:
            items = str(loaded).strip("{}[]").split(",")
        return {
            "".join(ch if ch.isalnum() else "_" for ch in str(item).strip().lower()).strip("_")
            for item in items
            if str(item).strip()
        }

    def _service_job_is_claimable(
        self,
        db: sqlite3.Connection,
        *,
        row: Any,
        agent_urn: str,
        agent_kind: str,
        agent_caps: set[str],
        agent_model: str,
        agent_provider: str,
        agent_tier: str,
        cost_tier_order: list[str],
        sub_throttled: bool,
        agent_pools: set[str] | None = None,
        dependency_statuses: dict[str, str] | None = None,
    ) -> bool:
        required_caps = set(self._loads(row[7], []) or [])
        if required_caps and not required_caps.issubset(agent_caps):
            return False
        eligible_kinds = self._loads(row[8], None)
        if eligible_kinds and agent_kind not in set(eligible_kinds):
            return False
        # Provider/model fields are ordering hints, not hard eligibility gates.
        # Hard execution requirements belong in required_capabilities.
        routing_metadata = self._loads(row[32], {}) if len(row) > 32 else {}
        required_pools = self._normalized_strings(
            routing_metadata.get("required_subscription_pools") if isinstance(routing_metadata, dict) else None
        )
        if required_pools:
            resolved_agent_pools = agent_pools
            if resolved_agent_pools is None:
                agent_pools_row = db.execute(
                    "SELECT subscription_pools FROM agents WHERE urn=? AND status IN ('online','idle')",
                    (agent_urn,),
                ).fetchone()
                resolved_agent_pools = self._normalized_strings(agent_pools_row[0] if agent_pools_row else None)
            if not resolved_agent_pools.intersection(required_pools):
                return False
        if sub_throttled and (row[10] or "A").upper() != "A":
            return False
        try:
            agent_tier_idx = cost_tier_order.index((agent_tier or "C").upper())
            max_tier_idx = cost_tier_order.index((row[10] or "A").upper())
        except ValueError:
            return False
        if agent_tier_idx > max_tier_idx:
            return False
        depends_on = self._loads(row[14], []) or []
        if depends_on:
            if dependency_statuses is not None:
                if any(dependency_statuses.get(dependency) != "done" for dependency in depends_on):
                    return False
            else:
                placeholders = ",".join("?" for _ in depends_on)
                done_count = db.execute(
                    f"SELECT COUNT(*) FROM jobs WHERE id IN ({placeholders}) AND status='done'",
                    tuple(depends_on),
                ).fetchone()[0]
                if int(done_count) != len(depends_on):
                    return False
        return True

    def _claim_next_job_sync(self, *, agent_urn: str, agent_kind: str) -> Optional[dict[str, Any]]:
        with sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None) as db:
            db.execute("PRAGMA busy_timeout=30000")
            db.execute("BEGIN IMMEDIATE")
            try:
                rows = db.execute(
                    "SELECT id, status, priority, kind, description, submitter_urn, "
                    "claimed_by, parent_job_id, created_at, started_at, ended_at, "
                    "result, eligible_kinds, project, tags, retry_count, max_retries "
                    "FROM memory_jobs "
                    "WHERE status='queued' "
                    "ORDER BY priority DESC, created_at ASC"
                ).fetchall()
                for row in rows:
                    eligible = json.loads(row[12]) if row[12] else None
                    if eligible and agent_kind not in eligible:
                        continue
                    claimed_at = time.time()
                    cur = db.execute(
                        "UPDATE memory_jobs "
                        "SET status='claimed', claimed_by=?, started_at=? "
                        "WHERE id=? AND status='queued'",
                        (agent_urn, claimed_at, row[0]),
                    )
                    if cur.rowcount != 1:
                        continue
                    db.commit()
                    claimed_row = (
                        row[0],
                        "claimed",
                        row[2],
                        row[3],
                        row[4],
                        row[5],
                        agent_urn,
                        row[7],
                        row[8],
                        claimed_at,
                        row[10],
                        row[11],
                        row[12],
                        row[13],
                        row[14],
                        row[15],
                        row[16],
                    )
                    return self._job_from_row(claimed_row)
                db.rollback()
                return None
            except Exception:
                db.rollback()
                raise

    async def list_jobs(
        self,
        *,
        status: Optional[str] = None,
        kind: Optional[str] = None,
        project: Optional[str] = None,
        submitter_urn: Optional[str] = None,
        claimed_by: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(
            self._list_jobs_sync,
            status=status,
            kind=kind,
            project=project,
            submitter_urn=submitter_urn,
            claimed_by=claimed_by,
            limit=limit,
        )

    def _list_jobs_sync(
        self,
        *,
        status: Optional[str],
        kind: Optional[str],
        project: Optional[str],
        submitter_urn: Optional[str],
        claimed_by: Optional[str],
        limit: int,
    ) -> list[dict[str, Any]]:
        sql = (
            "SELECT id, status, priority, kind, description, submitter_urn, "
            "claimed_by, parent_job_id, created_at, started_at, ended_at, result, "
            "eligible_kinds, project, tags, retry_count, max_retries "
            "FROM memory_jobs WHERE 1=1"
        )
        params: list[Any] = []
        for name, value in {
            "status": status,
            "kind": kind,
            "project": project,
            "submitter_urn": submitter_urn,
            "claimed_by": claimed_by,
        }.items():
            if value:
                sql += f" AND {name}=?"
                params.append(value)
        sql += " ORDER BY priority DESC, created_at DESC LIMIT ?"
        params.append(max(1, min(int(limit), self.max_result_page_size)))
        with sqlite3.connect(self.db_path, timeout=30.0) as db:
            rows = db.execute(sql, params).fetchall()
        return [self._job_from_row(row) for row in rows]

    @staticmethod
    def _job_from_row(row: Any) -> dict[str, Any]:
        return {
            "id": row[0],
            "status": row[1],
            "priority": row[2],
            "kind": row[3],
            "description": row[4],
            "submitter_urn": row[5],
            "claimed_by": row[6],
            "parent_job_id": row[7],
            "created_at": row[8],
            "queued_at": row[8],
            "started_at": row[9],
            "claimed_at": row[9],
            "ended_at": row[10],
            "result": json.loads(row[11]) if row[11] else None,
            "eligible_kinds": json.loads(row[12]) if row[12] else None,
            "project": row[13],
            "tags": json.loads(row[14]) if row[14] else None,
            "retry_count": row[15],
            "max_retries": row[16],
        }

    @classmethod
    def _service_job_from_row(cls, row: Any) -> dict[str, Any]:
        return {
            "id": row[0],
            "submitter_urn": row[1],
            "parent_job_id": row[2],
            "kind": row[3],
            "description": row[4],
            "priority": row[5],
            "deadline": row[6],
            "required_capabilities": cls._loads(row[7], None),
            "eligible_kinds": cls._loads(row[8], None),
            "project": row[9],
            "max_cost_tier": row[10],
            "preferred_providers": cls._loads(row[11], None),
            "preferred_models": cls._loads(row[12], None),
            "mnemos_refs": cls._loads(row[13], None),
            "depends_on": cls._loads(row[14], None),
            "status": row[15],
            "claimed_by": row[16],
            "claimed_at": row[17],
            "claimed_runtime": row[18],
            "claimed_model": row[19],
            "claimed_provider": row[20],
            "claimed_cost_tier": row[21],
            "started_at": row[22],
            "retry_backoff_until": row[23],
            "ended_at": row[24],
            "result": cls._loads(row[25], None),
            "result_mnemos_id": row[26],
            "tokens_in": row[27],
            "tokens_out": row[28],
            "estimated_cost_usd": row[29],
            "retry_count": row[30],
            "max_retries": row[31],
            "routing_metadata": cls._loads(row[32], {}),
            # F03 (2026-09-14): every successful claim mints a fresh claim_token
            # and bumps attempt_generation. Workers MUST send this token back on
            # every state-changing call (running/done/failed/cancelled) for the
            # life of the attempt; stale tokens from a prior/reaped attempt are
            # rejected at the CAS boundary. See job_attempts table for audit.
            "claim_token": row[33],
            "attempt_generation": row[34],
        }

    async def emit_event(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        agent_urn: Optional[str] = None,
        ts: Optional[float] = None,
    ) -> None:
        event_ts = float(ts if ts is not None else time.time())
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                "INSERT INTO hive_events (ts, kind, payload, agent_urn) " "VALUES (?, ?, ?, ?)",
                (
                    event_ts,
                    kind,
                    json.dumps(payload, separators=(",", ":")),
                    agent_urn or payload.get("urn") or payload.get("agent_urn"),
                ),
            )
            db.commit()

    # ------------------------------------------------------------------
    # F03 (2026-09-14) attempt-fencing helpers
    # ------------------------------------------------------------------
    async def mark_attempt_outcome(
        self,
        *,
        job_id: str,
        attempt_generation: int,
        outcome: str,
        outcome_at: float,
        result: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Stamp a terminal outcome onto the in_flight attempt record for
        (job_id, attempt_generation). Returns True iff exactly one row was
        updated (the row must still be 'in_flight'). Callers MUST pass
        attempt_generation as observed alongside the matching claim_token —
        a stale attempt_generation (e.g. from a reaped attempt) is silently
        ignored, which is correct: a reaped attempt is already closed.
        """
        outcome_norm = str(outcome).lower()
        if outcome_norm not in {"completed", "failed", "cancelled", "reaped"}:
            raise ValueError(f"unsupported attempt outcome: {outcome!r}")
        return await asyncio.to_thread(
            self._mark_attempt_outcome_sync,
            job_id=job_id,
            attempt_generation=int(attempt_generation),
            outcome=outcome_norm,
            outcome_at=float(outcome_at),
            result=result,
        )

    def _mark_attempt_outcome_sync(
        self,
        *,
        job_id: str,
        attempt_generation: int,
        outcome: str,
        outcome_at: float,
        result: Optional[dict[str, Any]],
    ) -> bool:
        result_json = (
            json.dumps(result, separators=(",", ":"), default=str)
            if result is not None
            else None
        )
        with sqlite3.connect(self.db_path, timeout=30.0) as db:
            cur = db.execute(
                "UPDATE job_attempts SET outcome=?, outcome_at=?, result=? "
                "WHERE job_id=? AND attempt_generation=? AND outcome='in_flight'",
                (outcome, float(outcome_at), result_json, job_id, int(attempt_generation)),
            )
            db.commit()
            return cur.rowcount == 1

    async def mark_attempt_reaped(
        self,
        *,
        job_id: str,
        prior_claimer: str,
        reason: str,
        now: float,
    ) -> bool:
        """Close the in_flight attempt for (job_id, current claimed_by=prior_claimer)
        as 'reaped'. Used by the service reaper path. The next successful claim
        mints a new token + bumps attempt_generation; the prior attempt stays
        auditable in the job_attempts log with outcome='reaped'."""
        return await asyncio.to_thread(
            self._mark_attempt_reaped_sync,
            job_id=job_id,
            prior_claimer=prior_claimer,
            reason=reason,
            now=float(now),
        )

    async def renew_attempt_lease(
        self,
        *,
        job_id: str,
        claim_token: str,
        now: float,
        extension_seconds: float | None = None,
    ) -> dict[str, Any]:
        """STIPHOS heartbeat-lease (2026-09-14): explicitly extend the
        per-attempt lease for a claimed/running job.

        Returns a dict describing the outcome:
          - ``{"ok": True, "lease_deadline": float, "renewed_at": float,
               "renewals": int}`` on a successful renewal.
          - ``{"ok": False, "reason": "stale_claim_token"}`` if the
            claim_token does not match the live attempt.
          - ``{"ok": False, "reason": "not_claimable"}`` if the job is not
            in claimed/running state.

        The renewal is atomic: the UPDATE is constrained by the live
        claim_token, so a stale (reaped/reclaimed) token cannot extend the
        lease any more than it could complete the job.
        """
        return await asyncio.to_thread(
            self._renew_attempt_lease_sync,
            job_id=job_id,
            claim_token=claim_token,
            now=float(now),
            extension_seconds=extension_seconds,
        )

    def _renew_attempt_lease_sync(
        self,
        *,
        job_id: str,
        claim_token: str,
        now: float,
        extension_seconds: float | None,
    ) -> dict[str, Any]:
        if extension_seconds is None:
            extension_seconds = _attempt_lease_seconds()
        with sqlite3.connect(self.db_path, timeout=30.0) as db:
            db.execute("PRAGMA busy_timeout=30000")
            row = db.execute(
                "SELECT status, claim_token, attempt_lease_renewals, "
                "attempt_lease_max_renewals, attempt_lease_deadline "
                "FROM jobs WHERE id=?",
                (job_id,),
            ).fetchone()
            if not row:
                return {"ok": False, "reason": "not_found"}
            status, current_token, renewals, max_renewals, current_deadline = row
            if status not in ("claimed", "running"):
                return {"ok": False, "reason": "not_claimable"}
            if current_token is None or current_token != claim_token:
                # Same fencing rule as completion: stale token rejected.
                return {"ok": False, "reason": "stale_claim_token"}
            # Optional bounded renewals — if attempt_lease_max_renewals is
            # set (>0), refuse to renew past the cap. The default cap is
            # 0 (unbounded) for backwards compatibility with workers that
            # don't set it; tests and configs that want a hard cap can
            # set HIVE_ATTEMPT_LEASE_MAX_RENEWALS via the env.
            if max_renewals and renewals is not None and renewals >= max_renewals:
                return {"ok": False, "reason": "renewal_cap_reached"}
            new_deadline = now + float(extension_seconds)
            cur = db.execute(
                "UPDATE jobs SET attempt_lease_deadline=?, "
                "attempt_lease_renewed_at=?, "
                "attempt_lease_renewals = COALESCE(attempt_lease_renewals, 0) + 1 "
                "WHERE id=? AND status IN ('claimed','running') "
                "AND claim_token=?",
                (new_deadline, now, job_id, claim_token),
            )
            db.commit()
            if cur.rowcount != 1:
                # Lost the CAS race — token mismatched between SELECT and UPDATE.
                return {"ok": False, "reason": "stale_claim_token"}
            return {
                "ok": True,
                "lease_deadline": float(new_deadline),
                "renewed_at": float(now),
                "renewals": int((renewals or 0) + 1),
                "previous_deadline": float(current_deadline) if current_deadline is not None else None,
            }

    async def cancel_attempt(
        self,
        *,
        job_id: str,
        claim_token: str,
        now: float,
        reason: str,
    ) -> dict[str, Any]:
        """STIPHOS heartbeat-lease cancellation-acknowledgment (2026-09-14):
        the OWNER of an in-flight attempt can acknowledge cancellation —
        the worker actively told the hive "I am stopping this attempt, do
        not count it as a hung-lease reap". This is the cancellation
        acknowledgment path; it persists the audit record with outcome
        'cancelled' and requeues the job for a fresh claim, rather than
        the default path where the worker simply stops responding and the
        reaper expires the lease.

        Returns ``{"ok": True, "requeued": True}`` on success.
        Returns ``{"ok": False, "reason": "stale_claim_token"}`` if the
        token doesn't match the live attempt.
        """
        return await asyncio.to_thread(
            self._cancel_attempt_sync,
            job_id=job_id,
            claim_token=claim_token,
            now=float(now),
            reason=reason,
        )

    def _cancel_attempt_sync(
        self,
        *,
        job_id: str,
        claim_token: str,
        now: float,
        reason: str,
    ) -> dict[str, Any]:
        with sqlite3.connect(self.db_path, timeout=30.0) as db:
            db.execute("PRAGMA busy_timeout=30000")
            row = db.execute(
                "SELECT claimed_by, status FROM jobs WHERE id=?",
                (job_id,),
            ).fetchone()
            if not row:
                return {"ok": False, "reason": "not_found"}
            claimed_by, status = row
            if status not in ("claimed", "running"):
                return {"ok": False, "reason": "not_claimable"}
            cur = db.execute(
                # F03 (2026-09-14): bump attempt_generation so any in-flight
                # token the worker is still holding is structurally stale.
                # STIPHOS (2026-09-14): clear the lease so the next claim
                # gets a fresh lease deadline.
                "UPDATE jobs SET status='queued', "
                "claimed_by=NULL, claimed_at=NULL, "
                "claimed_runtime=NULL, claimed_model=NULL, "
                "claimed_provider=NULL, claimed_cost_tier=NULL, "
                "claim_token=NULL, attempt_generation = attempt_generation + 1, "
                "attempt_lease_deadline=NULL, attempt_lease_renewed_at=NULL "
                "WHERE id=? AND claim_token=? AND status IN ('claimed','running')",
                (job_id, claim_token),
            )
            if cur.rowcount != 1:
                return {"ok": False, "reason": "stale_claim_token"}
            db.execute(
                "UPDATE job_attempts SET outcome='cancelled', outcome_at=?, "
                "release_reason=? WHERE job_id=? AND claimed_by=? AND outcome='in_flight'",
                (float(now), reason, job_id, claimed_by),
            )
            db.commit()
            return {"ok": True, "requeued": True}

    def _mark_attempt_reaped_sync(
        self,
        *,
        job_id: str,
        prior_claimer: str,
        reason: str,
        now: float,
    ) -> bool:
        with sqlite3.connect(self.db_path, timeout=30.0) as db:
            cur = db.execute(
                "UPDATE job_attempts SET outcome='reaped', outcome_at=?, "
                "released_by=?, released_at=?, release_reason=? "
                "WHERE job_id=? AND claimed_by=? AND outcome='in_flight'",
                (
                    float(now),
                    prior_claimer,
                    float(now),
                    reason,
                    job_id,
                    prior_claimer,
                ),
            )
            db.commit()
            return cur.rowcount >= 1

    async def fetch_attempt_audit(
        self,
        *,
        job_id: str,
    ) -> list[dict[str, Any]]:
        """Return the immutable attempt history for a job, ordered by generation."""
        return await asyncio.to_thread(self._fetch_attempt_audit_sync, job_id=job_id)

    def _fetch_attempt_audit_sync(self, *, job_id: str) -> list[dict[str, Any]]:
        with sqlite3.connect(self.db_path) as db:
            rows = db.execute(
                "SELECT attempt_id, job_id, attempt_generation, claim_token, "
                "claimed_by, claimed_at, outcome, outcome_at, result, "
                "released_by, released_at, release_reason "
                "FROM job_attempts WHERE job_id=? "
                "ORDER BY attempt_generation ASC",
                (job_id,),
            ).fetchall()
        history: list[dict[str, Any]] = []
        for row in rows:
            history.append(
                {
                    "attempt_id": row[0],
                    "job_id": row[1],
                    "attempt_generation": row[2],
                    "claim_token": row[3],
                    "claimed_by": row[4],
                    "claimed_at": row[5],
                    "outcome": row[6],
                    "outcome_at": row[7],
                    "result": self._loads(row[8], None),
                    "released_by": row[9],
                    "released_at": row[10],
                    "release_reason": row[11],
                }
            )
        return history

    async def cache_get(self, cache_key: str) -> Optional[dict[str, Any]]:
        with sqlite3.connect(self.db_path) as db:
            row = db.execute(
                "SELECT result_json, source_job_id, result_mnemos_id, "
                "hit_count, cost_saved_usd, model, provider, cached_at "
                "FROM hive_cache WHERE cache_key=?",
                (cache_key,),
            ).fetchone()
        if not row:
            return None
        return {
            "result": json.loads(row[0]),
            "source_job_id": row[1],
            "result_mnemos_id": row[2],
            "hit_count": row[3],
            "cost_saved_usd": row[4],
            "model": row[5],
            "provider": row[6],
            "cached_at": row[7],
        }

    async def cache_store(
        self,
        *,
        cache_key: str,
        result_json: Optional[dict[str, Any]] = None,
        source_job_id: Optional[str] = None,
        result_mnemos_id: Optional[str] = None,
        model: Optional[str] = None,
        provider: Optional[str] = None,
        cached_at: Optional[float] = None,
        result: Optional[dict[str, Any]] = None,
        stored_at: Optional[float] = None,
    ) -> None:
        body = result_json if result_json is not None else result
        cache_ts = float(cached_at if cached_at is not None else stored_at if stored_at is not None else time.time())
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                "INSERT INTO hive_cache "
                "(cache_key, result_json, source_job_id, result_mnemos_id, "
                "hit_count, cost_saved_usd, model, provider, cached_at, last_hit_at) "
                "VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, NULL) "
                "ON CONFLICT(cache_key) DO UPDATE SET "
                "result_json=excluded.result_json, "
                "source_job_id=excluded.source_job_id, "
                "result_mnemos_id=excluded.result_mnemos_id, "
                "model=excluded.model, provider=excluded.provider, "
                "cached_at=excluded.cached_at",
                (
                    cache_key,
                    json.dumps(body or {}, default=str),
                    source_job_id,
                    result_mnemos_id,
                    model,
                    provider,
                    cache_ts,
                ),
            )
            db.commit()

    async def record_worker_kind_stats(
        self,
        *,
        urn: str,
        kind: str,
        success_delta: int,
        fail_delta: int,
        cancelled_delta: int,
        tokens_in: int,
        tokens_out: int,
        cost_usd: float,
        duration_sec: float,
        last_run: float,
    ) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                "INSERT INTO hive_worker_kind_stats "
                "(urn, kind, success_count, fail_count, cancelled_count, "
                "total_tokens_in, total_tokens_out, total_cost_usd, "
                "total_duration_sec, last_run) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(urn, kind) DO UPDATE SET "
                "success_count=success_count+excluded.success_count, "
                "fail_count=fail_count+excluded.fail_count, "
                "cancelled_count=cancelled_count+excluded.cancelled_count, "
                "total_tokens_in=total_tokens_in+excluded.total_tokens_in, "
                "total_tokens_out=total_tokens_out+excluded.total_tokens_out, "
                "total_cost_usd=total_cost_usd+excluded.total_cost_usd, "
                "total_duration_sec=total_duration_sec+excluded.total_duration_sec, "
                "last_run=excluded.last_run",
                (
                    urn,
                    kind,
                    int(success_delta),
                    int(fail_delta),
                    int(cancelled_delta),
                    int(tokens_in),
                    int(tokens_out),
                    float(cost_usd),
                    float(duration_sec),
                    float(last_run),
                ),
            )
            db.commit()


__all__ = ["HiveMindRepository", "SqliteHiveMindRepository"]
