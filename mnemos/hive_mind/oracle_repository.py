"""Oracle backend for the Hive Mind repository contract.

This module deliberately does not call ``oracledb.init_oracle_client()``.
Callers can opt into thin or thick python-oracledb mode before constructing
``OracleHiveMindRepository``.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import random
import time
import uuid
from typing import Any, Optional

from mnemos.hive_mind.repository import HiveMindRepository


UTC = dt.timezone.utc
ORACLE_SCHEMA_COMPONENT = "stiphos-oracle"
ORACLE_SCHEMA_VERSION = 2
_CANONICAL_TABLES = {
    "AGENTS",
    "JOBS",
    "HIVE_MESSAGES",
    "HIVE_EVENTS",
    "HIVE_CACHE",
    "HIVE_WORKER_KIND_STATS",
    "HIVE_SCHEMA_METADATA",
}
_LEGACY_TABLES = {"MEMORY_AGENTS", "MEMORY_JOBS", "MEMORY_MESSAGES", "MEMORY_EVENTS"}
_ORACLE_JOB_SELECT_COLUMNS = (
    "id, submitter_urn, parent_job_id, kind, description, priority, deadline, "
    "required_capabilities, eligible_kinds, project, max_cost_tier, preferred_providers, "
    "preferred_models, mnemos_refs, depends_on, status, claimed_by, claimed_at, "
    "claimed_runtime, claimed_model, claimed_provider, claimed_cost_tier, started_at, "
    "retry_backoff_until, ended_at, result, result_mnemos_id, tokens_in, tokens_out, "
    "estimated_cost_usd, retry_count, max_retries, routing_metadata"
)


class OracleSchemaCompatibilityError(RuntimeError):
    """The connected schema is not the canonical STIPHOS Oracle schema."""


def _json_dumps(value: Any) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, separators=(",", ":"), default=str)


def _json_loads(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    if hasattr(value, "read"):
        value = value.read()
    if value in ("", b""):
        return default
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    return json.loads(value)


def _uuid_to_raw(value: Optional[str | bytes]) -> Optional[bytes]:
    if value is None:
        return None
    if isinstance(value, bytes):
        if len(value) != 16:
            raise ValueError("RAW UUID values must be exactly 16 bytes")
        return value
    return uuid.UUID(str(value)).bytes


def _raw_to_uuid(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return str(value)
    return str(uuid.UUID(bytes=bytes(value)))


def _ts_to_dt(value: Optional[float | int | dt.datetime]) -> Optional[dt.datetime]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(UTC).replace(tzinfo=None)
    return dt.datetime.fromtimestamp(float(value), UTC).replace(tzinfo=None)


def _ts_to_tstz(value: Optional[float | int | dt.datetime]) -> Optional[dt.datetime]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return dt.datetime.fromtimestamp(float(value), UTC)


def _dt_to_ts(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.timestamp()
    return float(value)


def _read_lob(value: Any) -> Any:
    if hasattr(value, "read"):
        return value.read()
    return value


def _uuid7_raw() -> bytes:
    try:
        return uuid.uuid7().bytes  # type: ignore[attr-defined]
    except AttributeError:
        unix_ms = int(time.time() * 1000) & ((1 << 48) - 1)
        rand_a = random.getrandbits(12)
        rand_b = random.getrandbits(62)
        value = (unix_ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
        return uuid.UUID(int=value).bytes


class OracleHiveMindRepository(HiveMindRepository):
    """python-oracledb implementation of the Phase-2 Hive Mind contract."""

    def __init__(
        self,
        *,
        user: str,
        password: str,
        dsn: str,
        min_pool: int = 1,
        max_pool: int = 4,
        increment: int = 1,
        pool: Any = None,
    ) -> None:
        if pool is not None:
            self._pool = pool
            return
        import oracledb

        self._pool = oracledb.create_pool(
            user=user,
            password=password,
            dsn=dsn,
            min=min_pool,
            max=max_pool,
            increment=increment,
        )

    async def _run(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(fn, *args, **kwargs)

    @staticmethod
    def _execute(conn: Any, sql: str, params: Optional[dict[str, Any]] = None) -> Any:
        cur = conn.cursor()
        cur.execute(sql, params or {})
        return cur

    async def init(self) -> None:
        await self._run(self._validate_schema_sync)

    def _validate_schema_sync(self) -> None:
        migration = "migrations/oracle/002_memory_to_canonical.sql"
        with self._pool.acquire() as conn:
            cur = self._execute(
                conn,
                "SELECT table_name FROM user_tables WHERE table_name IN ("
                + ",".join(f"'{name}'" for name in sorted(_CANONICAL_TABLES | _LEGACY_TABLES))
                + ")",
            )
            found = {str(row[0]).upper() for row in cur.fetchall()}
            legacy = sorted(found & _LEGACY_TABLES)
            missing = sorted(_CANONICAL_TABLES - found)
            if legacy:
                raise OracleSchemaCompatibilityError(
                    "Unsupported deprecated Oracle MEMORY_* schema detected "
                    f"({', '.join(legacy)}). STIPHOS expects canonical AGENTS/JOBS and HIVE_* tables "
                    f"at schema version {ORACLE_SCHEMA_VERSION}; apply {migration} before initializing the repository."
                )
            if missing:
                raise OracleSchemaCompatibilityError(
                    "Incomplete or unversioned Oracle schema detected; missing canonical tables: "
                    f"{', '.join(missing)}. Apply mnemos/hive_mind/oracle_ddl.sql to a fresh schema "
                    f"or {migration} to a deprecated MEMORY_* schema."
                )
            cur = self._execute(
                conn,
                "SELECT schema_version FROM hive_schema_metadata WHERE component = :component",
                {"component": ORACLE_SCHEMA_COMPONENT},
            )
            row = cur.fetchone()
            actual = int(row[0]) if row else None
            if actual != ORACLE_SCHEMA_VERSION:
                raise OracleSchemaCompatibilityError(
                    f"Unsupported STIPHOS Oracle schema version {actual!r}; expected "
                    f"{ORACLE_SCHEMA_VERSION}. Apply the required forward migration before initializing the repository."
                )

    async def close(self) -> None:
        await self._run(self._pool.close)

    # ---------- agents ----------

    async def register_agent(
        self,
        *,
        urn: str,
        kind: str,
        host: str,
        runtime: str | None = None,
        model: Optional[str] = None,
        provider: Optional[str] = None,
        cost_tier: Optional[str] = None,
        auth_method: Optional[str] = None,
        autonomy_level: Optional[str] = None,
        pid: Optional[int] = None,
        capabilities: Optional[list[str]] = None,
        version: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        await self._run(
            self._register_agent_sync,
            urn=urn,
            kind=kind,
            host=host,
            runtime=runtime,
            model=model,
            provider=provider,
            cost_tier=cost_tier,
            auth_method=auth_method,
            autonomy_level=autonomy_level,
            plan_cap_usd=None,
            subscription_pools=None,
            session_id="repository-register",
            pid=pid,
            capabilities=capabilities,
            version=version,
            started_at=None,
            metadata=metadata,
        )

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
        plan_cap_usd: float,
        subscription_pools: Optional[list[str]],
        host: str,
        session_id: str,
        pid: Optional[int],
        capabilities: Optional[list[str]],
        version: Optional[str],
        started_at: float,
        last_heartbeat: float,
        metadata: Optional[dict[str, Any]],
    ) -> None:
        await self._run(
            self._register_agent_sync,
            urn=urn,
            kind=kind,
            host=host,
            runtime=runtime,
            model=model,
            provider=provider,
            cost_tier=cost_tier,
            auth_method=auth_method,
            autonomy_level=autonomy_level,
            plan_cap_usd=plan_cap_usd,
            subscription_pools=subscription_pools,
            session_id=session_id,
            pid=pid,
            capabilities=capabilities,
            version=version,
            started_at=_ts_to_tstz(started_at),
            last_heartbeat=_ts_to_tstz(last_heartbeat),
            metadata=metadata,
        )

    def _register_agent_sync(
        self,
        *,
        urn: str,
        kind: str,
        host: str,
        runtime: Optional[str],
        model: Optional[str],
        provider: Optional[str],
        cost_tier: Optional[str],
        auth_method: Optional[str],
        autonomy_level: Optional[str],
        plan_cap_usd: Optional[float],
        subscription_pools: Optional[list[str]],
        session_id: str,
        pid: Optional[int],
        capabilities: Optional[list[str]],
        version: Optional[str],
        started_at: Optional[dt.datetime],
        last_heartbeat: Optional[dt.datetime] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        with self._pool.acquire() as conn:
            self._execute(
                conn,
                """
                MERGE INTO agents dst
                USING (
                  SELECT :urn AS urn FROM dual
                ) src
                ON (dst.urn = src.urn)
                WHEN MATCHED THEN UPDATE SET
                  dst.kind=:kind, dst.host=:host, dst.runtime=:runtime,
                  dst.model=:model, dst.provider=:provider, dst.cost_tier=:cost_tier,
                  dst.auth_method=COALESCE(:auth_method,'unknown'),
                  dst.autonomy_level=:autonomy_level, dst.plan_cap_usd=:plan_cap_usd,
                  dst.subscription_pools=:subscription_pools, dst.session_id=:session_id,
                  dst.pid=:pid, dst.capabilities=:capabilities, dst.version=:version,
                  dst.last_heartbeat=COALESCE(:last_heartbeat,SYSTIMESTAMP),
                  dst.status='online', dst.metadata=:metadata
                WHEN NOT MATCHED THEN INSERT (
                  urn, kind, host, runtime, model, provider, cost_tier, auth_method,
                  autonomy_level, plan_cap_usd, subscription_pools, session_id, pid,
                  capabilities, version, started_at, last_heartbeat, status, metadata
                ) VALUES (
                  :urn, :kind, :host, :runtime, :model, :provider, :cost_tier,
                  COALESCE(:auth_method,'unknown'), :autonomy_level, :plan_cap_usd,
                  :subscription_pools, :session_id, :pid, :capabilities, :version,
                  COALESCE(:started_at,SYSTIMESTAMP), COALESCE(:last_heartbeat,SYSTIMESTAMP),
                  'online', :metadata
                )
                """,
                {
                    "urn": urn,
                    "kind": kind,
                    "host": host,
                    "runtime": runtime,
                    "model": model,
                    "provider": provider,
                    "cost_tier": cost_tier,
                    "auth_method": auth_method,
                    "autonomy_level": autonomy_level,
                    "plan_cap_usd": plan_cap_usd,
                    "subscription_pools": _json_dumps(subscription_pools),
                    "session_id": session_id,
                    "pid": pid,
                    "last_heartbeat": last_heartbeat,
                    "capabilities": _json_dumps(capabilities),
                    "version": version,
                    "started_at": started_at,
                    "metadata": _json_dumps(metadata),
                },
            )
            conn.commit()

    async def heartbeat(self, *, urn: str, ts: Optional[float] = None) -> bool:
        return await self._run(self._heartbeat_sync, urn, _ts_to_dt(ts))

    async def heartbeat_agent(self, *, urn: str, ts: float) -> bool:
        return await self.heartbeat(urn=urn, ts=ts)

    def _heartbeat_sync(self, urn: str, heartbeat_at: Optional[dt.datetime]) -> bool:
        with self._pool.acquire() as conn:
            cur = self._execute(
                conn,
                """
                UPDATE agents
                SET last_heartbeat = COALESCE(:last_heartbeat, SYSTIMESTAMP)
                WHERE urn = :urn
                """,
                {"urn": urn, "last_heartbeat": heartbeat_at},
            )
            conn.commit()
            return cur.rowcount > 0

    async def get_agent(self, urn: str) -> Optional[dict[str, Any]]:
        return await self._run(self._get_agent_sync, urn)

    def _get_agent_sync(self, urn: str) -> Optional[dict[str, Any]]:
        with self._pool.acquire() as conn:
            cur = self._execute(
                conn,
                """
                SELECT urn, kind, host, started_at, last_heartbeat, capabilities
                FROM agents WHERE urn = :urn
                """,
                {"urn": urn},
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "urn": row[0],
                "kind": row[1],
                "host": row[2],
                "registered_at": _dt_to_ts(row[3]),
                "last_heartbeat": _dt_to_ts(row[4]),
                "capabilities": _json_loads(row[5], []),
            }

    async def list_agents(
        self,
        *,
        status: Optional[str] = None,
        kind: Optional[str] = None,
        host: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return await self._run(
            self._list_agents_sync,
            status=status,
            kind=kind,
            host=host,
            limit=limit,
        )

    def _list_agents_sync(
        self,
        *,
        status: Optional[str],
        kind: Optional[str],
        host: Optional[str],
        limit: int,
    ) -> list[dict[str, Any]]:
        sql = (
            "SELECT urn, kind, host, started_at, last_heartbeat, capabilities FROM agents WHERE 1=1"
        )
        params: dict[str, Any] = {"limit": int(limit)}
        if status:
            sql += " AND status = :status"
            params["status"] = status
        if kind:
            sql += " AND kind = :kind"
            params["kind"] = kind
        if host:
            sql += " AND host = :host"
            params["host"] = host
        sql += " ORDER BY last_heartbeat DESC FETCH FIRST :limit ROWS ONLY"
        with self._pool.acquire() as conn:
            cur = self._execute(conn, sql, params)
            return [
                {
                    "urn": r[0],
                    "kind": r[1],
                    "host": r[2],
                    "registered_at": _dt_to_ts(r[3]),
                    "last_heartbeat": _dt_to_ts(r[4]),
                    "capabilities": _json_loads(r[5], []),
                }
                for r in cur.fetchall()
            ]

    # ---------- jobs ----------

    async def create_job(
        self,
        *,
        job_id: str,
        submitter_urn: str,
        parent_job_id: Optional[str],
        kind: str,
        description: Optional[str],
        priority: int,
        deadline: Optional[float],
        required_capabilities: Optional[list[str]],
        eligible_kinds: Optional[list[str]],
        project: Optional[str],
        max_cost_tier: str,
        preferred_providers: Optional[list[str]],
        preferred_models: Optional[list[str]],
        mnemos_refs: Optional[list[str]],
        depends_on: Optional[list[str]],
        max_retries: int,
        started_at: float,
        routing_metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        await self._run(
            self._insert_service_job_sync,
            job_id=job_id,
            submitter_urn=submitter_urn,
            parent_job_id=parent_job_id,
            kind=kind,
            description=description,
            priority=priority,
            eligible_kinds=eligible_kinds,
            project=project,
            deadline=deadline,
            required_capabilities=required_capabilities,
            max_cost_tier=max_cost_tier,
            preferred_providers=preferred_providers,
            preferred_models=preferred_models,
            mnemos_refs=mnemos_refs,
            depends_on=depends_on,
            routing_metadata=routing_metadata,
            max_retries=max_retries,
            status="queued",
            started_at=started_at,
            ended_at=None,
            result=None,
            result_mnemos_id=None,
            claimed_model=None,
            claimed_provider=None,
        )

    async def insert_job_queued(self, **kwargs: Any) -> None:
        await self.create_job(**kwargs)

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
        tags_dict = tags if isinstance(tags, dict) else {}
        await self._run(
            self._insert_service_job_sync,
            job_id=job_id,
            submitter_urn=submitter_urn,
            parent_job_id=parent_job_id,
            kind=kind,
            description=description,
            priority=priority,
            deadline=None,
            required_capabilities=None,
            eligible_kinds=eligible_kinds,
            project=project,
            max_cost_tier=str(tags_dict.get("max_cost_tier") or "A"),
            preferred_providers=tags_dict.get("preferred_providers"),
            preferred_models=tags_dict.get("preferred_models"),
            mnemos_refs=tags_dict.get("mnemos_refs"),
            depends_on=tags_dict.get("depends_on"),
            routing_metadata=None,
            max_retries=max_retries,
            status="queued",
            started_at=created_at,
            ended_at=None,
            result=None,
            result_mnemos_id=None,
            claimed_model=None,
            claimed_provider=None,
        )

    async def insert_job_cache_hit(
        self,
        *,
        job_id: str,
        submitter_urn: str,
        parent_job_id: Optional[str],
        kind: str,
        description: Optional[str],
        priority: int,
        deadline: Optional[float] = None,
        required_capabilities: Optional[list[str]] = None,
        eligible_kinds: Optional[list[str]] = None,
        project: Optional[str] = None,
        max_cost_tier: str = "A",
        preferred_providers: Optional[list[str]] = None,
        preferred_models: Optional[list[str]] = None,
        mnemos_refs: Optional[list[str]] = None,
        started_at: Optional[float] = None,
        ended_at: Optional[float] = None,
        result: Optional[dict[str, Any]] = None,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        result_mnemos_id: Optional[str] = None,
    ) -> None:
        await self._run(
            self._insert_service_job_sync,
            job_id=job_id,
            submitter_urn=submitter_urn,
            parent_job_id=parent_job_id,
            kind=kind,
            description=description,
            priority=priority,
            deadline=deadline,
            required_capabilities=required_capabilities,
            status="done",
            eligible_kinds=eligible_kinds,
            project=project,
            max_cost_tier=max_cost_tier,
            preferred_providers=preferred_providers,
            preferred_models=preferred_models,
            mnemos_refs=mnemos_refs,
            depends_on=None,
            routing_metadata=None,
            max_retries=0,
            started_at=started_at,
            ended_at=ended_at,
            result=result,
            result_mnemos_id=result_mnemos_id,
            claimed_model=model,
            claimed_provider=provider,
        )

    def _insert_service_job_sync(
        self,
        *,
        job_id: str,
        submitter_urn: str,
        parent_job_id: Optional[str],
        kind: str,
        description: Optional[str],
        priority: int,
        deadline: Optional[float],
        required_capabilities: Optional[list[str]],
        status: str,
        eligible_kinds: Optional[list[str]],
        project: Optional[str],
        max_cost_tier: str,
        preferred_providers: Optional[list[str]],
        preferred_models: Optional[list[str]],
        mnemos_refs: Optional[list[str]],
        depends_on: Optional[list[str]],
        routing_metadata: Optional[dict[str, Any]],
        max_retries: int,
        started_at: Optional[float],
        ended_at: Optional[float],
        result: Optional[dict[str, Any]],
        result_mnemos_id: Optional[str],
        claimed_model: Optional[str],
        claimed_provider: Optional[str],
    ) -> None:
        with self._pool.acquire() as conn:
            self._execute(
                conn,
                """
                INSERT INTO jobs (
                  id, submitter_urn, parent_job_id, kind, description, priority,
                  deadline, required_capabilities, eligible_kinds, project,
                  max_cost_tier, preferred_providers, preferred_models, mnemos_refs,
                  depends_on, status, claimed_model, claimed_provider, started_at,
                  ended_at, result, result_mnemos_id, retry_count, max_retries,
                  routing_metadata
                ) VALUES (
                  :id, :submitter_urn, :parent_job_id, :kind, :description, :priority,
                  :deadline, :required_capabilities, :eligible_kinds, :project,
                  :max_cost_tier, :preferred_providers, :preferred_models, :mnemos_refs,
                  :depends_on, :status, :claimed_model, :claimed_provider,
                  COALESCE(:started_at,SYSTIMESTAMP), :ended_at, :result,
                  :result_mnemos_id, 0, :max_retries, :routing_metadata
                )
                """,
                {
                    "id": _uuid_to_raw(job_id),
                    "status": status,
                    "priority": int(priority),
                    "kind": kind,
                    "description": description,
                    "submitter_urn": submitter_urn,
                    "parent_job_id": _uuid_to_raw(parent_job_id),
                    "deadline": _ts_to_tstz(deadline),
                    "required_capabilities": _json_dumps(required_capabilities),
                    "started_at": _ts_to_tstz(started_at),
                    "ended_at": _ts_to_tstz(ended_at),
                    "result": _json_dumps(result),
                    "eligible_kinds": _json_dumps(eligible_kinds),
                    "project": project,
                    "max_cost_tier": str(max_cost_tier or "A").upper(),
                    "preferred_providers": _json_dumps(preferred_providers),
                    "preferred_models": _json_dumps(preferred_models),
                    "mnemos_refs": _json_dumps(mnemos_refs),
                    "depends_on": _json_dumps(depends_on),
                    "routing_metadata": _json_dumps(routing_metadata),
                    "result_mnemos_id": result_mnemos_id,
                    "claimed_model": claimed_model,
                    "claimed_provider": claimed_provider,
                    "max_retries": int(max_retries),
                },
            )
            conn.commit()

    async def claim_next_job(
        self,
        *,
        agent_urn: str,
        agent_kind: str,
        agent_capabilities: Optional[list[str]] = None,
        agent_cost_tier: str = "A",
    ) -> Optional[dict[str, Any]]:
        return await self.find_and_claim_job(
            agent_urn=agent_urn,
            agent_kind=agent_kind,
            agent_caps=set(agent_capabilities or []),
            agent_runtime="unknown",
            agent_model="unknown",
            agent_provider="unknown",
            agent_tier=agent_cost_tier,
            cost_tier_order=["A", "B", "C"],
            sub_throttled=False,
            now=time.time(),
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
        return await self._run(
            self._claim_service_job_sync,
            job_id=None,
            agent_urn=agent_urn,
            agent_kind=agent_kind,
            agent_caps=set(agent_caps),
            agent_runtime=agent_runtime,
            agent_model=agent_model,
            agent_provider=agent_provider,
            agent_tier=agent_tier,
            cost_tier_order=list(cost_tier_order),
            sub_throttled=sub_throttled,
            now=now,
        )

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
        claimed = await self._run(
            self._claim_service_job_sync,
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
            now=now,
        )
        if claimed is None:
            return ("unavailable", None)
        if claimed is False:
            return ("ineligible", None)
        return ("claimed", claimed)

    def _claim_service_job_sync(
        self,
        *,
        job_id: Optional[str],
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
    ) -> Optional[dict[str, Any]] | bool:
        with self._pool.acquire() as conn:
            try:
                cur = conn.cursor()
                where = "status IN ('queued','offered') AND claimed_by IS NULL AND (retry_backoff_until IS NULL OR retry_backoff_until <= :now)"
                params: dict[str, Any] = {"now": _ts_to_tstz(now)}
                if job_id is not None:
                    where += " AND id = :id"
                    params["id"] = _uuid_to_raw(job_id)
                cur.execute(
                    f"SELECT {_ORACLE_JOB_SELECT_COLUMNS} FROM jobs WHERE {where} "
                    "ORDER BY priority DESC, started_at ASC FOR UPDATE SKIP LOCKED",
                    params,
                )
                for row in cur:
                    if not self._service_job_is_claimable(
                        conn,
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
                        if job_id is not None:
                            conn.rollback()
                            return False
                        continue
                    update_cur = self._execute(
                        conn,
                        """
                        UPDATE jobs SET status='claimed', claimed_by=:claimed_by,
                            claimed_at=:claimed_at, claimed_runtime=:claimed_runtime,
                            claimed_model=:claimed_model, claimed_provider=:claimed_provider,
                            claimed_cost_tier=:claimed_cost_tier
                        WHERE id = :id
                          AND status IN ('queued','offered') AND claimed_by IS NULL
                        """,
                        {
                            "claimed_by": agent_urn,
                            "claimed_at": _ts_to_tstz(now),
                            "claimed_runtime": agent_runtime,
                            "claimed_model": agent_model,
                            "claimed_provider": agent_provider,
                            "claimed_cost_tier": agent_tier,
                            "id": row[0],
                        },
                    )
                    if update_cur.rowcount != 1:
                        continue
                    conn.commit()
                    claimed_row = list(row)
                    claimed_row[15] = "claimed"
                    claimed_row[16] = agent_urn
                    claimed_row[17] = _ts_to_tstz(now)
                    claimed_row[18] = agent_runtime
                    claimed_row[19] = agent_model
                    claimed_row[20] = agent_provider
                    claimed_row[21] = agent_tier
                    claimed = self._job_from_row(claimed_row)
                    claimed["claimed_resources"] = {
                        "runtime": agent_runtime,
                        "model": agent_model,
                        "provider": agent_provider,
                        "cost_tier": agent_tier,
                    }
                    return claimed
                conn.rollback()
                return None
            except Exception:
                conn.rollback()
                raise

    @staticmethod
    def _normalized_strings(value: Any) -> set[str]:
        loaded = _json_loads(value, value) if isinstance(value, (str, bytes)) else value
        if not loaded:
            return set()
        items = loaded.keys() if isinstance(loaded, dict) else loaded if isinstance(loaded, (list, tuple, set)) else [loaded]
        return {
            "".join(ch if ch.isalnum() else "_" for ch in str(item).strip().lower()).strip("_")
            for item in items
            if str(item).strip()
        }

    def _service_job_is_claimable(
        self,
        conn: Any,
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
    ) -> bool:
        if not set(_json_loads(row[7], []) or []).issubset(agent_caps):
            return False
        eligible = _json_loads(row[8], None)
        if eligible and agent_kind not in set(eligible):
            return False
        # preferred_providers/preferred_models are soft routing hints. Only
        # required_capabilities and the explicit gates below are mandatory.
        routing = _json_loads(row[32], {}) or {}
        required_pools = self._normalized_strings(routing.get("required_subscription_pools"))
        if required_pools:
            cur = self._execute(
                conn,
                "SELECT subscription_pools FROM agents WHERE urn=:urn AND status IN ('online','idle')",
                {"urn": agent_urn},
            )
            agent_row = cur.fetchone()
            if not required_pools.intersection(self._normalized_strings(agent_row[0] if agent_row else None)):
                return False
        if sub_throttled and str(row[10] or "A").upper() != "A":
            return False
        try:
            if cost_tier_order.index(str(agent_tier or "C").upper()) > cost_tier_order.index(str(row[10] or "A").upper()):
                return False
        except ValueError:
            return False
        for dependency in _json_loads(row[14], []) or []:
            cur = self._execute(conn, "SELECT status FROM jobs WHERE id=:id", {"id": _uuid_to_raw(dependency)})
            dep = cur.fetchone()
            if not dep or dep[0] != "done":
                return False
        return True

    async def update_job_routing(self, *, job_id: str, routed_at: float, **kwargs: Any) -> bool:
        return await self._run(self._update_job_routing_sync, job_id=job_id, routed_at=routed_at, **kwargs)

    def _update_job_routing_sync(self, *, job_id: str, routed_at: float, **kwargs: Any) -> bool:
        with self._pool.acquire() as conn:
            cur = self._execute(
                conn,
                "SELECT max_cost_tier, routing_metadata FROM jobs "
                "WHERE id=:id AND status='queued' AND claimed_by IS NULL FOR UPDATE",
                {"id": _uuid_to_raw(job_id)},
            )
            existing = cur.fetchone()
            if not existing:
                conn.rollback()
                return False
            requested_tier = kwargs.get("max_cost_tier")
            metadata = _json_loads(existing[1], {}) or {}
            if requested_tier is not None and metadata.get("submitter_max_cost_tier_explicit"):
                order = ["A", "B", "C"]
                try:
                    if order.index(str(requested_tier).upper()) > order.index(str(existing[0] or "A").upper()):
                        requested_tier = existing[0]
                except ValueError:
                    requested_tier = existing[0]
            fields = ["routed_at=:routed_at", "routing_metadata=:routing_metadata"]
            params: dict[str, Any] = {
                "id": _uuid_to_raw(job_id),
                "routed_at": _ts_to_tstz(routed_at),
                "routing_metadata": _json_dumps(kwargs.get("routing_metadata") or {}),
            }
            for column in (
                "required_capabilities",
                "eligible_kinds",
                "preferred_providers",
                "preferred_models",
                "mnemos_refs",
                "depends_on",
            ):
                if kwargs.get(column) is not None:
                    fields.append(f"{column}=:{column}")
                    params[column] = _json_dumps(kwargs[column])
            if requested_tier is not None:
                fields.append("max_cost_tier=:max_cost_tier")
                params["max_cost_tier"] = str(requested_tier).upper()
            cur = self._execute(
                conn,
                f"UPDATE jobs SET {', '.join(fields)} "
                "WHERE id=:id AND status='queued' AND claimed_by IS NULL",
                params,
            )
            conn.commit()
            return cur.rowcount == 1

    async def update_job(
        self,
        *,
        job_id: str,
        status: str,
        result: Optional[dict[str, Any]] = None,
        tokens_in: Optional[int] = None,
        tokens_out: Optional[int] = None,
        result_mnemos_id: Optional[str] = None,
        ended_at: Optional[float] = None,
        estimated_cost_usd: Optional[float] = None,
    ) -> None:
        await self._run(
            self._update_job_sync,
            job_id=job_id,
            status=status,
            result=result,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            result_mnemos_id=result_mnemos_id,
            ended_at=_ts_to_tstz(ended_at),
            estimated_cost_usd=estimated_cost_usd,
        )

    def _update_job_sync(
        self,
        *,
        job_id: str,
        status: str,
        result: Optional[dict[str, Any]],
        tokens_in: Optional[int],
        tokens_out: Optional[int],
        result_mnemos_id: Optional[str],
        ended_at: Optional[dt.datetime],
        estimated_cost_usd: Optional[float],
    ) -> None:
        with self._pool.acquire() as conn:
            cur = self._execute(
                conn,
                """
                UPDATE jobs
                SET status = :status,
                    result = COALESCE(:result, result),
                    tokens_in = COALESCE(:tokens_in, tokens_in),
                    tokens_out = COALESCE(:tokens_out, tokens_out),
                    result_mnemos_id = COALESCE(:result_mnemos_id, result_mnemos_id),
                    estimated_cost_usd = COALESCE(:estimated_cost_usd, estimated_cost_usd),
                    ended_at = CASE
                      WHEN :ended_at IS NOT NULL THEN :ended_at
                      WHEN :status IN ('done', 'failed', 'cancelled') THEN SYSTIMESTAMP
                      ELSE ended_at
                    END
                WHERE id = :id
                """,
                {
                    "id": _uuid_to_raw(job_id),
                    "status": status,
                    "result": _json_dumps(result),
                    "tokens_in": tokens_in,
                    "tokens_out": tokens_out,
                    "result_mnemos_id": result_mnemos_id,
                    "ended_at": ended_at,
                    "estimated_cost_usd": estimated_cost_usd,
                },
            )
            conn.commit()
            if cur.rowcount == 0:
                raise KeyError(f"job not found: {job_id}")

    async def get_job(self, job_id: str) -> Optional[dict[str, Any]]:
        return await self._run(self._get_job_sync, job_id)

    def _get_job_sync(self, job_id: str) -> Optional[dict[str, Any]]:
        with self._pool.acquire() as conn:
            cur = self._execute(
                conn,
                """
                SELECT """ + _ORACLE_JOB_SELECT_COLUMNS + """
                FROM jobs
                WHERE id = :id
                """,
                {"id": _uuid_to_raw(job_id)},
            )
            row = cur.fetchone()
            return self._job_from_row(row) if row else None

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
        return await self._run(
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
            "SELECT " + _ORACLE_JOB_SELECT_COLUMNS + " FROM jobs WHERE 1=1"
        )
        params: dict[str, Any] = {"limit": int(limit)}
        for name, value in {
            "status": status,
            "kind": kind,
            "project": project,
            "submitter_urn": submitter_urn,
            "claimed_by": claimed_by,
        }.items():
            if value:
                sql += f" AND {name} = :{name}"
                params[name] = value
        sql += " ORDER BY priority DESC, started_at DESC FETCH FIRST :limit ROWS ONLY"
        with self._pool.acquire() as conn:
            cur = self._execute(conn, sql, params)
            return [self._job_from_row(r) for r in cur.fetchall()]

    def _job_from_row(self, row: Any) -> dict[str, Any]:
        return {
            "id": _raw_to_uuid(row[0]),
            "submitter_urn": row[1],
            "parent_job_id": _raw_to_uuid(row[2]),
            "kind": row[3],
            "description": row[4],
            "priority": row[5],
            "deadline": _dt_to_ts(row[6]),
            "required_capabilities": _json_loads(row[7], None),
            "eligible_kinds": _json_loads(row[8], None),
            "project": row[9],
            "max_cost_tier": row[10],
            "preferred_providers": _json_loads(row[11], None),
            "preferred_models": _json_loads(row[12], None),
            "mnemos_refs": _json_loads(row[13], None),
            "depends_on": _json_loads(row[14], None),
            "status": row[15],
            "claimed_by": row[16],
            "claimed_at": _dt_to_ts(row[17]),
            "claimed_runtime": row[18],
            "claimed_model": row[19],
            "claimed_provider": row[20],
            "claimed_cost_tier": row[21],
            "started_at": _dt_to_ts(row[22]),
            "created_at": _dt_to_ts(row[22]),
            "queued_at": _dt_to_ts(row[22]),
            "retry_backoff_until": _dt_to_ts(row[23]),
            "ended_at": _dt_to_ts(row[24]),
            "result": _json_loads(row[25], None),
            "result_mnemos_id": row[26],
            "tokens_in": row[27],
            "tokens_out": row[28],
            "estimated_cost_usd": float(row[29]) if row[29] is not None else None,
            "retry_count": row[30],
            "max_retries": row[31],
            "routing_metadata": _json_loads(row[32], None),
        }

    # ---------- cache + stats ----------

    async def get_cache(self, cache_key: str) -> Optional[Any]:
        return await self._run(self._get_cache_sync, cache_key)

    async def cache_lookup(self, cache_key: str) -> Optional[dict[str, Any]]:
        value = await self.get_cache(cache_key)
        return value if isinstance(value, dict) else {"value": value} if value is not None else None

    def _get_cache_sync(self, cache_key: str) -> Optional[Any]:
        cached = self._cache_get_sync(cache_key)
        return cached["result"] if cached else None

    async def set_cache(
        self,
        *,
        cache_key: str,
        value: Any,
        expires_at: Optional[float | dt.datetime] = None,
    ) -> None:
        await self._run(
            self._set_cache_sync,
            cache_key=cache_key,
            value=value,
            expires_at=_ts_to_dt(expires_at),
        )

    async def cache_get(self, cache_key: str) -> Optional[dict[str, Any]]:
        return await self._run(self._cache_get_sync, cache_key)

    async def cache_store(
        self,
        *,
        cache_key: str,
        source_job_id: Optional[str] = None,
        result: Optional[dict[str, Any]] = None,
        result_json: Optional[dict[str, Any]] = None,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        result_mnemos_id: Optional[str] = None,
        stored_at: Optional[float] = None,
        cached_at: Optional[float] = None,
    ) -> None:
        await self._run(
            self._cache_store_sync,
            cache_key=cache_key,
            result_json=result_json if result_json is not None else result,
            source_job_id=source_job_id,
            result_mnemos_id=result_mnemos_id,
            model=model,
            provider=provider,
            cached_at=cached_at if cached_at is not None else stored_at,
        )

    def _cache_get_sync(self, cache_key: str) -> Optional[dict[str, Any]]:
        with self._pool.acquire() as conn:
            cur = self._execute(
                conn,
                """
                SELECT result_json, source_job_id, result_mnemos_id,
                       hit_count, cost_saved_usd, model, provider, cached_at
                FROM hive_cache
                WHERE cache_key = :cache_key
                """,
                {"cache_key": cache_key},
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "result": _json_loads(row[0], None),
                "source_job_id": _raw_to_uuid(row[1]) if row[1] is not None else None,
                "result_mnemos_id": row[2],
                "hit_count": row[3],
                "cost_saved_usd": float(row[4] or 0),
                "model": row[5],
                "provider": row[6],
                "cached_at": _dt_to_ts(row[7]) if isinstance(row[7], dt.datetime) else row[7],
            }

    def _cache_store_sync(
        self,
        *,
        cache_key: str,
        result_json: Optional[dict[str, Any]],
        source_job_id: Optional[str],
        result_mnemos_id: Optional[str],
        model: Optional[str],
        provider: Optional[str],
        cached_at: Optional[float],
    ) -> None:
        with self._pool.acquire() as conn:
            self._execute(
                conn,
                """
                MERGE INTO hive_cache dst
                USING (
                  SELECT :cache_key AS cache_key,
                         :result_json AS result_json,
                         :source_job_id AS source_job_id,
                         :result_mnemos_id AS result_mnemos_id,
                         :model AS model,
                         :provider AS provider,
                         :cached_at AS cached_at
                  FROM dual
                ) src
                ON (dst.cache_key = src.cache_key)
                WHEN MATCHED THEN UPDATE SET
                  dst.result_json = src.result_json,
                  dst.source_job_id = src.source_job_id,
                  dst.result_mnemos_id = src.result_mnemos_id,
                  dst.model = src.model,
                  dst.provider = src.provider,
                  dst.cached_at = src.cached_at
                WHEN NOT MATCHED THEN INSERT (
                  cache_key, result_json, source_job_id, result_mnemos_id,
                  hit_count, cost_saved_usd, model, provider, cached_at, last_hit_at
                ) VALUES (
                  src.cache_key, src.result_json, src.source_job_id,
                  src.result_mnemos_id, 0, 0, src.model, src.provider,
                  src.cached_at, NULL
                )
                """,
                {
                    "cache_key": cache_key,
                    "result_json": _json_dumps(result_json or {}),
                    "source_job_id": _uuid_to_raw(source_job_id) if source_job_id else None,
                    "result_mnemos_id": result_mnemos_id,
                    "model": model,
                    "provider": provider,
                    "cached_at": _ts_to_tstz(cached_at if cached_at is not None else time.time()),
                },
            )
            conn.commit()

    def _set_cache_sync(
        self,
        *,
        cache_key: str,
        value: Any,
        expires_at: Optional[dt.datetime],
    ) -> None:
        with self._pool.acquire() as conn:
            self._execute(
                conn,
                """
                MERGE INTO hive_cache dst
                USING (
                  SELECT :cache_key AS cache_key,
                         :result_json AS result_json
                  FROM dual
                ) src
                ON (dst.cache_key = src.cache_key)
                WHEN MATCHED THEN UPDATE SET
                  dst.result_json = src.result_json,
                  dst.cached_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN INSERT (
                  cache_key, result_json, cached_at
                ) VALUES (
                  src.cache_key, src.result_json, SYSTIMESTAMP
                )
                """,
                {
                    "cache_key": cache_key,
                    "result_json": _json_dumps(value),
                },
            )
            conn.commit()

    async def increment_kind_stat(
        self,
        *,
        kind: str,
        status: str,
        tokens_in: int = 0,
        tokens_out: int = 0,
        cost_usd: float = 0.0,
        duration_sec: float = 0.0,
    ) -> None:
        await self._run(
            self._increment_kind_stat_sync,
            kind=kind,
            status=status,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            cost_usd=cost_usd,
            duration_sec=duration_sec,
        )

    def _increment_kind_stat_sync(
        self,
        *,
        kind: str,
        status: str,
        tokens_in: int,
        tokens_out: int,
        cost_usd: float,
        duration_sec: float,
    ) -> None:
        counts = {
            "done": (1, 0, 0),
            "success": (1, 0, 0),
            "failed": (0, 1, 0),
            "fail": (0, 1, 0),
            "cancelled": (0, 0, 1),
            "canceled": (0, 0, 1),
        }
        success_count, fail_count, cancelled_count = counts.get(status, (0, 0, 0))
        with self._pool.acquire() as conn:
            self._execute(
                conn,
                """
                MERGE INTO hive_worker_kind_stats dst
                USING (
                  SELECT :kind AS kind,
                         :success_count AS success_count,
                         :fail_count AS fail_count,
                         :cancelled_count AS cancelled_count,
                         :tokens_in AS total_tokens_in,
                         :tokens_out AS total_tokens_out,
                         :cost_usd AS total_cost_usd,
                         :duration_sec AS total_duration_sec
                  FROM dual
                ) src
                ON (dst.urn = 'aggregate' AND dst.kind = src.kind)
                WHEN MATCHED THEN UPDATE SET
                  dst.success_count = dst.success_count + src.success_count,
                  dst.fail_count = dst.fail_count + src.fail_count,
                  dst.cancelled_count = dst.cancelled_count + src.cancelled_count,
                  dst.total_tokens_in = dst.total_tokens_in + src.total_tokens_in,
                  dst.total_tokens_out = dst.total_tokens_out + src.total_tokens_out,
                  dst.total_cost_usd = dst.total_cost_usd + src.total_cost_usd,
                  dst.total_duration_sec = dst.total_duration_sec + src.total_duration_sec,
                  dst.last_run = SYSTIMESTAMP
                WHEN NOT MATCHED THEN INSERT (
                  urn, kind, success_count, fail_count, cancelled_count,
                  total_tokens_in, total_tokens_out, total_cost_usd,
                  total_duration_sec, last_run
                ) VALUES (
                  'aggregate', src.kind, src.success_count, src.fail_count, src.cancelled_count,
                  src.total_tokens_in, src.total_tokens_out, src.total_cost_usd,
                  src.total_duration_sec, SYSTIMESTAMP
                )
                """,
                {
                    "kind": kind,
                    "success_count": success_count,
                    "fail_count": fail_count,
                    "cancelled_count": cancelled_count,
                    "tokens_in": int(tokens_in),
                    "tokens_out": int(tokens_out),
                    "cost_usd": float(cost_usd),
                    "duration_sec": float(duration_sec),
                },
            )
            conn.commit()

    # ---------- messages + events ----------

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
        msg_id = _raw_to_uuid(_uuid7_raw()) or str(uuid.uuid4())
        await self._run(
            self._insert_message_sync,
            msg_id=msg_id,
            from_urn=from_urn,
            to_urn=to_urn,
            in_reply_to=in_reply_to,
            topic=topic,
            payload=payload,
            ts=ts,
        )
        return msg_id

    async def post_message(self, **kwargs: Any) -> None:
        await self.insert_message(
            from_urn=kwargs["from_urn"],
            to_urn=kwargs.get("to_urn"),
            in_reply_to=kwargs.get("in_reply_to"),
            topic=kwargs["topic"],
            payload=kwargs["payload"],
            ts=kwargs["ts"],
        )

    def _insert_message_sync(
        self,
        *,
        msg_id: str,
        from_urn: str,
        to_urn: Optional[str],
        in_reply_to: Optional[str],
        topic: str,
        payload: dict[str, Any],
        ts: float,
    ) -> None:
        with self._pool.acquire() as conn:
            self._execute(
                conn,
                """
                INSERT INTO hive_messages (
                  id, from_urn, to_urn, in_reply_to, topic, payload, ts
                ) VALUES (
                  :id, :from_urn, :to_urn, :in_reply_to, :topic, :payload, :ts
                )
                """,
                {
                    "id": msg_id,
                    "from_urn": from_urn,
                    "to_urn": to_urn,
                    "in_reply_to": in_reply_to,
                    "topic": topic,
                    "payload": _json_dumps(payload),
                    "ts": _ts_to_tstz(ts),
                },
            )
            conn.commit()

    async def list_messages(
        self,
        *,
        to_urn: Optional[str] = None,
        topic: Optional[str] = None,
        since_ts: Optional[float] = None,
        limit: int = 100,
        **_legacy: Any,
    ) -> list[dict[str, Any]]:
        """Recent messages from hive_messages (option B: reuse agent_bus-owned table)."""
        return await self._run(
            self._list_messages_sync,
            to_urn=to_urn,
            topic=topic,
            since_ts=since_ts,
            limit=int(limit),
        )

    def _list_messages_sync(
        self,
        *,
        to_urn: Optional[str],
        topic: Optional[str],
        since_ts: Optional[float],
        limit: int,
    ) -> list[dict[str, Any]]:
        sql = "SELECT id, from_urn, to_urn, in_reply_to, topic, payload, ts " "FROM hive_messages WHERE 1=1"
        params: dict[str, Any] = {}
        if to_urn is not None:
            sql += " AND to_urn = :to_urn"
            params["to_urn"] = to_urn
        if topic is not None:
            sql += " AND topic = :topic"
            params["topic"] = topic
        if since_ts is not None:
            sql += " AND ts >= :since_ts"
            params["since_ts"] = _ts_to_tstz(since_ts)
        sql += " ORDER BY ts DESC FETCH FIRST :lim ROWS ONLY"
        params["lim"] = max(1, min(int(limit), 1000))
        with self._pool.acquire() as conn:
            cur = self._execute(conn, sql, params)
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
            out: list[dict[str, Any]] = []
            for r in rows:
                rid = str(r[0]) if r[0] is not None else ""
                payload_raw = r[5]
                try:
                    payload = (
                        payload_raw
                        if isinstance(payload_raw, (dict, list))
                        else (_json_loads(payload_raw) if payload_raw else {})
                    )
                except Exception:
                    payload = {"_raw": str(payload_raw)}
                out.append(
                    {
                        "id": rid,
                        "from_urn": r[1],
                        "to_urn": r[2],
                        "in_reply_to": str(r[3]) if r[3] is not None else None,
                        "topic": r[4],
                        "payload": payload,
                        "ts": _dt_to_ts(r[6]) or 0.0,
                    }
                )
            return out

    async def emit_event(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        agent_urn: Optional[str] = None,
        ts: Optional[float | dt.datetime] = None,
    ) -> None:
        event_ts = ts if ts is not None else time.time()
        await self._run(
            self._emit_event_sync,
            kind=kind,
            payload=payload,
            agent_urn=agent_urn or payload.get("urn") or payload.get("agent_urn"),
            ts=_ts_to_tstz(event_ts),
            ts_epoch=_dt_to_ts(event_ts) if isinstance(event_ts, dt.datetime) else float(event_ts),
        )

    def _emit_event_sync(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        agent_urn: Optional[str],
        ts: Optional[dt.datetime],
        ts_epoch: float,
    ) -> None:
        with self._pool.acquire() as conn:
            params = {
                "ts": ts,
                "kind": kind,
                "payload": _json_dumps(payload),
                "agent_urn": agent_urn,
            }
            sql = """
            INSERT INTO hive_events (ts, kind, payload, agent_urn)
            VALUES (:ts, :kind, :payload, :agent_urn)
            """
            try:
                self._execute(conn, sql, params)
            except Exception as exc:
                if "ORA-00932" not in str(exc) or "NUMBER" not in str(exc):
                    raise
                params["ts"] = ts_epoch
                self._execute(conn, sql, params)
            conn.commit()

    async def tail_events(
        self,
        *,
        kind: Optional[str] = None,
        agent_urn: Optional[str] = None,
        since_ts: Optional[float] = None,
        limit: int = 100,
        **_legacy: Any,
    ) -> list[dict[str, Any]]:
        """Recent events from hive_events (option B: reuse agent_bus-owned table)."""
        return await self._run(
            self._tail_events_sync,
            kind=kind,
            agent_urn=agent_urn,
            since_ts=since_ts,
            limit=int(limit),
        )

    def _tail_events_sync(
        self,
        *,
        kind: Optional[str],
        agent_urn: Optional[str],
        since_ts: Optional[float],
        limit: int,
    ) -> list[dict[str, Any]]:
        sql = "SELECT id, ts, kind, payload, agent_urn FROM hive_events WHERE 1=1"
        params: dict[str, Any] = {}
        if kind is not None:
            sql += " AND kind = :kind"
            params["kind"] = kind
        if agent_urn is not None:
            sql += " AND agent_urn = :agent_urn"
            params["agent_urn"] = agent_urn
        if since_ts is not None:
            sql += " AND ts >= :since_ts"
            params["since_ts"] = _ts_to_tstz(since_ts)
        sql += " ORDER BY ts DESC, id DESC FETCH FIRST :lim ROWS ONLY"
        params["lim"] = max(1, min(int(limit), 5000))
        with self._pool.acquire() as conn:
            cur = self._execute(conn, sql, params)
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
            out: list[dict[str, Any]] = []
            for r in rows:
                payload_raw = r[3]
                try:
                    payload = (
                        payload_raw
                        if isinstance(payload_raw, (dict, list))
                        else (_json_loads(payload_raw) if payload_raw else {})
                    )
                except Exception:
                    payload = {"_raw": str(payload_raw)}
                out.append(
                    {
                        "id": int(r[0]) if r[0] is not None else 0,
                        "ts": _dt_to_ts(r[1]) or 0.0,
                        "kind": r[2],
                        "payload": payload,
                        "agent_urn": r[4],
                    }
                )
            return out

    async def cache_record_hit(
        self,
        *,
        cache_key: str,
        delta: int = 1,
        **_legacy: Any,
    ) -> None:
        """Atomically increment the canonical cache hit counter."""
        await self._run(
            self._cache_record_hit_sync,
            cache_key=cache_key,
            delta=int(delta),
        )

    def _cache_record_hit_sync(
        self,
        *,
        cache_key: str,
        delta: int,
    ) -> None:
        with self._pool.acquire() as conn:
            self._execute(
                conn,
                "UPDATE hive_cache SET hit_count=hit_count+:delta, last_hit_at=SYSTIMESTAMP "
                "WHERE cache_key=:ck",
                {"delta": int(delta), "ck": cache_key},
            )
            conn.commit()

    async def stats_costs(
        self,
        *,
        since_ts: Optional[float] = None,
        limit: int = 100,
        **_legacy: Any,
    ) -> dict[str, Any]:
        """Aggregate canonical worker statistics by worker and kind."""
        return await self._run(
            self._stats_costs_sync,
            since_ts=since_ts,
            limit=int(limit),
        )

    def _stats_costs_sync(
        self,
        *,
        since_ts: Optional[float],
        limit: int,
    ) -> dict[str, Any]:
        where = ""
        params: dict[str, Any] = {}
        if since_ts is not None:
            where = " WHERE last_run >= :since_ts"
            params["since_ts"] = _ts_to_tstz(since_ts)
        with self._pool.acquire() as conn:
            cur = self._execute(
                conn,
                "SELECT NVL(SUM(total_tokens_in),0), NVL(SUM(total_tokens_out),0), "
                "NVL(SUM(total_cost_usd),0), COUNT(DISTINCT urn), "
                "COUNT(DISTINCT kind) FROM hive_worker_kind_stats" + where,
                params,
            )
            tr = cur.fetchone() if hasattr(cur, "fetchone") else None
            total = {
                "tokens_in": int(tr[0]) if tr else 0,
                "tokens_out": int(tr[1]) if tr else 0,
                "est_cost_usd": float(tr[2]) if tr else 0.0,
                "distinct_agents": int(tr[3]) if tr else 0,
                "distinct_kinds": int(tr[4]) if tr else 0,
            }
            lim = max(1, min(int(limit), 1000))
            params_lim = dict(params)
            params_lim["lim"] = lim
            cur = self._execute(
                conn,
                "SELECT urn, SUM(total_tokens_in), SUM(total_tokens_out), "
                "SUM(total_cost_usd) FROM hive_worker_kind_stats" + where + " GROUP BY urn ORDER BY 4 DESC "
                "FETCH FIRST :lim ROWS ONLY",
                params_lim,
            )
            by_agent = [
                {
                    "agent_urn": r[0],
                    "tokens_in": int(r[1]) if r[1] is not None else 0,
                    "tokens_out": int(r[2]) if r[2] is not None else 0,
                    "est_cost_usd": float(r[3]) if r[3] is not None else 0.0,
                }
                for r in (cur.fetchall() if hasattr(cur, "fetchall") else [])
            ]
            cur = self._execute(
                conn,
                "SELECT kind, SUM(total_tokens_in), SUM(total_tokens_out), "
                "SUM(total_cost_usd) FROM hive_worker_kind_stats" + where + " GROUP BY kind ORDER BY 4 DESC "
                "FETCH FIRST :lim ROWS ONLY",
                params_lim,
            )
            by_kind = [
                {
                    "kind": r[0],
                    "tokens_in": int(r[1]) if r[1] is not None else 0,
                    "tokens_out": int(r[2]) if r[2] is not None else 0,
                    "est_cost_usd": float(r[3]) if r[3] is not None else 0.0,
                }
                for r in (cur.fetchall() if hasattr(cur, "fetchall") else [])
            ]
            return {"total": total, "by_agent": by_agent, "by_kind": by_kind}

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
        await self._run(
            self._record_worker_kind_stats_sync,
            urn=urn,
            kind=kind,
            success_delta=success_delta,
            fail_delta=fail_delta,
            cancelled_delta=cancelled_delta,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            cost_usd=cost_usd,
            duration_sec=duration_sec,
            last_run=last_run,
        )

    def _record_worker_kind_stats_sync(
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
        with self._pool.acquire() as conn:
            self._execute(
                conn,
                """
                MERGE INTO hive_worker_kind_stats dst
                USING (
                  SELECT :urn AS urn,
                         :kind AS kind,
                         :success_delta AS success_count,
                         :fail_delta AS fail_count,
                         :cancelled_delta AS cancelled_count,
                         :tokens_in AS total_tokens_in,
                         :tokens_out AS total_tokens_out,
                         :cost_usd AS total_cost_usd,
                         :duration_sec AS total_duration_sec,
                         :last_run AS last_run
                  FROM dual
                ) src
                ON (dst.urn = src.urn AND dst.kind = src.kind)
                WHEN MATCHED THEN UPDATE SET
                  dst.success_count = dst.success_count + src.success_count,
                  dst.fail_count = dst.fail_count + src.fail_count,
                  dst.cancelled_count = dst.cancelled_count + src.cancelled_count,
                  dst.total_tokens_in = dst.total_tokens_in + src.total_tokens_in,
                  dst.total_tokens_out = dst.total_tokens_out + src.total_tokens_out,
                  dst.total_cost_usd = dst.total_cost_usd + src.total_cost_usd,
                  dst.total_duration_sec = dst.total_duration_sec + src.total_duration_sec,
                  dst.last_run = src.last_run
                WHEN NOT MATCHED THEN INSERT (
                  urn, kind, success_count, fail_count, cancelled_count,
                  total_tokens_in, total_tokens_out, total_cost_usd,
                  total_duration_sec, last_run
                ) VALUES (
                  src.urn, src.kind, src.success_count, src.fail_count,
                  src.cancelled_count, src.total_tokens_in, src.total_tokens_out,
                  src.total_cost_usd, src.total_duration_sec, src.last_run
                )
                """,
                {
                    "urn": urn,
                    "kind": kind,
                    "success_delta": int(success_delta),
                    "fail_delta": int(fail_delta),
                    "cancelled_delta": int(cancelled_delta),
                    "tokens_in": int(tokens_in),
                    "tokens_out": int(tokens_out),
                    "cost_usd": float(cost_usd),
                    "duration_sec": float(duration_sec),
                    "last_run": _ts_to_tstz(last_run),
                },
            )
            conn.commit()

    async def stats_workers(
        self,
        *,
        kind: Optional[str] = None,
        since_ts: Optional[float] = None,
        limit: int = 100,
        **_legacy: Any,
    ) -> list[dict[str, Any]]:
        """List canonical per-worker/per-kind aggregate rows."""
        return await self._run(
            self._stats_workers_sync,
            kind=kind,
            since_ts=since_ts,
            limit=int(limit),
        )

    def _stats_workers_sync(
        self,
        *,
        kind: Optional[str],
        since_ts: Optional[float],
        limit: int,
    ) -> list[dict[str, Any]]:
        sql = (
            "SELECT urn, kind, success_count + fail_count + cancelled_count, "
            "success_count, fail_count, total_tokens_in, total_tokens_out, "
            "total_cost_usd, last_run FROM hive_worker_kind_stats WHERE 1=1"
        )
        params: dict[str, Any] = {}
        if kind is not None:
            sql += " AND kind = :kind"
            params["kind"] = kind
        if since_ts is not None:
            sql += " AND last_run >= :since_ts"
            params["since_ts"] = _ts_to_tstz(since_ts)
        sql += " ORDER BY last_run DESC, total_cost_usd DESC FETCH FIRST :lim ROWS ONLY"
        params["lim"] = max(1, min(int(limit), 1000))
        with self._pool.acquire() as conn:
            cur = self._execute(conn, sql, params)
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
            return [
                {
                    "agent_urn": r[0],
                    "kind": r[1],
                    "claims": int(r[2]) if r[2] is not None else 0,
                    "completions": int(r[3]) if r[3] is not None else 0,
                    "failures": int(r[4]) if r[4] is not None else 0,
                    "tokens_in": int(r[5]) if r[5] is not None else 0,
                    "tokens_out": int(r[6]) if r[6] is not None else 0,
                    "est_cost_usd": float(r[7]) if r[7] is not None else 0.0,
                    "last_seen_at": _dt_to_ts(r[8]) or 0.0,
                }
                for r in rows
            ]


__all__ = [
    "ORACLE_SCHEMA_VERSION",
    "OracleHiveMindRepository",
    "OracleSchemaCompatibilityError",
]
