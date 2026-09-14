"""
GRAEAE Hive Mind — fleet-wide MCP-compatible agent coordination + triage queue.

Brand: GRAEAE Hive Mind (extension of GRAEAE consensus engine — sister to mnemos memory)
Identity: urn:agent:<kind>:<host>:<session_uuid>   (kinds: claude, goose, opencode, codex, zeroclaw, openclaw, hermes, human, ic-engine, mnemos, ...)
Backend: SQLite WAL (Phase 1) — PG migration documented for Phase 2 when >50 agents OR LISTEN/NOTIFY needed
Pub/sub: SSE (Phase 1) — NATS migration Phase 2 when >20 concurrent OR >100 msg/s
Triage queue: /v1/jobs/next dequeues highest-priority eligible work; no central scheduler — agents self-claim.
"""

from __future__ import annotations
import asyncio
import hashlib
import json
import os
import re
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from importlib.resources import files
from typing import Optional

import aiosqlite
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from mnemos.hive_mind.config import agent_bus_db_env

DB_PATH = agent_bus_db_env()
HIVE_BUS_TOKEN_ENV_NAMES = ("HIVE_BUS_TOKEN", "MNEMOS_HIVE_BUS_TOKEN")
HIVE_ADMIN_TOKEN_ENV_NAMES = ("HIVE_ADMIN_TOKEN", "MNEMOS_HIVE_ADMIN_TOKEN")
HIVE_BUS_AUTH_REQUIRED_ENV_NAMES = ("HIVE_BUS_AUTH_REQUIRED", "MNEMOS_HIVE_BUS_AUTH_REQUIRED")
_FALSE_ENV_VALUES = {"0", "false", "no", "off"}
_PUBLIC_PATHS = {"/health", "", "/dashboard.js"}  # dashboard assets collect the API token in-browser
# F02 (2026-09-14): bootstrap credentials are registration-only. They may only
# be used to mint a new agent/worker identity via POST /v1/agents/register.
# Anything else (reading events/messages/jobs, hitting SSE, mutating jobs,
# publishing messages, MCP, ...) is forbidden — an authenticated worker with a
# proper agent_token never needs the bootstrap credential for any of those.
_BOOTSTRAP_ALLOWED_PATHS = frozenset({"/v1/agents/register"})


def _positive_env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, str(default))))
    except ValueError:
        return default

# Phase 2 migration cut 1 (2026-05-23): storage abstraction. SQL methods
# migrate into the repo one at a time; service-level helpers forward.
# See mnemos/hive_mind/repository.py for the Protocol + cut plan.
from mnemos.hive_mind.repository import SqliteHiveMindRepository  # noqa: E402

_REPO = SqliteHiveMindRepository(DB_PATH)
HEARTBEAT_REAP_INTERVAL = 30.0
HEARTBEAT_DEAD_AFTER = 60.0
EVENTS_RETAIN_HOURS = 168  # 7 days
SYSTEM_LOAD_RETAIN_HOURS = max(0.0, float(os.environ.get("HIVE_SYSTEM_LOAD_RETAIN_HOURS", "0")))
SSE_PING_INTERVAL = 15.0
MAX_RESULT_PAGE_SIZE = _positive_env_int("HIVE_MAX_RESULT_PAGE_SIZE", 1000)
SSE_CATCHUP_PAGE_SIZE = _positive_env_int("HIVE_SSE_CATCHUP_PAGE_SIZE", 500)
SSE_EVENT_QUEUE_SIZE = _positive_env_int("HIVE_SSE_EVENT_QUEUE_SIZE", 1000)
DASHBOARD_LIVE_REFRESH_SECONDS = _positive_env_int("HIVE_DASHBOARD_LIVE_REFRESH_SECONDS", 5)
DASHBOARD_ANALYTICS_REFRESH_SECONDS = _positive_env_int(
    "HIVE_DASHBOARD_ANALYTICS_REFRESH_SECONDS", DASHBOARD_LIVE_REFRESH_SECONDS
)
EVENT_QUEUE: dict[str, asyncio.Queue] = {}  # subscriber_id -> queue


# ---------- helpers ----------


def _env_bool(names: tuple[str, ...], *, default: bool) -> bool:
    for name in names:
        raw = os.environ.get(name)
        if raw is not None:
            return raw.strip().lower() not in _FALSE_ENV_VALUES
    return default


def _bounded_result_limit(limit: int) -> int:
    return max(1, min(int(limit), MAX_RESULT_PAGE_SIZE))


def _configured_bus_tokens() -> tuple[str, ...]:
    tokens: list[str] = []
    for name in HIVE_BUS_TOKEN_ENV_NAMES:
        raw = os.environ.get(name, "")
        tokens.extend(token.strip() for token in raw.split(",") if token.strip())
    return tuple(dict.fromkeys(tokens))


def _configured_admin_tokens() -> tuple[str, ...]:
    tokens: list[str] = []
    for name in HIVE_ADMIN_TOKEN_ENV_NAMES:
        raw = os.environ.get(name, "")
        tokens.extend(token.strip() for token in raw.split(",") if token.strip())
    return tuple(dict.fromkeys(tokens))


def _hive_bus_auth_required() -> bool:
    return _env_bool(HIVE_BUS_AUTH_REQUIRED_ENV_NAMES, default=True)


def _bearer_token(request: Request) -> str | None:
    value = request.headers.get("authorization", "")
    scheme, _, token = value.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()


def _request_has_valid_bus_token(request: Request, expected_tokens: tuple[str, ...]) -> bool:
    supplied = _bearer_token(request)
    if supplied is None:
        return False
    return any(secrets.compare_digest(supplied, expected) for expected in expected_tokens)


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _request_role(request: Request) -> str:
    return getattr(request.state, "hive_auth_role", "disabled" if not _hive_bus_auth_required() else "unknown")


def _require_admin(request: Request) -> None:
    if _request_role(request) == "disabled":
        return
    if not _configured_admin_tokens():
        raise HTTPException(503, "admin authentication is required, but no HIVE_ADMIN_TOKEN is configured")
    if _request_role(request) != "admin":
        raise HTTPException(403, "admin credential required")


def _require_registration_authority(request: Request) -> None:
    if _request_role(request) == "disabled":
        return
    if _request_role(request) not in {"bootstrap", "admin"}:
        raise HTTPException(403, "fleet bootstrap credential required")


def _require_agent(request: Request, claimed_urn: str | None = None) -> str:
    if _request_role(request) == "disabled":
        if not claimed_urn:
            raise HTTPException(403, "agent identity is required")
        return claimed_urn
    if _request_role(request) != "agent":
        raise HTTPException(403, "per-agent credential required")
    caller = getattr(request.state, "agent_urn", None)
    if not caller:
        raise HTTPException(403, "per-agent credential is not bound to an agent")
    if claimed_urn and not secrets.compare_digest(caller, claimed_urn):
        raise HTTPException(403, "claimed agent identity does not match authenticating credential")
    return caller


def _require_owner_or_admin(request: Request, owner_urn: str) -> str:
    if _request_role(request) == "admin":
        return owner_urn
    return _require_agent(request, owner_urn)


def _internal_agent_request(agent_urn: str) -> Request:
    request = Request({"type": "http", "method": "POST", "path": "/internal", "headers": []})
    request.state.hive_auth_role = "agent"
    request.state.agent_urn = agent_urn
    return request


def uuidv7() -> str:
    """Time-ordered UUID for monotonic index inserts."""
    ts_ms = int(time.time() * 1000)
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    val = (ts_ms & ((1 << 48) - 1)) << 80 | (0x7 << 76) | (rand_a << 64) | (0x2 << 62) | rand_b
    return str(uuid.UUID(int=val))


def make_urn(kind: str, host: str, session_id: str) -> str:
    return f"urn:agent:{kind}:{host}:{session_id}"


async def emit_event(db, kind: str, payload: dict) -> None:
    ts = time.time()
    payload_json = json.dumps(payload, separators=(",", ":"))
    await db.execute(
        "INSERT INTO events (ts, kind, payload, agent_urn) VALUES (?, ?, ?, ?)",
        (ts, kind, payload_json, payload.get("urn") or payload.get("agent_urn")),
    )
    await db.commit()
    # broadcast to live SSE subscribers
    for q in list(EVENT_QUEUE.values()):
        try:
            q.put_nowait({"kind": kind, "ts": ts, "payload": payload})
        except asyncio.QueueFull:
            pass


# ---------- schema ----------

SCHEMA = """
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
CREATE INDEX IF NOT EXISTS idx_agents_status ON agents(status);
CREATE INDEX IF NOT EXISTS idx_agents_kind ON agents(kind);

CREATE TABLE IF NOT EXISTS agent_credentials (
  urn TEXT PRIMARY KEY REFERENCES agents(urn) ON DELETE CASCADE,
  token_hash TEXT NOT NULL UNIQUE,
  issued_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  submitter_urn TEXT NOT NULL,                  -- who created the work (user/human OR delegating agent)
  parent_job_id TEXT,
  kind TEXT NOT NULL,                           -- code-edit/research/review/build/test/etc
  description TEXT,
  priority INTEGER NOT NULL DEFAULT 0,          -- higher = more urgent
  deadline REAL,
  required_capabilities TEXT,                   -- json array; worker must have ALL
  eligible_kinds TEXT,                          -- json array; agent kinds eligible (null = any)
  project TEXT,                                 -- #10 FIX: separate project tag from capabilities (riskyeats/investorclaw/etc)
  max_cost_tier TEXT NOT NULL DEFAULT 'A',
  preferred_providers TEXT,
  preferred_models TEXT,
  mnemos_refs TEXT,
  depends_on TEXT,
  status TEXT NOT NULL CHECK(status IN ('queued','offered','claimed','running','done','failed','cancelled')),
  claimed_by TEXT,                              -- worker urn (set on claim/dequeue)
  claimed_at REAL,
  claimed_runtime TEXT,
  claimed_model TEXT,
  claimed_provider TEXT,
  claimed_cost_tier TEXT,
  started_at REAL NOT NULL,                     -- when job ENTERED queue
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
  attempt_generation INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_submitter ON jobs(submitter_urn);
CREATE INDEX IF NOT EXISTS idx_jobs_claimed_by ON jobs(claimed_by);
CREATE INDEX IF NOT EXISTS idx_jobs_parent ON jobs(parent_job_id);
CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(status, priority DESC, started_at ASC);
CREATE INDEX IF NOT EXISTS idx_jobs_project ON jobs(project);  -- #10 FIX
CREATE INDEX IF NOT EXISTS idx_jobs_retry_backoff ON jobs(retry_backoff_until);
CREATE INDEX IF NOT EXISTS idx_jobs_costs ON jobs(ended_at, claimed_provider, claimed_model);

-- F03 (2026-09-14): immutable per-claim audit record. One row per claim
-- attempt (including reaped/requeued/cancelled attempts), so a stale
-- completion can be diagnosed after the fact, not just rejected at the
-- CAS boundary.
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

CREATE TABLE IF NOT EXISTS scheduled_jobs (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_by_urn TEXT NOT NULL,
  interval_seconds INTEGER NOT NULL,
  job_template TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  last_fired_at REAL,
  next_fire_at REAL NOT NULL,
  fire_count INTEGER NOT NULL DEFAULT 0,
  created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_due ON scheduled_jobs(enabled, next_fire_at);

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

CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  from_urn TEXT NOT NULL,
  to_urn TEXT,
  in_reply_to TEXT,
  topic TEXT NOT NULL,
  payload TEXT NOT NULL,
  ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_to ON messages(to_urn);
CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages(topic);
CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts REAL NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  agent_urn TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_kind ON events(kind);
CREATE INDEX IF NOT EXISTS idx_events_agent ON events(agent_urn);
"""


# ---------- models ----------

# Runtime → eligible kinds map (Kimi-K2.6 advisory 2026-05-23).
RUNTIME_KIND_MAP: dict[str, set[str]] = {
    "claude-code": {"claude", "claude-code"},
    "claude-cli": {"claude", "claude-code"},
    "opencode": {"opencode"},  # opencode CAN run any model — kind=opencode
    "opencode-cli": {"opencode"},
    # goose RETIRED 2026-05-25 — see ~/.claude/rules/agentic-cli-usage-patterns-2026-05-23.md
    "codex": {"codex"},
    "codex-cli": {"codex"},
    "hermes": {"hermes"},
    "zeroclaw": {"zeroclaw"},
    "openclaw": {"openclaw"},
    "ic-engine": {"ic-engine"},
    "mnemos": {"mnemos"},
    "human": {"human"},
    "claude": {"claude"},
    "system": {"system"},  # fleet hosts (ARGOS/TYPHON/HYDRA/MEDUSA/CERBERUS/PROTEUS/cixmini)
    "unknown": {"unknown"},
}
AUTONOMY_LEVELS = {"autonomous", "confirm-risky", "interactive", "unknown"}
AUTH_METHODS = {"subscription", "api", "free", "unknown"}
# Plan caps (USD/month) per CLAUDE.md:
DEFAULT_PLAN_CAPS = {
    "subscription": 200.0,  # Anthropic Max plan ($200 until 2026-05-31, $100 from 2026-06-01 — operator updates)
    "api": 1000.0,  # pay-per-token has no hard cap; treat as high ceiling for safety
    "free": 0.0,  # no cap (no cost)
    "unknown": 50.0,  # conservative
}
THROTTLE_HEADROOM = 0.85  # at >=85% of plan cap, prefer non-subscription workers for tier-B/C jobs

# ROLE SPLIT (user directive 2026-05-23, goose retired 2026-05-25):
# opencode + codex + hermes + claw-family + ic-engine + unknown = WORKERS (claim-only).
# Cannot submit jobs. Orchestrators: claude-code, human, mnemos. They submit work; workers execute it.
WORKER_ONLY_RUNTIMES: set[str] = {
    "opencode",
    "opencode-cli",
    # goose RETIRED 2026-05-25
    "codex",
    "codex-cli",
    "hermes",
    "zeroclaw",
    "openclaw",
    "ic-engine",
    "system",  # fleet hosts (system-watcher daemons) — sensors + optional build/ci workers, never submitters
}
ORCHESTRATOR_RUNTIMES: set[str] = {
    "claude-code",
    "claude-cli",
    "claude",
    "human",
    "mnemos",
    "unknown",
}

# COST-TIER MAP (per ~/.claude/rules/llm-usage-policy-2026-05-22.md):
#   A = FREE   — local + NGC NIM (try first, token-miser)
#   B = CHEAP  — Groq Dev tier, xAI, DeepSeek direct, Together cheap, Gemini-Flash, OpenAI-mini
#   C = RESERVE — Anthropic Opus/Sonnet, OpenAI GPT-5.5/Pro, Gemini Pro, Together DeepSeek-Pro
#                 (Together DeepSeek-V4-Pro = anti-pattern — use DeepSeek direct instead)
PROVIDER_COST_TIER: dict[str, str] = {
    "ngc": "A",
    "nvidia": "A",
    "nvidia-ngc": "A",
    "local-llamacpp": "A",
    "local-vllm": "A",
    "ollama": "A",
    "ollama-cerberus": "A",
    "local": "A",
    "groq": "B",
    "xai": "B",
    "deepseek": "B",
    "deepseek-direct": "B",
    "siliconflow": "B",
    "together": "B",  # default — only MiniMax + cheap models; DO NOT default to Together DeepSeek-V4-Pro
    "gemini-flash": "B",
    "openai-mini": "B",
    "perplexity": "B",
    "anthropic": "C",
    "openai": "C",
    "openai-pro": "C",
    "openai-gpt55": "C",
    "gemini": "C",
    "gemini-pro": "C",
    "together-pro": "C",
    "unknown": "C",  # treat as expensive until classified
}
COST_TIERS = ["A", "B", "C"]


def cost_tier_for(provider: str) -> str:
    return PROVIDER_COST_TIER.get((provider or "unknown").lower(), "C")


# Per-million-token rates (USD). Workers SHOULD report tokens_in/tokens_out in PATCH result;
# hive computes estimated_cost_usd. Wildcard model "*" applies to any model on that provider.
# Source: ~/.claude/rules/llm-usage-policy-2026-05-22.md
LLM_RATES: dict[tuple[str, str], tuple[float, float]] = {
    ("anthropic", "claude-opus-4-7"): (5.00, 25.00),
    ("anthropic", "claude-opus-4-6"): (5.00, 25.00),
    ("anthropic", "claude-sonnet-4-6"): (3.00, 15.00),
    ("anthropic", "claude-sonnet-4-7"): (3.00, 15.00),
    ("anthropic", "claude-haiku-4-5"): (1.00, 5.00),
    ("anthropic", "*"): (3.00, 15.00),
    ("openai", "gpt-5.5"): (5.00, 30.00),
    ("openai", "gpt-5.5-pro"): (30.00, 180.00),
    ("openai", "gpt-5.4-nano"): (0.20, 1.25),
    ("openai", "o4-mini"): (0.55, 2.20),
    ("openai", "o3"): (2.00, 8.00),
    ("openai", "*"): (5.00, 30.00),
    ("xai", "grok-4.3"): (1.25, 2.50),
    ("xai", "grok-4.1-fast"): (0.20, 0.50),
    ("xai", "grok-4.20"): (2.00, 6.00),
    ("xai", "*"): (1.25, 2.50),
    ("groq", "llama-3.3-70b-versatile"): (0.59, 0.79),
    ("groq", "llama-3.1-8b-instant"): (0.05, 0.08),
    ("groq", "llama-4-scout-17b"): (0.11, 0.34),
    ("groq", "qwen3-32b"): (0.29, 0.59),
    ("groq", "gpt-oss-120b"): (0.15, 0.60),
    ("groq", "gpt-oss-20b"): (0.075, 0.30),
    ("groq", "*"): (0.29, 0.59),
    ("deepseek-direct", "deepseek-v4-pro"): (0.435, 0.87),
    ("deepseek-direct", "deepseek-v4-flash"): (0.14, 0.28),
    ("deepseek-direct", "*"): (0.435, 0.87),
    ("deepseek", "*"): (0.435, 0.87),
    ("together", "minimax-m2.7"): (0.40, 1.20),
    ("together", "deepseek-v4-pro"): (2.10, 4.40),
    ("together", "kimi-k2.6"): (1.20, 4.40),
    ("together", "glm-3.5-90"): (0.10, 0.15),
    ("together", "qwen2.5-coder-32b"): (0.05, 0.12),
    ("together", "*"): (0.40, 1.20),
    ("gemini", "gemini-2.5-flash-lite"): (0.10, 0.40),
    ("gemini", "gemini-2.5-flash"): (0.30, 2.50),
    ("gemini", "gemini-3.1-pro"): (2.00, 12.00),
    ("gemini", "*"): (1.25, 10.00),
    ("perplexity", "sonar"): (1.00, 1.00),
    ("perplexity", "sonar-pro"): (3.00, 15.00),
    ("perplexity", "*"): (1.00, 1.00),
}


def rate_for(provider: str, model: str) -> tuple[float, float]:
    p = (provider or "unknown").lower()
    m = (model or "").lower()
    # exact match first
    if (p, m) in LLM_RATES:
        return LLM_RATES[(p, m)]
    # wildcard model on provider
    if (p, "*") in LLM_RATES:
        return LLM_RATES[(p, "*")]
    # tier A providers = free
    if cost_tier_for(p) == "A":
        return (0.0, 0.0)
    return (1.0, 5.0)  # conservative fallback


def estimate_cost(provider: str, model: str, tokens_in: int, tokens_out: int) -> float:
    in_rate, out_rate = rate_for(provider, model)
    return round((tokens_in / 1_000_000) * in_rate + (tokens_out / 1_000_000) * out_rate, 6)


def hallucination_check(result: dict) -> Optional[str]:
    """Return reason string if result looks like an LLM token-loop / hallucination,
    else None. Saves marking-done garbage that wastes a cache entry + claims completion.

    Heuristics:
    - >40% of stdout is a single repeated word/phrase
    - stdout >1500 chars with <50 unique words (Kimi-K2.6 'extension extension extension' loop)
    - stdout contains 'Rate limit exceeded' / 'Authentication error' / 'context-length exceeded'
    """
    if not isinstance(result, dict):
        return None
    stdout = result.get("stdout") or result.get("output") or ""
    if not isinstance(stdout, str) or len(stdout) < 200:
        return None
    if "rate limit exceeded" in stdout.lower():
        return "rate_limit_in_output"
    if "authentication error" in stdout.lower() or "authentication failed" in stdout.lower():
        return "auth_error_in_output"
    if "context length" in stdout.lower() and "exceed" in stdout.lower():
        return "context_overflow_in_output"
    # token-loop detection
    words = stdout.split()
    if len(words) > 200:
        from collections import Counter

        top_word, top_count = Counter(words).most_common(1)[0]
        if top_count / len(words) > 0.4:
            return f"token_loop:{top_word!r}_repeated_{top_count}_of_{len(words)}"
    return None


# RESULT CACHE — Nemotron killer feature 2026-05-23.
# Memoize (kind, description, max_cost_tier, sorted-required-capabilities) → result.
# When identical job submitted, return cached result instantly. Cuts LLM spend
# 30-70% on repetitive work + avoids NGC 429 storms from duplicate dispatches.
import hashlib as _hashlib

CACHE_TTL_SECONDS = 24 * 3600  # 24h default; idempotent work like compiles/lints often valid much longer

# F12 (2026-09-14): upper bound on the serialized JSON size we are willing to
# write into hive_cache.result_json. Historically the cache silently truncated
# at 32_000 characters, which produced invalid JSON and poisoned subsequent
# cache reads with a json.JSONDecodeError. A cache miss is always safe; a
# corrupted cache hit is not. Oversized payloads are simply not cached.
CACHE_RESULT_JSON_MAX_BYTES = 1 * 1024 * 1024  # 1 MiB serialized JSON


def cache_key_for(
    kind: str,
    description: Optional[str],
    max_cost_tier: str,
    required_capabilities: Optional[list[str]],
    *,
    submitter_urn: Optional[str] = None,
    parent_job_id: Optional[str] = None,
    priority: int = 0,
    deadline: Optional[float] = None,
    eligible_kinds: Optional[list[str]] = None,
    project: Optional[str] = None,
    preferred_providers: Optional[list[str]] = None,
    preferred_models: Optional[list[str]] = None,
    mnemos_refs: Optional[list[str]] = None,
    depends_on: Optional[list[str]] = None,
    max_cost_tier_explicit: Optional[bool] = None,
    cache_context: Optional[str] = None,
) -> str:
    # Versioned canonical representation: every field that can affect routing,
    # available context, or the worker-visible job is part of equivalence.
    payload = json.dumps(
        {
            "schema": 4,
            "kind": kind,
            "description": (description or "").strip(),
            "max_cost_tier": max_cost_tier,
            "required_capabilities": sorted(required_capabilities or []),
            "submitter_urn": submitter_urn,
            "parent_job_id": parent_job_id,
            "priority": priority,
            "deadline": deadline,
            "eligible_kinds": sorted(eligible_kinds or []),
            "project": project,
            "preferred_providers": preferred_providers or [],
            "preferred_models": preferred_models or [],
            "mnemos_refs": mnemos_refs or [],
            "depends_on": depends_on or [],
            "max_cost_tier_explicit": max_cost_tier_explicit,
            "cache_context": cache_context,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return _hashlib.sha256(payload.encode()).hexdigest()


async def cache_lookup(db, cache_key: str) -> Optional[dict]:
    cutoff = time.time() - CACHE_TTL_SECONDS
    async with db.execute(
        "SELECT result_json, source_job_id, result_mnemos_id, hit_count, cost_saved_usd, model, provider, cached_at "
        "FROM hive_cache WHERE cache_key=? AND cached_at >= ?",
        (cache_key, cutoff),
    ) as cur:
        r = await cur.fetchone()
    if not r:
        return None
    raw = r[0]
    # F12 (2026-09-14): older builds could write a 32_000-char-truncated slice
    # of result_json to the cache, which is no longer valid JSON. Treat any
    # decode failure here as a cache miss and evict the poisoned row so the
    # next write of the same key gets a clean slate.
    try:
        decoded = json.loads(raw) if raw else None
    except (ValueError, TypeError) as exc:
        print(
            f"cache_lookup: evicting malformed cache entry "
            f"cache_key={cache_key[:16]}... err={exc!r}",
            flush=True,
        )
        try:
            await db.execute(
                "DELETE FROM hive_cache WHERE cache_key=?",
                (cache_key,),
            )
            await db.commit()
        except Exception as del_exc:  # noqa: BLE001 — best-effort eviction
            print(
                f"cache_lookup: failed to evict malformed entry "
                f"cache_key={cache_key[:16]}... del_err={del_exc!r}",
                flush=True,
            )
        return None
    return {
        "result": decoded,
        "source_job_id": r[1],
        "result_mnemos_id": r[2],
        "hit_count": r[3],
        "cost_saved_usd": r[4],
        "model": r[5],
        "provider": r[6],
        "cached_at": r[7],
    }


async def cache_store(
    db,
    cache_key: str,
    source_job_id: str,
    result: dict,
    result_mnemos_id: Optional[str],
    model: str,
    provider: str,
    cost_for_save: float,
):
    now = time.time()
    serialized = json.dumps(result, default=str)
    # F12 (2026-09-14): never silently write a truncated/invalid JSON string
    # to the cache. If the serialized payload exceeds the size bound, skip
    # caching entirely — a cache miss is safe, a corrupted cache hit is not.
    # See CACHE_RESULT_JSON_MAX_BYTES for the rationale.
    if len(serialized.encode("utf-8")) > CACHE_RESULT_JSON_MAX_BYTES:
        print(
            f"cache_store: skipping oversized cache entry "
            f"cache_key={cache_key[:16]}... size={len(serialized)} "
            f"max={CACHE_RESULT_JSON_MAX_BYTES}",
            flush=True,
        )
        return
    await db.execute(
        "INSERT INTO hive_cache (cache_key, result_json, source_job_id, result_mnemos_id, "
        "hit_count, cost_saved_usd, model, provider, cached_at, last_hit_at) "
        "VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, NULL) "
        "ON CONFLICT(cache_key) DO UPDATE SET "
        "result_json=excluded.result_json, source_job_id=excluded.source_job_id, "
        "result_mnemos_id=excluded.result_mnemos_id, cached_at=excluded.cached_at, "
        "model=excluded.model, provider=excluded.provider",
        (cache_key, serialized, source_job_id, result_mnemos_id, model, provider, now),
    )


async def cache_record_hit(db, cache_key: str, cost_saved: float):
    await db.execute(
        "UPDATE hive_cache SET hit_count = hit_count + 1, "
        "cost_saved_usd = COALESCE(cost_saved_usd,0) + ?, last_hit_at = ? "
        "WHERE cache_key=?",
        (cost_saved, time.time(), cache_key),
    )


async def _claim_agent_context(agent_urn: str) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT kind, capabilities, runtime, model, provider, cost_tier, "
            "auth_method, plan_cap_usd, plan_period_used_usd, subscription_pools "
            "FROM agents WHERE urn=? AND status IN ('online','idle')",
            (agent_urn,),
        ) as cur:
            row = await cur.fetchone()
    if not row:
        raise HTTPException(404, f"agent not registered or offline: {agent_urn}")
    agent_kind, caps_json, a_runtime, a_model, a_provider, a_tier, a_auth, a_cap, a_used, pools_json = row
    agent_caps = set(json.loads(caps_json)) if caps_json else set()
    a_tier = a_tier or "C"
    executable_tiers = [tier for tier in COST_TIERS if f"cost-tier:{tier}" in agent_caps]
    if executable_tiers:
        # A fallback-capable worker may have a paid primary identity while still
        # being able to execute free work. The terminal result is revalidated
        # against the job cap using the provider actually used.
        a_tier = min([a_tier, *executable_tiers], key=COST_TIERS.index)
    a_auth = (a_auth or "unknown").lower()
    # Throttle: subscription agents over 85% MTD usage get refused tier-B/C jobs;
    # they can still claim tier-A (free) work. Forces API/free fallback as plan cap nears.
    sub_throttled = a_auth == "subscription" and a_cap and a_used and a_used >= THROTTLE_HEADROOM * a_cap
    return {
        "agent_kind": agent_kind,
        "agent_caps": agent_caps,
        "agent_runtime": a_runtime or "unknown",
        "agent_model": a_model or "unknown",
        "agent_provider": a_provider or "unknown",
        "agent_tier": a_tier,
        "sub_throttled": bool(sub_throttled),
        "subscription_pools": set(json.loads(pools_json)) if pools_json else set(),
    }


async def _job_is_visible_claim_candidate(row: tuple, agent: dict, now: float) -> bool:
    """Return whether an unowned queued job is presently claimable by an agent."""
    required_caps = set(json.loads(row[11])) if row[11] else set()
    if required_caps and not required_caps.issubset(agent["agent_caps"]):
        return False
    eligible_kinds = set(json.loads(row[12])) if row[12] else set()
    if eligible_kinds and agent["agent_kind"] not in eligible_kinds:
        return False
    max_tier = (row[13] or "A").upper()
    try:
        if COST_TIERS.index(agent["agent_tier"]) > COST_TIERS.index(max_tier):
            return False
    except ValueError:
        return False
    if agent["sub_throttled"] and max_tier != "A":
        return False
    depends_on = json.loads(row[17]) if row[17] else []
    if depends_on:
        placeholders = ",".join("?" for _ in depends_on)
        async with (
            aiosqlite.connect(DB_PATH) as db,
            db.execute(
                f"SELECT COUNT(*) FROM jobs WHERE id IN ({placeholders}) AND status='done'",
                tuple(depends_on),
            ) as cur,
        ):
            done_count = int((await cur.fetchone())[0])
        if done_count != len(depends_on):
            return False
    retry_backoff_until = row[20]
    if retry_backoff_until is not None and retry_backoff_until > now:
        return False
    routing_metadata = json.loads(row[19]) if row[19] else {}
    required_pools = {
        str(pool).strip().lower()
        for pool in (routing_metadata.get("required_subscription_pools") or [])
        if str(pool).strip()
    }
    agent_pools = {str(pool).strip().lower() for pool in agent["subscription_pools"]}
    return not required_pools or bool(required_pools.intersection(agent_pools))


# F02 (2026-09-14): event history, SSE live delivery, and directed messages were
# all read with no audience check, while /v1/jobs already filtered by submitter,
# claimer, or claim-eligibility. That let any authenticated worker (and the
# bootstrap token) recover the description/id/mnemos_refs of jobs they had no
# right to see. The single shared audience predicate below is the fix and is
# used by /v1/events/log, /v1/events (SSE), and /v1/messages. /v1/jobs keeps
# its original inline filter (do NOT weaken the reference implementation while
# extracting this helper).
_JOB_AUDIENCE_KINDS_PREFIX = "job."


async def _job_row_visible_to_agent(
    job_id: str, agent_urn: str | None, agent_ctx: dict | None, now: float
) -> bool:
    """Mirror /v1/jobs visibility: a job is visible to an agent iff the agent
    is the submitter, the claimer, or eligible to claim it while it is queued
    or offered and unowned. Returns False for unknown ids. Returns True for
    admin-equivalent callers (agent_ctx is None AND agent_urn is None) so
    internal callers with no per-agent identity see everything.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, submitter_urn, parent_job_id, kind, description, priority, "
            "status, claimed_by, started_at, ended_at, result, required_capabilities, "
            "eligible_kinds, max_cost_tier, preferred_providers, preferred_models, "
            "mnemos_refs, depends_on, routed_at, routing_metadata, retry_backoff_until "
            "FROM jobs WHERE id=?",
            (job_id,),
        ) as cur:
            row = await cur.fetchone()
    if row is None:
        return False
    submitter_urn = row[1]
    claimed_by = row[7]
    status = row[6]
    if agent_urn and (submitter_urn == agent_urn or claimed_by == agent_urn):
        return True
    if status in ("queued", "offered") and claimed_by is None:
        if agent_urn is None and agent_ctx is None:
            return True  # no per-agent identity attached — admin/internal
        if agent_ctx is None:
            return False
        return await _job_is_visible_claim_candidate(row, agent_ctx, now)
    return False


async def _event_visible_to_agent(
    event: dict, agent_urn: str | None, agent_ctx: dict | None, now: float
) -> bool:
    """Audience predicate for events emitted on the bus. Job lifecycle events
    ride on the referenced job's visibility (same rule as /v1/jobs). Message
    events are visible to the recipient, the sender, or the whole fleet when
    broadcast (to_urn is null). Other events (agent.*, schedule.*, heartbeat)
    are visible if addressed to the caller or broadcast.
    """
    if agent_urn is None and agent_ctx is None:
        return True  # no per-agent identity — admin/internal caller
    kind = (event.get("kind") or "").strip()
    payload = event.get("payload") or {}
    if kind.startswith(_JOB_AUDIENCE_KINDS_PREFIX) or kind == "job":
        job_id = payload.get("id")
        if not job_id:
            # Malformed event: we cannot verify audience, so deny rather than
            # fail open. A job.* event without an id is broken at the source.
            return False
        return await _job_row_visible_to_agent(job_id, agent_urn, agent_ctx, now)
    if kind == "message":
        to_urn = payload.get("to")
        from_urn = payload.get("from")
        # A direct message MUST have a recipient. A message without `to`
        # is a broadcast (visible to every authenticated agent) provided it
        # has a sender — that is the only valid shape that the publish
        # endpoint emits. A malformed message with neither `to` nor `from`
        # is denied: we cannot verify audience, so fail closed.
        if to_urn is None:
            return from_urn is not None
        if not agent_urn:
            return False
        return to_urn == agent_urn or from_urn == agent_urn
    # Other event kinds (agent.online / agent.offline, schedule.fired, …) are
    # addressed to a specific agent: visible to that agent, hidden from others.
    # Unaddressed events (no urn anywhere) are broadcast — they are fleet-wide
    # notices by convention (e.g. "schedule.fired" is operational metadata,
    # not private content). Admin/internal callers are handled by the
    # short-circuit at the top of this function.
    target = event.get("agent_urn") or payload.get("urn")
    if target is None:
        return True
    if agent_urn and target == agent_urn:
        return True
    return False


class AgentRegister(BaseModel):
    # OPEN REGISTRATION + RUNTIME→KIND ENFORCEMENT (per Kimi advisory 2026-05-23).
    # Any agent registers; identity is recorded for transparency. Kind must align
    # with runtime (prevents opencode-misregistering-as-claude). Soft fields
    # below default to "unknown" if not declared.
    #
    #   runtime          = TOOL/ORCHESTRATOR (claude-code, opencode, goose, codex, hermes, zeroclaw, openclaw, ic-engine, mnemos, human)
    #   model            = LLM (claude-opus-4-7, grok-4.3, kimi-k2.6, deepseek-v4-pro, gpt-5.5, gemini-3.1-pro, ...)
    #   provider         = INFERENCE HOST (anthropic, xai, ngc, openai, deepseek, together, groq, gemini, local-llamacpp, ...)
    #   kind             = URN routing segment — MUST be in RUNTIME_KIND_MAP[runtime] if runtime known
    #   autonomy_level   = autonomous / confirm-risky / interactive / unknown
    runtime: str = Field("unknown", pattern=r"^[a-z][a-z0-9-]{0,31}$")
    model: str = "unknown"
    provider: str = "unknown"
    host: str
    kind: Optional[str] = Field(
        None, pattern=r"^[a-z][a-z0-9-]{0,31}$", description="URN routing segment — defaults to runtime"
    )
    autonomy_level: str = Field("unknown", description="autonomous / confirm-risky / interactive / unknown")
    auth_method: str = Field("unknown", description="subscription (Max plan), api (pay-per-token), free, unknown")
    plan_cap_usd: Optional[float] = None  # monthly cap; defaults from DEFAULT_PLAN_CAPS by auth_method
    subscription_pools: Optional[list[str]] = None
    pid: Optional[int] = None
    capabilities: Optional[list[str]] = None
    version: Optional[str] = None
    metadata: Optional[dict] = None


class AgentHeartbeat(BaseModel):
    urn: str
    status: str = "online"


class JobCreate(BaseModel):
    submitter_urn: str  # who is asking (user OR delegating agent)
    parent_job_id: Optional[str] = None
    kind: str  # work type (code-edit/research/review/build/etc)
    description: Optional[str] = None
    priority: int = 0  # higher = more urgent (default 0)
    deadline: Optional[float] = None  # unix ts, optional SLA
    required_capabilities: Optional[list[str]] = None  # worker must have ALL of these
    eligible_kinds: Optional[list[str]] = None  # restrict to agent kinds; null = any
    # #10 FIX (review 2026-05-23): project tag — separate from worker capabilities.
    # 'riskyeats'/'investorclaw' are PROJECTS, not capabilities. Workers don't gain/lose
    # the ability to do work because of a project label; the label is for filter+routing.
    project: Optional[str] = None
    max_retries: int = 2  # auto-resubmit after worker reports failed (up to N times)
    # COST DISCIPLINE (per CLAUDE.md llm-usage-policy):
    max_cost_tier: str = "A"  # cap which tier may execute: A=free, B=cheap, C=reserve. Default A=free first.
    preferred_providers: Optional[list[str]] = None  # ranked preference (first match wins among tier-eligible)
    preferred_models: Optional[list[str]] = None  # ranked model preference
    # MNEMOS provenance:
    mnemos_refs: Optional[list[str]] = None  # mem_XXX ids — context/handoffs/related work the worker should consult
    # DAG support:
    depends_on: Optional[list[str]] = None  # job ids that must be status='done' before this job is dequeueable
    cacheable: bool = Field(
        False,
        description="Opt into caching after also declaring the job pure and idempotent.",
    )
    pure: bool = Field(False, description="The result has no external side effects or mutable-state dependency.")
    idempotent: bool = Field(False, description="Repeated evaluation is contractually equivalent.")
    cache_context: Optional[str] = Field(
        None,
        description="Immutable dependency/toolchain identity required when repository work opts into caching.",
    )


class JobUpdate(BaseModel):
    status: str = Field(pattern=r"^(queued|claimed|running|done|failed|cancelled)$")
    result: Optional[dict] = None
    claimed_by: Optional[str] = None
    tokens_in: Optional[int] = None  # workers SHOULD report token usage on done/failed for cost audit
    tokens_out: Optional[int] = None
    result_mnemos_id: Optional[str] = None  # mem_XXX id where worker stored the outcome — closes provenance loop
    # F03 (2026-09-14): per-claim fencing token. Workers MUST echo the
    # `claim_token` they received from the successful claim on every
    # state-changing call (running/done/failed/cancelled). A stale or missing
    # token on a terminal write is rejected with 409 — the existing
    # different-worker rejection guard is preserved on top of this.
    claim_token: Optional[str] = None


class JobRoutingUpdate(BaseModel):
    required_capabilities: Optional[list[str]] = None
    eligible_kinds: Optional[list[str]] = None
    max_cost_tier: Optional[str] = None
    preferred_providers: Optional[list[str]] = None
    preferred_models: Optional[list[str]] = None
    mnemos_refs: Optional[list[str]] = None
    depends_on: Optional[list[str]] = None
    routing_metadata: Optional[dict] = None


class ScheduleCreate(BaseModel):
    name: str
    interval_seconds: int = Field(..., ge=60, le=86400 * 30, description="60s minimum (avoid hot-loop); 30d maximum")
    job_template: dict  # full JobCreate body that will be submitted each tick
    enabled: bool = True


class MessagePublish(BaseModel):
    from_urn: str
    to_urn: Optional[str] = None
    in_reply_to: Optional[str] = None
    topic: str
    payload: dict


# ---------- lifecycle ----------

async def _reap_stale_agents_and_jobs(now: float) -> tuple[int, int]:
    """Mark stale agents offline and atomically release only their owned jobs."""
    cutoff = now - HEARTBEAT_DEAD_AFTER
    offline_count = 0
    requeued_count = 0
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT urn FROM agents WHERE status != 'offline' AND last_heartbeat < ?",
            (cutoff,),
        ) as cur:
            stale_agents = [row[0] async for row in cur]
        for urn in stale_agents:
            updated = await db.execute(
                "UPDATE agents SET status='offline' "
                "WHERE urn=? AND status != 'offline' AND last_heartbeat < ?",
                (urn, cutoff),
            )
            if updated.rowcount == 1:
                offline_count += 1
                await emit_event(db, "agent.offline", {"urn": urn, "reason": "heartbeat_timeout"})

        async with db.execute(
            "SELECT j.id, j.claimed_by FROM jobs j "
            "LEFT JOIN agents a ON a.urn=j.claimed_by "
            "WHERE j.status IN ('claimed','running') "
            "AND (a.urn IS NULL OR a.last_heartbeat < ?)",
            (cutoff,),
        ) as cur:
            orphan_candidates = [(row[0], row[1]) async for row in cur]
        for job_id, claimer in orphan_candidates:
            updated = await db.execute(
                # F03 (2026-09-14): the reaper also bumps attempt_generation so
                # any in-flight token the prior claimer is still holding is
                # GUARANTEED not to match the new generation. The prior
                # attempt_generation row in job_attempts is closed as 'reaped'
                # by the mark_attempt_reaped call below for audit.
                "UPDATE jobs SET status='queued', claimed_by=NULL, claimed_at=NULL, "
                "claimed_runtime=NULL, claimed_model=NULL, claimed_provider=NULL, "
                "claimed_cost_tier=NULL, claim_token=NULL, "
                "attempt_generation = attempt_generation + 1 "
                "WHERE id=? AND claimed_by=? AND status IN ('claimed','running') "
                "AND NOT EXISTS ("
                "SELECT 1 FROM agents WHERE urn=? AND last_heartbeat >= ?"
                ")",
                (job_id, claimer, claimer, cutoff),
            )
            if updated.rowcount == 1:
                requeued_count += 1
                await emit_event(
                    db,
                    "job.unclaimed",
                    {
                        "id": job_id,
                        "prior_claimer": claimer,
                        "reason": "worker_heartbeat_stale_or_missing",
                    },
                )
                # F03 (2026-09-14): close the prior attempt row as 'reaped' so
                # a stale completion landing after the reap cannot accidentally
                # stamp a terminal outcome on a still-in_flight attempt. Best-
                # effort: if this audit write fails, the reap itself succeeded
                # (job is queued with a fresh generation), so don't 500.
                try:
                    await _REPO.mark_attempt_reaped(
                        job_id=job_id,
                        prior_claimer=claimer,
                        reason="worker_heartbeat_stale_or_missing",
                        now=now,
                    )
                except Exception as reap_audit_exc:
                    print(
                        f"F03 reap-audit stamp failed for job={job_id}: {reap_audit_exc}",
                        flush=True,
                    )
        await db.commit()
    return offline_count, requeued_count


async def reaper_task(app: FastAPI):
    while True:
        await asyncio.sleep(HEARTBEAT_REAP_INTERVAL)
        try:
            await _reap_stale_agents_and_jobs(time.time())
            async with aiosqlite.connect(DB_PATH) as db:
                # Scheduler — fire due interval-based jobs
                now_ts = time.time()
                async with db.execute(
                    "SELECT id, name, created_by_urn, interval_seconds, job_template, fire_count "
                    "FROM scheduled_jobs WHERE enabled=1 AND next_fire_at <= ?",
                    (now_ts,),
                ) as cur:
                    due = [tuple(r) async for r in cur]
                for sched_id, sname, sub_urn, interval, tpl_json, fcount in due:
                    try:
                        tpl = json.loads(tpl_json)
                        tpl["submitter_urn"] = sub_urn
                        # synth a JobCreate + go through the cache+role machinery
                        await create_job(JobCreate(**tpl), _internal_agent_request(sub_urn))
                        await db.execute(
                            "UPDATE scheduled_jobs SET last_fired_at=?, next_fire_at=?, "
                            "fire_count=fire_count+1 WHERE id=?",
                            (now_ts, now_ts + interval, sched_id),
                        )
                        await emit_event(
                            db,
                            "schedule.fired",
                            {
                                "schedule_id": sched_id,
                                "name": sname,
                                "fire_count": fcount + 1,
                                "next_at": now_ts + interval,
                            },
                        )
                    except Exception as se:
                        print(f"scheduler error {sched_id}: {se}", flush=True)
                if due:
                    await db.commit()
                # 4. Auto-cancel stale jobs (queued > 7 days untouched)
                stale_job_cutoff = time.time() - 7 * 24 * 3600
                async with db.execute(
                    "SELECT id FROM jobs WHERE status='queued' AND started_at < ?", (stale_job_cutoff,)
                ) as cur:
                    stale_ids = [r[0] async for r in cur]
                if stale_ids:
                    await db.executemany(
                        "UPDATE jobs SET status='cancelled', ended_at=? WHERE id=?",
                        [(time.time(), j) for j in stale_ids],
                    )
                    for j in stale_ids:
                        await emit_event(db, "job.cancelled", {"id": j, "reason": "stale_>7d"})
                    await db.commit()
                # 5. Purge old events
                retain_cutoff = time.time() - EVENTS_RETAIN_HOURS * 3600
                await db.execute("DELETE FROM events WHERE ts < ?", (retain_cutoff,))
                if SYSTEM_LOAD_RETAIN_HOURS > 0:
                    load_retain_cutoff = time.time() - SYSTEM_LOAD_RETAIN_HOURS * 3600
                    await db.execute(
                        "DELETE FROM messages WHERE topic='system.load' AND ts < ?",
                        (load_retain_cutoff,),
                    )
                await db.commit()
            app.state.reaper_last_success = time.time()
            app.state.reaper_last_error = None
        except Exception as e:
            app.state.reaper_last_error = f"{type(e).__name__}: {e}"
            print(f"reaper error: {e}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    requested_backend = (
        os.environ.get("HIVE_REPOSITORY_BACKEND")
        or os.environ.get("MNEMOS_PERSISTENCE_BACKEND")
        or "sqlite"
    ).strip().lower()
    if requested_backend not in {"sqlite", "sqlite3"} or os.environ.get("HIVE_ORACLE_DSN"):
        raise RuntimeError(
            "The STIPHOS REST service currently supports only SQLite; OracleHiveMindRepository "
            "is a library API and cannot back service.py until all endpoint and lifecycle operations "
            "are repository-backed. Refusing to silently use SQLite for the requested backend."
        )
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
        await db.executescript(SCHEMA)
        # Additive migrations for live DBs. Keep this list in lockstep with
        # columns used by endpoints and SqliteHiveMindRepository.
        # Add-only via ALTER TABLE; ignore "duplicate column" errors so reruns are no-ops.
        migrations = (
            "ALTER TABLE agents ADD COLUMN runtime TEXT",
            "ALTER TABLE agents ADD COLUMN model TEXT",
            "ALTER TABLE agents ADD COLUMN provider TEXT",
            "ALTER TABLE agents ADD COLUMN cost_tier TEXT",
            "ALTER TABLE agents ADD COLUMN autonomy_level TEXT",
            "ALTER TABLE agents ADD COLUMN auth_method TEXT",
            "ALTER TABLE agents ADD COLUMN plan_cap_usd REAL",
            "ALTER TABLE agents ADD COLUMN plan_period_used_usd REAL NOT NULL DEFAULT 0",
            "ALTER TABLE agents ADD COLUMN subscription_pools TEXT",
            "ALTER TABLE jobs ADD COLUMN project TEXT",
            "ALTER TABLE jobs ADD COLUMN max_cost_tier TEXT NOT NULL DEFAULT 'A'",
            "ALTER TABLE jobs ADD COLUMN preferred_providers TEXT",
            "ALTER TABLE jobs ADD COLUMN preferred_models TEXT",
            "ALTER TABLE jobs ADD COLUMN mnemos_refs TEXT",
            "ALTER TABLE jobs ADD COLUMN depends_on TEXT",
            "ALTER TABLE jobs ADD COLUMN claimed_runtime TEXT",
            "ALTER TABLE jobs ADD COLUMN claimed_model TEXT",
            "ALTER TABLE jobs ADD COLUMN claimed_provider TEXT",
            "ALTER TABLE jobs ADD COLUMN claimed_cost_tier TEXT",
            "ALTER TABLE jobs ADD COLUMN retry_backoff_until REAL",
            "ALTER TABLE jobs ADD COLUMN routed_at REAL",
            "ALTER TABLE jobs ADD COLUMN routing_metadata TEXT",
            "ALTER TABLE jobs ADD COLUMN result_mnemos_id TEXT",
            "ALTER TABLE jobs ADD COLUMN tokens_in INTEGER",
            "ALTER TABLE jobs ADD COLUMN tokens_out INTEGER",
            "ALTER TABLE jobs ADD COLUMN estimated_cost_usd REAL",
            "ALTER TABLE jobs ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE jobs ADD COLUMN max_retries INTEGER NOT NULL DEFAULT 2",
            # F03 (2026-09-14): per-claim fencing token + attempt generation.
            "ALTER TABLE jobs ADD COLUMN claim_token TEXT",
            "ALTER TABLE jobs ADD COLUMN attempt_generation INTEGER NOT NULL DEFAULT 0",
        )
        for stmt in migrations:
            try:
                await db.execute(stmt)
            except Exception as e:
                if "duplicate column" not in str(e).lower():
                    print(f"migration warn ({stmt!r}): {e}", flush=True)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_jobs_project ON jobs(project)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_jobs_retry_backoff ON jobs(retry_backoff_until)")
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_costs ON jobs(ended_at, claimed_provider, claimed_model)"
        )
        await db.commit()
    app.state.reaper_last_success = time.time()
    app.state.reaper_last_error = None
    task = asyncio.create_task(reaper_task(app))
    app.state.reaper_task = task
    yield
    task.cancel()


app = FastAPI(title="GRAEAE Hive Mind", version="0.1.0", lifespan=lifespan)


@app.middleware("http")
async def require_hive_bus_bearer(request: Request, call_next):
    if request.url.path.rstrip("/") in _PUBLIC_PATHS:
        return await call_next(request)
    if not _hive_bus_auth_required():
        request.state.hive_auth_role = "disabled"
        return await call_next(request)

    expected_tokens = _configured_bus_tokens()
    if not expected_tokens:
        return JSONResponse(
            status_code=503,
            content={
                "detail": (
                    "Hive bus authentication is required, but no HIVE_BUS_TOKEN "
                    "or MNEMOS_HIVE_BUS_TOKEN is configured."
                )
            },
        )
    admin_tokens = _configured_admin_tokens()
    if any(admin in expected_tokens for admin in admin_tokens):
        return JSONResponse(
            status_code=503,
            content={"detail": "HIVE_ADMIN_TOKEN must be distinct from HIVE_BUS_TOKEN"},
        )
    supplied = _bearer_token(request)
    if supplied is None:
        return JSONResponse(status_code=401, content={"detail": "missing or invalid bearer token"})
    if any(secrets.compare_digest(supplied, token) for token in admin_tokens):
        request.state.hive_auth_role = "admin"
    elif any(secrets.compare_digest(supplied, token) for token in expected_tokens):
        request.state.hive_auth_role = "bootstrap"
    else:
        try:
            async with (
                aiosqlite.connect(DB_PATH) as db,
                db.execute(
                    "SELECT c.urn FROM agent_credentials c JOIN agents a ON a.urn=c.urn "
                    "WHERE c.token_hash=?",
                    (_token_hash(supplied),),
                ) as cur,
            ):
                row = await cur.fetchone()
        except aiosqlite.OperationalError:
            row = None
        if not row:
            return JSONResponse(status_code=401, content={"detail": "missing or invalid bearer token"})
        request.state.hive_auth_role = "agent"
        request.state.agent_urn = row[0]
    if request.url.path.rstrip("/") == "/v1/agents/register" and _request_role(request) not in {
        "bootstrap",
        "admin",
    }:
        return JSONResponse(status_code=403, content={"detail": "fleet bootstrap credential required"})
    if (
        _request_role(request) == "bootstrap"
        and request.url.path.rstrip("/") not in _BOOTSTRAP_ALLOWED_PATHS
    ):
        return JSONResponse(
            status_code=403,
            content={"detail": "bootstrap credential is registration-only"},
        )
    return await call_next(request)


# ---------- endpoints ----------


@app.get("/health")
async def health():
    now = time.time()
    problems: list[str] = []
    try:
        async with aiosqlite.connect(DB_PATH, timeout=1.0) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('agents','jobs','events')"
            ) as cur:
                row = await cur.fetchone()
        if not row or row[0] != 3:
            problems.append("storage schema is incomplete")
    except Exception as exc:
        problems.append(f"storage probe failed: {type(exc).__name__}: {exc}")

    task = getattr(app.state, "reaper_task", None)
    last_success = getattr(app.state, "reaper_last_success", None)
    last_error = getattr(app.state, "reaper_last_error", None)
    if task is None or task.done():
        problems.append("reaper task is not running")
    elif last_success is None or now - last_success > HEARTBEAT_REAP_INTERVAL * 2.5:
        problems.append(f"reaper is stale: {last_error or 'no successful cycle'}")

    payload = {
        "status": "unhealthy" if problems else "healthy",
        "ts": now,
        "service": "graeae-hive-mind",
        "version": "0.1.0",
    }
    if problems:
        payload["problems"] = problems
        return JSONResponse(status_code=503, content=payload)
    return payload


@app.get("/")
async def dashboard():
    """Minimal HTML+JS dashboard. Auto-refreshes /v1/agents + /v1/jobs + /v1/stats/* + SSE."""
    from fastapi.responses import HTMLResponse

    try:
        return HTMLResponse(
            files("mnemos.hive_mind").joinpath("dashboard.html").read_text(encoding="utf-8"),
            headers={
                "Content-Security-Policy": (
                    "default-src 'none'; script-src 'self'; style-src 'unsafe-inline'; "
                    "connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
                )
            },
        )
    except (FileNotFoundError, ModuleNotFoundError):
        return HTMLResponse("<h1>dashboard.html missing</h1>", status_code=404)


@app.get("/dashboard.js")
async def dashboard_script():
    """Serve the packaged dashboard program as a same-origin CSP-safe asset."""
    from fastapi.responses import Response
    from importlib.resources import files

    source = files("mnemos.hive_mind").joinpath("dashboard.html").read_text(encoding="utf-8")
    marker = '<script id="dashboard-code" type="text/plain">'
    script = source.split(marker, 1)[1].split("</script>", 1)[0]
    script = script.replace("__LIVE_REFRESH_MS__", str(DASHBOARD_LIVE_REFRESH_SECONDS * 1000))
    script = script.replace("__ANALYTICS_REFRESH_MS__", str(DASHBOARD_ANALYTICS_REFRESH_SECONDS * 1000))
    return Response(script, media_type="application/javascript", headers={"Cache-Control": "no-store"})


@app.post("/v1/agents/register")
async def register(req: AgentRegister):
    # #5 FIX (review 2026-05-23): reject minimally-incomplete registrations with 422 so
    # callers see the error instead of getting a null URN that bricks their session.
    if not req.host or not req.host.strip():
        raise HTTPException(422, "host is required (e.g., 'studio' or hostname -s)")
    runtime = (req.runtime or "unknown").lower()
    kind = (req.kind or runtime).lower()
    if runtime == "unknown" and not req.kind:
        raise HTTPException(
            422,
            "must provide runtime (claude-code/opencode/goose/codex/...) OR explicit kind. "
            "Defaulting to 'unknown' was masking session-bricking misregistrations.",
        )
    if runtime == "unknown" and kind in WORKER_ONLY_RUNTIMES:
        raise HTTPException(
            422,
            f"worker kind={kind!r} must declare its runtime; runtime='unknown' cannot register as a worker",
        )
    allowed = RUNTIME_KIND_MAP.get(runtime, {runtime, "unknown"})
    if runtime != "unknown" and kind not in allowed and kind != runtime:
        raise HTTPException(
            status_code=422,
            detail=(
                f"identity-mismatch: runtime={runtime!r} cannot register as kind={kind!r}. "
                f"Allowed kinds for this runtime: {sorted(allowed)}. "
                f"Set kind to one of those (or omit it) — fixes opencode-misregistering-as-claude per advisory."
            ),
        )
    autonomy = (req.autonomy_level or "unknown").lower()
    if autonomy not in AUTONOMY_LEVELS:
        raise HTTPException(422, f"autonomy_level must be one of {sorted(AUTONOMY_LEVELS)}, got {autonomy!r}")
    auth_method = (req.auth_method or "unknown").lower()
    if auth_method not in AUTH_METHODS:
        raise HTTPException(422, f"auth_method must be one of {sorted(AUTH_METHODS)}, got {auth_method!r}")
    plan_cap_usd = req.plan_cap_usd if req.plan_cap_usd is not None else DEFAULT_PLAN_CAPS.get(auth_method, 50.0)
    provider = (req.provider or "unknown").lower()
    model = (req.model or "unknown").lower()
    tier = cost_tier_for(provider)
    session_id = str(uuid.uuid4())
    urn = make_urn(kind, req.host, session_id)
    agent_token = secrets.token_urlsafe(32)
    now = time.time()
    # Phase 2 cut 1: INSERT delegated to SqliteHiveMindRepository.insert_agent.
    # emit_event stays inline (separate transaction is acceptable — event log
    # is observability, not the agent row's source of truth).
    await _REPO.insert_agent(
        urn=urn,
        kind=kind,
        runtime=runtime,
        model=model,
        provider=provider,
        cost_tier=tier,
        autonomy_level=autonomy,
        auth_method=auth_method,
        plan_cap_usd=plan_cap_usd,
        subscription_pools=req.subscription_pools,
        host=req.host,
        session_id=session_id,
        pid=req.pid,
        capabilities=req.capabilities,
        version=req.version,
        started_at=now,
        last_heartbeat=now,
        metadata=req.metadata,
    )
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO agent_credentials (urn, token_hash, issued_at) VALUES (?, ?, ?)",
            (urn, _token_hash(agent_token), now),
        )
        await db.commit()
        await emit_event(
            db,
            "agent.online",
            {
                "urn": urn,
                "kind": kind,
                "runtime": runtime,
                "model": model,
                "provider": provider,
                "cost_tier": tier,
                "host": req.host,
                "autonomy_level": autonomy,
                "subscription_pools": req.subscription_pools,
            },
        )
        await db.commit()
    return {
        "urn": urn,
        "agent_token": agent_token,
        "session_id": session_id,
        "registered_at": now,
        "kind": kind,
        "runtime": runtime,
        "model": model,
        "provider": provider,
        "cost_tier": tier,
        "autonomy_level": autonomy,
        "auth_method": auth_method,
        "plan_cap_usd": plan_cap_usd,
        "subscription_pools": req.subscription_pools,
    }


@app.post("/v1/agents/heartbeat")
async def heartbeat(req: AgentHeartbeat, request: Request):
    caller = _require_agent(request, req.urn)
    now = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "UPDATE agents SET last_heartbeat=?, status=? WHERE urn=?",
            (now, req.status, caller),
        )
        await db.commit()
        if cur.rowcount == 0:
            raise HTTPException(404, f"agent not found: {caller}")
    return {"ack": True, "ts": now}


@app.get("/v1/agents")
async def list_agents(
    status: Optional[str] = None,
    kind: Optional[str] = None,
    host: Optional[str] = None,
    runtime: Optional[str] = None,
    pid: Optional[int] = None,
    cost_tier: Optional[str] = None,
):
    sql = (
        "SELECT urn, kind, host, status, last_heartbeat, capabilities, version, metadata, "
        "pid, runtime, model, provider, cost_tier, autonomy_level, subscription_pools "
        "FROM agents WHERE 1=1"
    )
    args: list = []
    if status:
        sql += " AND status=?"
        args.append(status)
    if kind:
        sql += " AND kind=?"
        args.append(kind)
    if host:
        sql += " AND host=?"
        args.append(host)
    if runtime:
        sql += " AND runtime=?"
        args.append(runtime)
    if pid is not None:
        sql += " AND pid=?"
        args.append(pid)
    if cost_tier:
        sql += " AND cost_tier=?"
        args.append(cost_tier)
    sql += " ORDER BY last_heartbeat DESC"
    rows = []
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, args) as cur:
            async for r in cur:
                meta = json.loads(r[7]) if r[7] else {}
                # build display name: kind@host[pid] cwd:cwd_basename — distinguishes multiple sessions on same host
                cwd = (meta or {}).get("cwd") if isinstance(meta, dict) else None
                cwd_short = cwd.split("/")[-1] if cwd else None
                display = f"{r[1]}@{r[2]}"
                if r[8] is not None:
                    display += f"[pid={r[8]}]"
                if cwd_short:
                    display += f" cwd={cwd_short}"
                rows.append(
                    {
                        "urn": r[0],
                        "kind": r[1],
                        "host": r[2],
                        "status": r[3],
                        "last_heartbeat": r[4],
                        "capabilities": json.loads(r[5]) if r[5] else None,
                        "version": r[6],
                        "metadata": meta,
                        "pid": r[8],
                        "runtime": r[9],
                        "model": r[10],
                        "provider": r[11],
                        "cost_tier": r[12],
                        "autonomy_level": r[13],
                        "subscription_pools": json.loads(r[14]) if r[14] else None,
                        "display": display,
                    }
                )
    return {"count": len(rows), "agents": rows}


@app.get("/v1/agents/{urn_path:path}/throttle")
async def agent_throttle(urn_path: str):
    """Inspect an agent's plan-cap throttle state."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT urn, kind, runtime, auth_method, plan_cap_usd, plan_period_used_usd FROM agents WHERE urn=?",
            (urn_path,),
        ) as cur:
            row = await cur.fetchone()
    if not row:
        raise HTTPException(404, f"agent not found: {urn_path}")
    used = row[5] or 0
    cap = row[4] or 0
    pct = (100 * used / cap) if cap else 0
    return {
        "urn": row[0],
        "kind": row[1],
        "runtime": row[2],
        "auth_method": row[3],
        "plan_cap_usd": cap,
        "plan_period_used_usd": round(used, 4),
        "plan_period_pct": round(pct, 1),
        "throttled": row[3] == "subscription" and pct >= 85.0,
        "headroom_pct": THROTTLE_HEADROOM * 100,
    }


@app.post("/v1/agents/{urn_path:path}/plan-reset")
async def reset_plan_period(urn_path: str, request: Request):
    """Operator zeros the MTD usage — call monthly on billing rollover."""
    _require_admin(request)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("UPDATE agents SET plan_period_used_usd = 0 WHERE urn=?", (urn_path,))
        await db.commit()
    if cur.rowcount == 0:
        raise HTTPException(404, f"agent not found: {urn_path}")
    return {"ok": True, "urn": urn_path, "reset_at": time.time()}


@app.get("/v1/agents/whoami")
async def whoami(host: str, pid: int):
    """Help a session find ITS OWN urn by (host, pid) — most recent online registration wins.
    Useful when a session forgets its urn after restart and needs to re-discover its identity."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT urn, kind, runtime, model, provider, cost_tier, autonomy_level, started_at "
            "FROM agents WHERE host=? AND pid=? AND status='online' "
            "ORDER BY started_at DESC LIMIT 1",
            (host, pid),
        ) as cur:
            r = await cur.fetchone()
    if not r:
        raise HTTPException(404, f"no online agent matches host={host} pid={pid}")
    return {
        "urn": r[0],
        "kind": r[1],
        "runtime": r[2],
        "model": r[3],
        "provider": r[4],
        "cost_tier": r[5],
        "autonomy_level": r[6],
        "started_at": r[7],
    }


@app.post("/v1/jobs")
async def create_job(req: JobCreate, request: Request):
    """Submit work to the triage queue. No agent assignment — workers self-claim via /v1/jobs/next.

    ROLE ENFORCEMENT (user directive 2026-05-23):
    Worker runtimes (opencode/goose/codex/hermes/claw-family/ic-engine) are CLAIMERS, not submitters.
    Posting jobs requires submitter to be registered with an orchestrator runtime (claude-code/human/mnemos).
    Workers attempting to POST jobs get 403 — they should call /v1/jobs/next instead.
    """
    _require_agent(request, req.submitter_urn)
    job_id = uuidv7()
    now = time.time()
    repo_directive = re.match(r"^\s*\[repo:(?P<body>[^]]+)\]", req.description or "", re.IGNORECASE)
    if req.cacheable and not (req.pure and req.idempotent):
        raise HTTPException(422, "cacheable jobs must explicitly declare pure=true and idempotent=true")
    if req.cacheable and repo_directive:
        base_match = re.search(r"(?:^|\s)base:([0-9a-fA-F]{40}|[0-9a-fA-F]{64})(?:\s|$)", repo_directive.group("body"))
        if not base_match or not req.cache_context:
            raise HTTPException(
                422,
                "cacheable repository jobs require base:<full immutable commit hash> and cache_context dependency/toolchain identities",
            )
    if req.mnemos_refs:
        bad = [r for r in req.mnemos_refs if not (isinstance(r, str) and r.startswith("mem_"))]
        if bad:
            raise HTTPException(422, f"mnemos_refs must be mem_XXX ids — bad entries: {bad}")
    # DAG: validate depends_on targets exist + no self-cycle
    if req.depends_on:
        if job_id in req.depends_on:
            raise HTTPException(422, "depends_on cannot include self")
        async with aiosqlite.connect(DB_PATH) as _vd:
            placeholders = ",".join("?" * len(req.depends_on))
            async with _vd.execute(
                f"SELECT id FROM jobs WHERE id IN ({placeholders})",
                tuple(req.depends_on),
            ) as cur:
                found = {row[0] async for row in cur}
            missing = [d for d in req.depends_on if d not in found]
            if missing:
                raise HTTPException(422, f"depends_on references unknown job ids: {missing}")
    # Authorize before cache lookup: a cache hit must not turn a forbidden
    # worker submission into a successful completed job.
    async with aiosqlite.connect(DB_PATH) as auth_db:
        async with auth_db.execute("SELECT runtime FROM agents WHERE urn=?", (req.submitter_urn,)) as cur:
            submitter_row = await cur.fetchone()
    if submitter_row and (submitter_row[0] or "unknown").lower() in WORKER_ONLY_RUNTIMES:
        submitter_runtime = (submitter_row[0] or "unknown").lower()
        raise HTTPException(
            status_code=403,
            detail=(
                f"role-violation: runtime={submitter_runtime!r} is a WORKER, not an orchestrator. "
                f"Workers CLAIM jobs via POST /v1/jobs/next, they don't SUBMIT them. "
                f"If you need to delegate, message a human or claude-code orchestrator instead. "
                f"Orchestrators: {sorted(ORCHESTRATOR_RUNTIMES)}."
            ),
        )
    ck = cache_key_for(
        req.kind,
        req.description,
        (req.max_cost_tier or "A").upper(),
        req.required_capabilities,
        submitter_urn=req.submitter_urn,
        parent_job_id=req.parent_job_id,
        priority=req.priority,
        deadline=req.deadline,
        eligible_kinds=req.eligible_kinds,
        project=req.project,
        preferred_providers=req.preferred_providers,
        preferred_models=req.preferred_models,
        mnemos_refs=req.mnemos_refs,
        depends_on=req.depends_on,
        max_cost_tier_explicit="max_cost_tier" in getattr(req, "model_fields_set", set()),
        cache_context=req.cache_context,
    )
    async with aiosqlite.connect(DB_PATH) as db:
        cached = await cache_lookup(db, ck) if req.cacheable else None
        if cached:
            # Phase 2 cut 3: cache-hit insert delegated to repo. Cache-hit
            # accounting (cache_record_hit) stays inline because it needs
            # the existing db handle for emit_event.
            cached_result = {**(cached["result"] or {}), "cache_hit": True, "source_job_id": cached["source_job_id"]}
            await _REPO.insert_job_cache_hit(
                job_id=job_id,
                submitter_urn=req.submitter_urn,
                parent_job_id=req.parent_job_id,
                kind=req.kind,
                description=req.description,
                priority=req.priority,
                deadline=req.deadline,
                required_capabilities=req.required_capabilities,
                eligible_kinds=req.eligible_kinds,
                project=req.project,
                max_cost_tier=(req.max_cost_tier or "A").upper(),
                preferred_providers=req.preferred_providers,
                preferred_models=req.preferred_models,
                mnemos_refs=req.mnemos_refs,
                depends_on=req.depends_on,
                started_at=now,
                ended_at=now,
                result=cached_result,
                provider=cached["provider"],
                model=cached["model"],
                result_mnemos_id=cached.get("result_mnemos_id"),
            )
            # record cache hit with estimated cost-saving (use prior job's cost or fallback)
            saved = 0.01  # conservative fallback if no token data
            await cache_record_hit(db, ck, saved)
            await db.commit()
            await emit_event(
                db,
                "job.cached",
                {
                    "id": job_id,
                    "source_job_id": cached["source_job_id"],
                    "kind": req.kind,
                    "cost_saved_usd": saved,
                },
            )
            return {
                "id": job_id,
                "created_at": now,
                "status": "done",
                "cache_hit": True,
                "source_job_id": cached["source_job_id"],
                "result": cached["result"],
                "result_mnemos_id": cached.get("result_mnemos_id"),
            }
        # Unregistered submitters remain allowed for the documented external
        # curl/human path; registered worker-only runtimes were rejected above.
        # Phase 2 cut 2: INSERT delegated to repo.insert_job_queued.
        # The cache-hit short-circuit path above still runs inline (different
        # row shape — status='done' + claimed_provider — cut 3 target).
        await _REPO.insert_job_queued(
            job_id=job_id,
            submitter_urn=req.submitter_urn,
            parent_job_id=req.parent_job_id,
            kind=req.kind,
            description=req.description,
            priority=req.priority,
            deadline=req.deadline,
            required_capabilities=req.required_capabilities,
            eligible_kinds=req.eligible_kinds,
            project=req.project,
            max_cost_tier=(req.max_cost_tier or "A").upper(),
            preferred_providers=req.preferred_providers,
            preferred_models=req.preferred_models,
            mnemos_refs=req.mnemos_refs,
            depends_on=req.depends_on,
            max_retries=req.max_retries,
            started_at=now,
            routing_metadata={
                "submitter_max_cost_tier_explicit": "max_cost_tier" in getattr(req, "model_fields_set", set()),
                "cacheable": req.cacheable,
                "cache_context": req.cache_context,
            },
        )
        await emit_event(
            db,
            "job.queued",
            {
                "id": job_id,
                "submitter": req.submitter_urn,
                "kind": req.kind,
                "description": req.description,
                "priority": req.priority,
                "required_capabilities": req.required_capabilities,
                "eligible_kinds": req.eligible_kinds,
                "max_cost_tier": (req.max_cost_tier or "A").upper(),
                "mnemos_refs": req.mnemos_refs,
            },
        )
    return {"id": job_id, "created_at": now, "mnemos_refs": req.mnemos_refs}


@app.post("/v1/jobs/next")
async def dequeue_next_job(request: Request, agent_urn: str | None = None):
    """Atomic dequeue: highest-priority queued job that this agent is eligible for.

    Self-assignment for swarm: agent calls this in its main loop. Server:
      1. Looks up agent's kind + capabilities.
      2. Finds top-priority queued job where (eligible_kinds covers agent kind) AND (required_capabilities subset of agent capabilities).
      3. Atomically claims it (status='claimed', claimed_by=agent_urn, claimed_at=now).
      4. Returns job to caller; or 204 No Content if nothing matches.

    Race-safe via SQLite immediate-mode UPDATE...WHERE rowid=(SELECT...LIMIT 1) under a transaction.
    """
    agent_urn = _require_agent(request, agent_urn)
    now = time.time()
    agent = await _claim_agent_context(agent_urn)

    # Phase 2 cut 4: atomic claim delegated to repo (transaction +
    # filter chain are storage semantics; only event emission stays here).
    claimed = await _REPO.find_and_claim_job(
        agent_urn=agent_urn,
        agent_kind=agent["agent_kind"],
        agent_caps=agent["agent_caps"],
        agent_runtime=agent["agent_runtime"],
        agent_model=agent["agent_model"],
        agent_provider=agent["agent_provider"],
        agent_tier=agent["agent_tier"],
        cost_tier_order=list(COST_TIERS),
        sub_throttled=agent["sub_throttled"],
        now=now,
    )
    if claimed:
        async with aiosqlite.connect(DB_PATH) as db2:
            await emit_event(
                db2,
                "job.claimed",
                {
                    "id": claimed["id"],
                    "claimed_by": agent_urn,
                    "kind": claimed["kind"],
                    "runtime": agent["agent_runtime"],
                    "model": agent["agent_model"],
                    "provider": agent["agent_provider"],
                    "cost_tier": agent["agent_tier"],
                    "attempt_generation": claimed.get("attempt_generation"),
                },
            )
        # F03 (2026-09-14): claimed dict already carries the freshly minted
        # claim_token (set atomically inside _find_and_claim_job_sync).
        return claimed
    return JSONResponse(status_code=204, content=None)


@app.patch("/v1/jobs/{job_id}")
async def update_job(job_id: str, req: JobUpdate, request: Request):
    now = time.time()
    async with aiosqlite.connect(DB_PATH) as state_db:
        async with state_db.execute(
            # F03 (2026-09-14): pull the current claim_token + attempt_generation
            # so the same-worker-different-attempt stale-completion case can be
            # rejected at the CAS boundary. The existing different-worker guard
            # below (claimed_by != caller) is preserved on top of this.
            "SELECT status, claimed_by, max_cost_tier, routing_metadata, submitter_urn, "
            "claim_token, attempt_generation FROM jobs WHERE id=?",
            (job_id,),
        ) as state_cur:
            state_row = await state_cur.fetchone()
    if not state_row:
        raise HTTPException(404, f"job not found: {job_id}")
    (
        current_status,
        current_claimer,
        current_cap,
        routing_metadata_json,
        submitter_urn,
        current_claim_token,
        current_attempt_generation,
    ) = state_row
    caller = _require_agent(request, req.claimed_by)
    allowed_transitions = {
        # Claiming is exclusively handled by the eligibility-checked CAS routes.
        "queued": {"cancelled"},
        "offered": {"queued", "cancelled"},
        "claimed": {"running", "queued", "done", "failed", "cancelled"},
        "running": {"running", "queued", "done", "failed", "cancelled"},
        "done": set(),
        "failed": {"queued"},
        "cancelled": set(),
    }
    if req.status != current_status and req.status not in allowed_transitions.get(current_status, set()):
        raise HTTPException(409, f"invalid job transition: {current_status} -> {req.status}")
    if current_claimer:
        if caller != current_claimer:
            raise HTTPException(403, "job is owned by a different worker")
    elif caller != submitter_urn:
        raise HTTPException(403, "only the submitter may mutate an unclaimed job")
    # Validate terminal-result schema FIRST so a malformed request (missing
    # provider_used/model_used, or KNEMON-decision mismatch) is reported with
    # 422/409 before any F03 fencing-token check — a malformed request should
    # not look like a stale-claim_token attack.
    reported_provider = None
    reported_model = None
    reported_tier = None
    if req.status in ("done", "failed"):
        if not isinstance(req.result, dict) or not req.result.get("provider_used") or not req.result.get("model_used"):
            raise HTTPException(422, "terminal results require provider_used and model_used")
        reported_provider = str(req.result["provider_used"]).lower()
        reported_model = str(req.result["model_used"])
        reported_tier = cost_tier_for(reported_provider)
        routing_metadata = json.loads(routing_metadata_json) if routing_metadata_json else {}
        decision = routing_metadata.get("decision") if isinstance(routing_metadata, dict) else None
        if isinstance(decision, dict):
            authorized = {(str(decision.get("provider") or "").lower(), str(decision.get("model_id") or ""))}
            for alternate in decision.get("fallback_chain") or []:
                if isinstance(alternate, dict):
                    authorized.add(
                        (
                            str(alternate.get("provider") or "").lower(),
                            str(alternate.get("model_id") or alternate.get("model") or ""),
                        )
                    )
                elif isinstance(alternate, (list, tuple)) and len(alternate) >= 2:
                    authorized.add((str(alternate[0]).lower(), str(alternate[1])))
            if (reported_provider, reported_model) not in authorized:
                raise HTTPException(409, "reported provider/model is outside the authorized KNEMON decision")
        if reported_tier not in COST_TIERS[: COST_TIERS.index(current_cap) + 1]:
            patched = dict(req.result)
            patched["exit_code"] = 1
            patched["failure_reason"] = (
                f"cost_tier_violation: provider={reported_provider} tier={reported_tier} exceeds cap={current_cap}"
            )
            req = JobUpdate(
                status="failed",
                result=patched,
                claimed_by=req.claimed_by,
                tokens_in=req.tokens_in,
                tokens_out=req.tokens_out,
                result_mnemos_id=req.result_mnemos_id,
                claim_token=req.claim_token,
            )
    # #3 FIX: Hallucination guard now surfaces failure_reason + exit_code=-2 (per review 2026-05-23)
    halluc_reason = None
    if req.status == "done" and req.result:
        halluc_reason = hallucination_check(req.result)
        if halluc_reason:
            # patch result: exit_code=-2 + top-level failure_reason for easy filtering
            patched = dict(req.result or {})
            patched["exit_code"] = -2
            patched["failure_reason"] = f"hallucination_guard:{halluc_reason}"
            patched["_hallucination_guard"] = halluc_reason
            req = JobUpdate(
                status="failed",
                result=patched,
                claimed_by=req.claimed_by,
                tokens_in=req.tokens_in,
                tokens_out=req.tokens_out,
                result_mnemos_id=req.result_mnemos_id,
                claim_token=req.claim_token,
            )
    # F03 (2026-09-14): fencing-token enforcement. A state-changing call on
    # a currently-claimed job MUST echo the current claim_token — otherwise
    # it is a stale token from a prior (reaped/reclaimed) attempt. We
    # enforce this for any write that targets a claimed/running job,
    # including the running transition, done/failed/cancelled, etc. We do
    # NOT require the token for writes the SUBMITTER makes against an
    # unclaimed job (queued/offered -> cancelled) — those never carry a
    # claim_token because no claim ever happened.
    if current_claimer and current_claim_token is not None:
        if not req.claim_token or req.claim_token != current_claim_token:
            raise HTTPException(
                409,
                "stale claim_token: this job's current attempt has a different "
                "claim_token (reaped + re-claimed or requeued; you are holding "
                "a token from a prior attempt)",
            )
    if current_status in {"done", "failed", "cancelled"} and req.status == current_status:
        return {"ok": True, "duplicate": True, "ts": now}
    fields = ["status=?"]
    args: list = [req.status]
    if req.status == "queued":
        fields.extend(
            [
                "claimed_by=NULL", "claimed_at=NULL", "claimed_runtime=NULL", "claimed_model=NULL",
                "claimed_provider=NULL", "claimed_cost_tier=NULL", "ended_at=NULL", "result=NULL",
                "result_mnemos_id=NULL", "tokens_in=NULL", "tokens_out=NULL", "estimated_cost_usd=NULL",
            ]
        )
    if req.status in ("done", "failed", "cancelled"):
        fields.append("ended_at=?")
        args.append(now)
    if req.result is not None and req.status != "queued":
        fields.append("result=?")
        args.append(json.dumps(req.result))
    if req.claimed_by and req.status != "queued":
        fields.extend(["claimed_by=?", "claimed_at=?"])
        args.extend([req.claimed_by, now])
    if reported_provider:
        fields.extend(["claimed_provider=?", "claimed_model=?", "claimed_cost_tier=?"])
        args.extend([reported_provider, reported_model, reported_tier])
    # token usage + cost estimation
    cost_estimate = None
    claimed_by_urn = None
    if req.status != "queued" and (req.tokens_in is not None or req.tokens_out is not None):
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT claimed_provider, claimed_model, claimed_by FROM jobs WHERE id=?", (job_id,)
            ) as cur:
                row = await cur.fetchone()
        if row:
            prov, mod, claimed_by_urn = row
            if reported_provider:
                prov = reported_provider
                mod = reported_model
            t_in = int(req.tokens_in or 0)
            t_out = int(req.tokens_out or 0)
            cost_estimate = estimate_cost(prov or "unknown", mod or "unknown", t_in, t_out)
            fields.extend(["tokens_in=?", "tokens_out=?", "estimated_cost_usd=?"])
            args.extend([t_in, t_out, cost_estimate])
    args.extend([job_id, current_status])
    # Always constrain the UPDATE by owner when the job is claimed, so a
    # concurrent re-claim between the SELECT above and this UPDATE cannot let
    # the losing worker write the winner's row.
    ownership_guard = ""
    if current_claimer:
        ownership_guard = " AND claimed_by=?"
        args.append(caller)
        # F03 (2026-09-14): also constrain the UPDATE by the current claim_token
        # so a stale token from a prior attempt cannot satisfy the WHERE clause.
        # The early-return 409 above catches the obvious cases, but a concurrent
        # re-claim that lands between the SELECT and this UPDATE is the
        # remaining race — the WHERE constraint closes it.
        if current_claim_token is not None:
            ownership_guard += " AND claim_token=?"
            args.append(current_claim_token)
    sql = f"UPDATE jobs SET {', '.join(fields)} WHERE id=? AND status=?{ownership_guard}"
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(sql, args)
        if cur.rowcount == 0:
            async with db.execute("SELECT 1 FROM jobs WHERE id=?", (job_id,)) as exists_cur:
                exists = await exists_cur.fetchone()
            if exists:
                raise HTTPException(
                    409,
                    "job ownership, state, or claim_token changed before update "
                    "(stale token or concurrent re-claim)",
                )
            raise HTTPException(404, f"job not found: {job_id}")
        # F03 (2026-09-14): stamp the terminal outcome on the in_flight attempt
        # record so the audit log closes for this attempt. Inline in the same
        # transaction so we do not depend on a cross-connection sqlite3 lock
        # (which can deadlock under WAL when the reaper_task is concurrently
        # touching the same DB). We stamp BEFORE emitting job.{status} so the
        # audit row is consistent with the event.
        if req.status in ("done", "failed", "cancelled") and current_attempt_generation is not None:
            outcome_norm = (
                "completed"
                if req.status == "done"
                else "failed"
                if req.status == "failed"
                else "cancelled"
            )
            try:
                result_json = (
                    json.dumps(req.result, separators=(",", ":"), default=str)
                    if req.result is not None
                    else None
                )
                await db.execute(
                    "UPDATE job_attempts SET outcome=?, outcome_at=?, result=? "
                    "WHERE job_id=? AND attempt_generation=? AND outcome='in_flight'",
                    (
                        outcome_norm,
                        float(now),
                        result_json,
                        job_id,
                        int(current_attempt_generation),
                    ),
                )
            except Exception as audit_exc:
                # Audit failure must not block the terminal write — the
                # job-state update already committed and the worker needs its
                # ack. Surface the failure to logs but do not 500.
                print(f"F03 attempt-audit stamp failed for job={job_id}: {audit_exc}", flush=True)
        # Roll MTD spend onto claimer (subscription throttle requires this)
        if cost_estimate and cost_estimate > 0 and claimed_by_urn:
            await db.execute(
                "UPDATE agents SET plan_period_used_usd = COALESCE(plan_period_used_usd,0) + ? WHERE urn=?",
                (cost_estimate, claimed_by_urn),
            )
        attempt_jrow = None
        if req.status in ("done", "failed", "cancelled"):
            async with db.execute(
                "SELECT kind, description, max_cost_tier, required_capabilities, "
                "claimed_model, claimed_provider, claimed_by, result, started_at, "
                "submitter_urn, parent_job_id, priority, deadline, eligible_kinds, project, "
                "preferred_providers, preferred_models, mnemos_refs, depends_on, routing_metadata "
                "FROM jobs WHERE id=?",
                (job_id,),
            ) as attempt_cur:
                attempt_jrow = await attempt_cur.fetchone()

        # On failed: auto-retry if under max_retries. Exponential backoff: 30s × 2^retry_count
        if req.status == "failed":
            async with db.execute("SELECT retry_count, max_retries FROM jobs WHERE id=?", (job_id,)) as rcur:
                rrow = await rcur.fetchone()
            if rrow and rrow[0] < rrow[1]:
                backoff = 30.0 * (2 ** rrow[0])
                next_at = time.time() + backoff
                # #4 FIX: clear ended_at + result + tokens + cost when re-queueing — prevents
                # impossible state (status=queued AND ended_at IS NOT NULL) per review 2026-05-23.
                await db.execute(
                    "UPDATE jobs SET status='queued', retry_count=retry_count+1, "
                    "retry_backoff_until=?, claimed_by=NULL, claimed_at=NULL, "
                    "claimed_runtime=NULL, claimed_model=NULL, claimed_provider=NULL, claimed_cost_tier=NULL, "
                    "ended_at=NULL, result=NULL, tokens_in=NULL, tokens_out=NULL, estimated_cost_usd=NULL "
                    "WHERE id=?",
                    (next_at, job_id),
                )
                await emit_event(
                    db,
                    "job.retry",
                    {
                        "id": job_id,
                        "retry_count": rrow[0] + 1,
                        "max_retries": rrow[1],
                        "backoff_sec": backoff,
                    },
                )
                # fall through to stats roll-up below (count as fail for this attempt)
        # On done/failed/cancelled: roll per-worker per-kind stats (capability scoring)
        if req.status in ("done", "failed", "cancelled"):
            jrow = attempt_jrow
            if jrow:
                (kind_j, desc_j, mtier, reqcaps_json, mdl_j, prov_j, claimed_by_j, result_j, started_j,
                 submitter_j, parent_j, priority_j, deadline_j, eligible_json, project_j,
                 preferred_providers_json, preferred_models_json, refs_json, depends_json, routing_metadata_json) = jrow
                # capability scoring stats
                if claimed_by_j and kind_j:
                    duration = (time.time() - (started_j or time.time())) if started_j else 0
                    col = {"done": "success_count", "failed": "fail_count", "cancelled": "cancelled_count"}[req.status]
                    await db.execute(
                        f"INSERT INTO worker_kind_stats (urn, kind, {col}, total_tokens_in, total_tokens_out, "
                        f"total_cost_usd, total_duration_sec, last_run) "
                        f"VALUES (?, ?, 1, ?, ?, ?, ?, ?) "
                        f"ON CONFLICT(urn, kind) DO UPDATE SET "
                        f"{col} = {col} + 1, "
                        f"total_tokens_in = total_tokens_in + ?, "
                        f"total_tokens_out = total_tokens_out + ?, "
                        f"total_cost_usd = total_cost_usd + ?, "
                        f"total_duration_sec = total_duration_sec + ?, "
                        f"last_run = ?",
                        (
                            claimed_by_j,
                            kind_j,
                            int(req.tokens_in or 0),
                            int(req.tokens_out or 0),
                            cost_estimate or 0,
                            duration,
                            time.time(),
                            int(req.tokens_in or 0),
                            int(req.tokens_out or 0),
                            cost_estimate or 0,
                            duration,
                            time.time(),
                        ),
                    )
                # cache only successful results
                if req.status == "done":
                    ck = cache_key_for(
                        kind_j,
                        desc_j,
                        (mtier or "A").upper(),
                        json.loads(reqcaps_json) if reqcaps_json else None,
                        submitter_urn=submitter_j,
                        parent_job_id=parent_j,
                        priority=priority_j,
                        deadline=deadline_j,
                        eligible_kinds=json.loads(eligible_json) if eligible_json else None,
                        project=project_j,
                        preferred_providers=json.loads(preferred_providers_json) if preferred_providers_json else None,
                        preferred_models=json.loads(preferred_models_json) if preferred_models_json else None,
                        mnemos_refs=json.loads(refs_json) if refs_json else None,
                        depends_on=json.loads(depends_json) if depends_json else None,
                        max_cost_tier_explicit=bool(
                            (json.loads(routing_metadata_json) if routing_metadata_json else {}).get(
                                "submitter_max_cost_tier_explicit"
                            )
                        ),
                        cache_context=(
                            (json.loads(routing_metadata_json) if routing_metadata_json else {}).get("cache_context")
                        ),
                    )
                    rdict = json.loads(result_j) if result_j else (req.result or {})
                    ec = (rdict or {}).get("exit_code")
                    cache_metadata = json.loads(routing_metadata_json) if routing_metadata_json else {}
                    if cache_metadata.get("cacheable") is True and (ec == 0 or ec is None):
                        await cache_store(
                            db,
                            ck,
                            job_id,
                            rdict,
                            req.result_mnemos_id,
                            mdl_j or "unknown",
                            prov_j or "unknown",
                            cost_estimate or 0,
                        )
        await db.commit()
        await emit_event(
            db,
            f"job.{req.status}",
            {
                "id": job_id,
                "claimed_by": caller,
                "tokens_in": req.tokens_in,
                "tokens_out": req.tokens_out,
                "estimated_cost_usd": cost_estimate,
            },
        )
    return {"ok": True, "ts": now, "estimated_cost_usd": cost_estimate}


@app.patch("/v1/jobs/{job_id}/routing")
async def route_job(job_id: str, req: JobRoutingUpdate, request: Request):
    """Apply KNEMON/Hive triage metadata to a queued job before workers claim it."""
    _require_admin(request)
    now = time.time()
    updated = await _REPO.update_job_routing(
        job_id=job_id,
        routed_at=now,
        required_capabilities=req.required_capabilities,
        eligible_kinds=req.eligible_kinds,
        max_cost_tier=req.max_cost_tier,
        preferred_providers=req.preferred_providers,
        preferred_models=req.preferred_models,
        mnemos_refs=req.mnemos_refs,
        depends_on=req.depends_on,
        routing_metadata=req.routing_metadata,
    )
    if not updated:
        raise HTTPException(409, "job not queued, already claimed, or not found")
    async with aiosqlite.connect(DB_PATH) as db:
        await emit_event(
            db,
            "job.routed",
            {
                "id": job_id,
                "eligible_kinds": req.eligible_kinds,
                "preferred_providers": req.preferred_providers,
                "preferred_models": req.preferred_models,
                "required_capabilities": req.required_capabilities,
            },
        )
    return {"ok": True, "id": job_id, "routed_at": now}


@app.post("/v1/schedules")
async def create_schedule(req: ScheduleCreate, request: Request):
    """Create a recurring scheduled job. Re-fires every `interval_seconds`.
    job_template is the JobCreate body that will be submitted each tick.
    Submitter_urn auto-injected from caller — they take responsibility for the cron loop.
    """
    sid = uuidv7()
    now = time.time()
    tpl_copy = dict(req.job_template)
    asserted_submitter = tpl_copy.get("submitter_urn")
    if _request_role(request) == "disabled" and not asserted_submitter:
        asserted_submitter = "urn:agent:human:scheduler-placeholder"
    caller = _require_agent(request, asserted_submitter)
    tpl_copy["submitter_urn"] = caller
    # Validate the template parses as a JobCreate
    try:
        JobCreate(**tpl_copy)
    except Exception as e:
        raise HTTPException(422, f"job_template invalid: {e}")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO scheduled_jobs (id, name, created_by_urn, interval_seconds, "
            "job_template, enabled, last_fired_at, next_fire_at, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)",
            (
                sid,
                req.name,
                tpl_copy.get("submitter_urn"),
                int(req.interval_seconds),
                json.dumps(tpl_copy),
                1 if req.enabled else 0,
                now + req.interval_seconds,
                now,
            ),
        )
        await db.commit()
    return {"id": sid, "name": req.name, "next_fire_at": now + req.interval_seconds}


@app.get("/v1/schedules")
async def list_schedules():
    rows = []
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, name, created_by_urn, interval_seconds, enabled, "
            "last_fired_at, next_fire_at, fire_count, created_at FROM scheduled_jobs"
        ) as cur:
            async for r in cur:
                rows.append(
                    {
                        "id": r[0],
                        "name": r[1],
                        "created_by": r[2],
                        "interval_seconds": r[3],
                        "enabled": bool(r[4]),
                        "last_fired_at": r[5],
                        "next_fire_at": r[6],
                        "fire_count": r[7],
                        "created_at": r[8],
                    }
                )
    return {"count": len(rows), "schedules": rows}


@app.patch("/v1/schedules/{sid}")
async def patch_schedule(
    sid: str,
    request: Request,
    enabled: bool | None = None,
    interval_seconds: int | None = None,
):
    async with (
        aiosqlite.connect(DB_PATH) as auth_db,
        auth_db.execute("SELECT created_by_urn FROM scheduled_jobs WHERE id=?", (sid,)) as cur,
    ):
        owner = await cur.fetchone()
    if not owner:
        raise HTTPException(404, "schedule not found")
    _require_owner_or_admin(request, owner[0])
    sets, args = [], []
    if enabled is not None:
        sets.append("enabled=?")
        args.append(1 if enabled else 0)
    if interval_seconds is not None:
        if interval_seconds < 60 or interval_seconds > 86400 * 30:
            raise HTTPException(422, "interval_seconds must be 60..2592000")
        sets.append("interval_seconds=?")
        args.append(int(interval_seconds))
        sets.append("next_fire_at=?")
        args.append(time.time() + int(interval_seconds))
    if not sets:
        raise HTTPException(422, "no fields to update")
    args.append(sid)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(f"UPDATE scheduled_jobs SET {', '.join(sets)} WHERE id=?", args)
        await db.commit()
    if cur.rowcount == 0:
        raise HTTPException(404, "schedule not found")
    return {"ok": True}


@app.delete("/v1/schedules/{sid}")
async def delete_schedule(sid: str, request: Request):
    async with (
        aiosqlite.connect(DB_PATH) as auth_db,
        auth_db.execute("SELECT created_by_urn FROM scheduled_jobs WHERE id=?", (sid,)) as cur,
    ):
        owner = await cur.fetchone()
    if not owner:
        raise HTTPException(404, "schedule not found")
    _require_owner_or_admin(request, owner[0])
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("DELETE FROM scheduled_jobs WHERE id=?", (sid,))
        await db.commit()
    if cur.rowcount == 0:
        raise HTTPException(404, "schedule not found")
    return {"ok": True}


@app.get("/v1/stats/workers")
async def worker_stats(kind: Optional[str] = None, top_n: int = 30, include_system: bool = False):
    """Per-worker per-kind capability scores. Submitters use this to pick best worker for a kind.

    #8 FIX (review 2026-05-23): exclude `system` kind agents by default (they're host monitors,
    not compute workers — including them was misleading). Pass include_system=true to override.
    """
    sql = (
        "SELECT urn, kind, success_count, fail_count, cancelled_count, "
        "total_tokens_in, total_tokens_out, ROUND(total_cost_usd,4), "
        "ROUND(total_duration_sec/NULLIF(success_count+fail_count,0),1) AS avg_dur, "
        "datetime(last_run,'unixepoch') AS last_run, "
        "ROUND(100.0 * success_count / NULLIF(success_count+fail_count+cancelled_count,0), 1) AS success_pct "
        "FROM worker_kind_stats WHERE 1=1"
    )
    args: list = []
    if kind:
        sql += " AND kind=?"
        args.append(kind)
    if not include_system:
        sql += " AND urn NOT LIKE 'urn:agent:system:%'"
    sql += " ORDER BY success_count DESC, success_pct DESC LIMIT ?"
    args.append(_bounded_result_limit(top_n))
    rows = []
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, args) as cur:
            async for r in cur:
                rows.append(
                    {
                        "urn": r[0],
                        "kind": r[1],
                        "success_count": r[2],
                        "fail_count": r[3],
                        "cancelled_count": r[4],
                        "total_tokens_in": r[5],
                        "total_tokens_out": r[6],
                        "total_cost_usd": r[7],
                        "avg_duration_sec": r[8],
                        "last_run": r[9],
                        "success_pct": r[10],
                    }
                )
    return {"count": len(rows), "workers": rows}


@app.get("/v1/stats/cache")
async def cache_stats(top_n: int = 20):
    """Result-cache hit-rate + cost savings."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*), SUM(hit_count), ROUND(SUM(cost_saved_usd),4) FROM hive_cache") as cur:
            r = await cur.fetchone()
        totals = {"cached_jobs": r[0] or 0, "total_hits": r[1] or 0, "total_cost_saved_usd": r[2] or 0}
        # Every successful cache short circuit increments exactly one cache row.
        totals["jobs_short_circuited"] = totals["total_hits"]
        # top cached entries
        async with db.execute(
            "SELECT cache_key, hit_count, cost_saved_usd, model, provider, "
            "datetime(cached_at,'unixepoch') AS cached_at, source_job_id "
            "FROM hive_cache ORDER BY hit_count DESC, cost_saved_usd DESC LIMIT ?",
            (_bounded_result_limit(top_n),),
        ) as cur:
            entries = []
            async for row in cur:
                entries.append(
                    {
                        "cache_key": row[0][:16],
                        "hit_count": row[1],
                        "cost_saved_usd": row[2],
                        "model": row[3],
                        "provider": row[4],
                        "cached_at": row[5],
                        "source_job_id": row[6],
                    }
                )
    return {"totals": totals, "top_entries": entries}


@app.get("/v1/stats/costs")
async def cost_stats(since_hours: int = 168, group_by: str = "provider"):
    """Aggregated cost stats. group_by: provider | model | runtime | cost_tier | claimed_by | day.
    since_hours: window in hours (default 168 = 7 days).
    """
    if group_by not in {"provider", "model", "runtime", "cost_tier", "claimed_by", "day", "kind"}:
        raise HTTPException(422, "group_by must be one of: provider, model, runtime, cost_tier, claimed_by, day, kind")
    cutoff = time.time() - since_hours * 3600
    if group_by == "day":
        sel = "DATE(ended_at, 'unixepoch')"
    elif group_by == "provider":
        sel = "COALESCE(claimed_provider,'unknown')"
    elif group_by == "model":
        sel = "COALESCE(claimed_model,'unknown')"
    elif group_by == "runtime":
        sel = "COALESCE(claimed_runtime,'unknown')"
    elif group_by == "cost_tier":
        sel = "COALESCE(claimed_cost_tier,'unknown')"
    elif group_by == "claimed_by":
        sel = "COALESCE(claimed_by,'unknown')"
    else:  # kind
        sel = "kind"
    sql = (
        f"SELECT {sel} AS bucket, "
        "COUNT(*) AS job_count, "
        "SUM(COALESCE(tokens_in,0)) AS tot_in, "
        "SUM(COALESCE(tokens_out,0)) AS tot_out, "
        "ROUND(SUM(COALESCE(estimated_cost_usd,0)),4) AS tot_cost_usd, "
        "ROUND(AVG(CASE WHEN ended_at IS NOT NULL THEN ended_at-started_at END),2) AS avg_dur_s "
        "FROM jobs WHERE ended_at IS NOT NULL AND ended_at >= ? "
        f"GROUP BY bucket ORDER BY tot_cost_usd DESC, job_count DESC"
    )
    rows = []
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, (cutoff,)) as cur:
            async for r in cur:
                rows.append(
                    {
                        "bucket": r[0],
                        "job_count": r[1],
                        "tokens_in": r[2],
                        "tokens_out": r[3],
                        "estimated_cost_usd": r[4],
                        "avg_duration_sec": r[5],
                    }
                )
    # totals
    totals = {
        "job_count": sum(r["job_count"] for r in rows),
        "tokens_in": sum(r["tokens_in"] or 0 for r in rows),
        "tokens_out": sum(r["tokens_out"] or 0 for r in rows),
        "estimated_cost_usd": round(sum(r["estimated_cost_usd"] or 0 for r in rows), 4),
    }
    return {"since_hours": since_hours, "group_by": group_by, "totals": totals, "buckets": rows}


@app.post("/v1/jobs/{job_id}/claim")
async def claim_job(job_id: str, request: Request, by: str | None = None):
    """First-write-wins claim after the same eligibility gates as /v1/jobs/next."""
    by = _require_agent(request, by)
    now = time.time()
    agent = await _claim_agent_context(by)
    status, claimed = await _REPO.claim_job_by_id(
        job_id=job_id,
        agent_urn=by,
        agent_kind=agent["agent_kind"],
        agent_caps=agent["agent_caps"],
        agent_runtime=agent["agent_runtime"],
        agent_model=agent["agent_model"],
        agent_provider=agent["agent_provider"],
        agent_tier=agent["agent_tier"],
        cost_tier_order=list(COST_TIERS),
        sub_throttled=agent["sub_throttled"],
        now=now,
    )
    if status == "ineligible":
        raise HTTPException(403, "agent is not eligible to claim this job")
    if status != "claimed" or not claimed:
        raise HTTPException(409, "job already claimed or not available")
    async with aiosqlite.connect(DB_PATH) as db:
        await emit_event(
            db,
            "job.claimed",
            {
                "id": job_id,
                "claimed_by": by,
                "kind": claimed["kind"],
                "runtime": agent["agent_runtime"],
                "model": agent["agent_model"],
                "provider": agent["agent_provider"],
                "cost_tier": agent["agent_tier"],
                "attempt_generation": claimed.get("attempt_generation"),
            },
        )
    # F03 (2026-09-14): hand the freshly minted claim_token back to the
    # caller. Every subsequent state-changing call from this attempt MUST
    # echo this token; otherwise it is rejected with 409 stale-claim_token.
    return {
        "claimed": True,
        "ts": now,
        "claim_token": claimed.get("claim_token"),
        "attempt_generation": claimed.get("attempt_generation"),
    }


@app.get("/v1/jobs")
async def list_jobs(
    request: Request = None,
    status: Optional[str] = None,
    agent_urn: Optional[str] = None,
    since: Optional[float] = None,
    limit: int = 100,
):
    if request is None:
        request = Request({"type": "http", "method": "GET", "path": "/internal", "headers": []})
        request.state.hive_auth_role = "disabled"
    role = _request_role(request)
    caller: str | None = None
    agent: dict | None = None
    if role == "agent":
        caller = _require_agent(request, agent_urn)
        agent = await _claim_agent_context(caller)
    elif role == "admin":
        caller = None
    elif role == "disabled":
        caller = agent_urn
    else:
        raise HTTPException(403, "per-agent or admin credential required")
    sql = (
        "SELECT id, submitter_urn, parent_job_id, kind, description, priority, status, claimed_by, "
        "started_at, ended_at, result, required_capabilities, eligible_kinds, max_cost_tier, "
        "preferred_providers, preferred_models, mnemos_refs, depends_on, routed_at, routing_metadata, "
        "retry_backoff_until, claim_token, attempt_generation "
        "FROM jobs WHERE 1=1"
    )
    args: list = []
    if status:
        sql += " AND status=?"
        args.append(status)
    if role == "agent":
        sql += " AND (submitter_urn=? OR claimed_by=? OR (status IN ('queued','offered') AND claimed_by IS NULL))"
        args.extend([caller, caller])
    elif agent_urn:
        sql += " AND (submitter_urn=? OR claimed_by=?)"
        args.extend([agent_urn, agent_urn])
    if since:
        sql += " AND started_at >= ?"
        args.append(since)
    sql += " ORDER BY priority DESC, started_at DESC"
    if role != "agent":
        sql += " LIMIT ?"
        args.append(_bounded_result_limit(limit))
    rows = []
    now = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, args) as cur:
            async for r in cur:
                owned = role != "agent" or r[1] == caller or r[7] == caller
                if not owned and (not agent or not await _job_is_visible_claim_candidate(tuple(r), agent, now)):
                    continue
                rows.append(
                    {
                        "id": r[0],
                        "submitter_urn": r[1],
                        "parent_job_id": r[2],
                        "kind": r[3],
                        "description": r[4],
                        "priority": r[5],
                        "status": r[6],
                        "claimed_by": r[7],
                        "started_at": r[8],
                        "ended_at": r[9],
                        "result": json.loads(r[10]) if r[10] else None,
                        "required_capabilities": json.loads(r[11]) if r[11] else None,
                        "eligible_kinds": json.loads(r[12]) if r[12] else None,
                        "max_cost_tier": r[13],
                        "preferred_providers": json.loads(r[14]) if r[14] else None,
                        "preferred_models": json.loads(r[15]) if r[15] else None,
                        "mnemos_refs": json.loads(r[16]) if r[16] else None,
                        "depends_on": json.loads(r[17]) if r[17] else None,
                        "routed_at": r[18],
                        "routing_metadata": json.loads(r[19]) if r[19] else None,
                        # F03 (2026-09-14): expose claim_token + attempt_generation
                        # so workers can recover their current fencing token via
                        # GET /v1/jobs and admins can audit attempt history.
                        "claim_token": r[21],
                        "attempt_generation": r[22],
                    }
                )
                if len(rows) >= _bounded_result_limit(limit):
                    break
    return {"count": len(rows), "jobs": rows}


@app.post("/v1/messages")
async def publish_message(req: MessagePublish, request: Request):
    _require_agent(request, req.from_urn)
    msg_id = uuidv7()
    now = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO messages (id, from_urn, to_urn, in_reply_to, topic, payload, ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (msg_id, req.from_urn, req.to_urn, req.in_reply_to, req.topic, json.dumps(req.payload), now),
        )
        await emit_event(
            db,
            "message",
            {
                "id": msg_id,
                "from": req.from_urn,
                "to": req.to_urn,
                "topic": req.topic,
                "payload": req.payload,
            },
        )
    return {"id": msg_id, "ts": now}


@app.get("/v1/messages")
async def list_messages(
    request: Request = None,
    to_urn: Optional[str] = None,
    topic: Optional[str] = None,
    since: Optional[float] = None,
    limit: int = 100,
):
    """Read directed messages. Audience-scoped to the caller: an agent sees
    messages it sent, messages addressed to it, and broadcast messages
    (to_urn IS NULL). The to_urn query parameter, if supplied, must match the
    caller (admins may pass any to_urn). Bootstrap credentials cannot read
    (registration-only scope; F02 2026-09-14).

    `request` is optional so that internal callers (e.g. perf-clamp unit
    tests) can invoke this function directly without going through FastAPI's
    dependency injection. When None, the caller is treated as the
    admin-equivalent unfiltered path — same convention as `events_log`.
    """
    if request is None:
        request = Request({"type": "http", "method": "GET", "path": "/internal", "headers": []})
        request.state.hive_auth_role = "disabled"
    role = _request_role(request)
    caller_urn: str | None = None
    if role == "bootstrap":
        raise HTTPException(403, "bootstrap credential cannot read messages")
    if role == "agent":
        caller_urn = getattr(request.state, "agent_urn", None)
        if not caller_urn:
            raise HTTPException(403, "per-agent credential is not bound to an agent")
        if to_urn and to_urn != caller_urn:
            raise HTTPException(403, "agents may only read messages addressed to themselves")
    elif role == "disabled":
        if not to_urn:
            # Internal direct call (no FastAPI caller): preserve the
            # pre-fix behaviour for perf-clamp callers, who pass no to_urn
            # and expect a count of every message.
            pass
        else:
            caller_urn = to_urn
    elif role != "admin":
        raise HTTPException(403, "per-agent or admin credential required")

    sql = "SELECT id, from_urn, to_urn, in_reply_to, topic, payload, ts FROM messages WHERE 1=1"
    args: list = []
    if role == "agent":
        sql += " AND (to_urn=? OR to_urn IS NULL OR from_urn=?)"
        args.extend([caller_urn, caller_urn])
    elif role == "disabled" and caller_urn is not None:
        # HTTP-disabled caller supplied to_urn — scope to that agent.
        sql += " AND (to_urn=? OR to_urn IS NULL OR from_urn=?)"
        args.extend([caller_urn, caller_urn])
    elif to_urn:
        sql += " AND (to_urn=? OR to_urn IS NULL)"
        args.append(to_urn)
    if topic:
        sql += " AND topic LIKE ?"
        args.append(topic.replace("*", "%"))
    if since:
        sql += " AND ts >= ?"
        args.append(since)
    sql += " ORDER BY ts DESC LIMIT ?"
    args.append(_bounded_result_limit(limit))
    rows = []
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, args) as cur:
            async for r in cur:
                rows.append(
                    {
                        "id": r[0],
                        "from": r[1],
                        "to": r[2],
                        "in_reply_to": r[3],
                        "topic": r[4],
                        "payload": json.loads(r[5]),
                        "ts": r[6],
                    }
                )
    return {"count": len(rows), "messages": rows}


@app.get("/v1/events")
async def stream_events(request: Request, since_id: Optional[int] = None):
    """SSE stream of events. Optional since_id for catch-up. Audience-scoped
    to the calling agent using the same predicate /v1/jobs uses for job
    visibility (F02 2026-09-14). Bootstrap credentials cannot subscribe
    (registration-only scope).
    """
    role = _request_role(request)
    caller_urn: str | None = None
    caller_ctx: dict | None = None
    if role == "bootstrap":
        raise HTTPException(403, "bootstrap credential cannot subscribe to live events")
    if role == "agent":
        caller_urn = getattr(request.state, "agent_urn", None)
        if not caller_urn:
            raise HTTPException(403, "per-agent credential is not bound to an agent")
        caller_ctx = await _claim_agent_context(caller_urn)
    elif role not in {"admin", "disabled"}:
        raise HTTPException(403, "per-agent or admin credential required")

    sub_id = str(uuid.uuid4())
    queue: asyncio.Queue = asyncio.Queue(maxsize=SSE_EVENT_QUEUE_SIZE)
    EVENT_QUEUE[sub_id] = queue

    async def gen():
        try:
            # catch-up phase
            if since_id is not None:
                async with aiosqlite.connect(DB_PATH) as db:
                    async with db.execute("SELECT COALESCE(MAX(id), ?) FROM events", (since_id,)) as cur:
                        high_watermark = int((await cur.fetchone())[0])
                    catchup_cursor = since_id
                    while catchup_cursor < high_watermark:
                        batch = []
                        async with db.execute(
                            "SELECT id, ts, kind, payload FROM events "
                            "WHERE id > ? AND id <= ? ORDER BY id ASC LIMIT ?",
                            (catchup_cursor, high_watermark, SSE_CATCHUP_PAGE_SIZE),
                        ) as cur:
                            async for r in cur:
                                batch.append(r)
                        if not batch:
                            break
                        for r in batch:
                            evt = {
                                "id": r[0],
                                "ts": r[1],
                                "kind": r[2],
                                "payload": json.loads(r[3]),
                            }
                            if not await _event_visible_to_agent(evt, caller_urn, caller_ctx, time.time()):
                                continue
                            yield {
                                "id": str(evt["id"]),
                                "event": evt["kind"],
                                "data": json.dumps({"ts": evt["ts"], "kind": evt["kind"], "payload": evt["payload"]}),
                            }
                        catchup_cursor = int(batch[-1][0])
            # live stream
            last_ping = time.time()
            while True:
                if await request.is_disconnected():
                    break
                try:
                    evt = await asyncio.wait_for(queue.get(), timeout=SSE_PING_INTERVAL)
                    if not await _event_visible_to_agent(evt, caller_urn, caller_ctx, time.time()):
                        continue
                    yield {
                        "event": evt["kind"],
                        "data": json.dumps(evt),
                    }
                except asyncio.TimeoutError:
                    if time.time() - last_ping >= SSE_PING_INTERVAL:
                        yield {"event": "ping", "data": str(int(time.time()))}
                        last_ping = time.time()
        finally:
            EVENT_QUEUE.pop(sub_id, None)

    # Use the same ping interval for the sse_starlette keep-alive task as
    # the inline generator's queue.get() timeout — both honour the
    # SSE_PING_INTERVAL module constant so tests can shrink it without
    # waiting 15s for keep-alives.
    return EventSourceResponse(gen(), ping=SSE_PING_INTERVAL)


@app.get("/v1/events/log")
async def events_log(
    request: Request = None,
    since_id: Optional[int] = None,
    limit: int = 100,
):
    """JSON polling alternative to SSE. Audience-scoped: the same predicate
    /v1/jobs uses for job visibility also gates which events a caller can read
    (job lifecycle events ride on their referenced job; message events ride on
    the message's from/to; others ride on agent_urn). Bootstrap credentials
    cannot read this surface (registration-only scope; F02 2026-09-14).

    `request` is optional so that internal callers (e.g. perf-clamp unit
    tests) can invoke this function directly without going through FastAPI's
    dependency injection. When None, the caller is treated as the
    admin-equivalent unfiltered path (same as `_job_row_visible_to_agent`
    when both `agent_urn` and `agent_ctx` are None).
    """
    if request is None:
        request = Request({"type": "http", "method": "GET", "path": "/internal", "headers": []})
        request.state.hive_auth_role = "disabled"
    role = _request_role(request)
    caller_urn: str | None = None
    caller_ctx: dict | None = None
    if role == "bootstrap":
        raise HTTPException(403, "bootstrap credential cannot read event history")
    if role == "agent":
        caller_urn = getattr(request.state, "agent_urn", None)
        if not caller_urn:
            raise HTTPException(403, "per-agent credential is not bound to an agent")
        caller_ctx = await _claim_agent_context(caller_urn)
    elif role not in {"admin", "disabled"}:
        raise HTTPException(403, "per-agent or admin credential required")

    sql = "SELECT id, ts, kind, payload, agent_urn FROM events"
    args: list = []
    if since_id is not None:
        sql += " WHERE id > ?"
        args.append(since_id)
    sql += " ORDER BY id ASC LIMIT ?"
    args.append(_bounded_result_limit(limit))
    raw_rows = []
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, args) as cur:
            async for r in cur:
                raw_rows.append(
                    {
                        "id": r[0],
                        "ts": r[1],
                        "kind": r[2],
                        "payload": json.loads(r[3]),
                        "agent_urn": r[4],
                    }
                )
    now = time.time()
    rows = []
    # last_id is the highest event id the caller has CONSIDERED, not the
    # highest id they were allowed to see. When every event in the window is
    # hidden by the audience filter, last_id still advances past them so the
    # client's next poll (since_id=last_id) makes forward progress instead of
    # re-querying the same window forever. Match the original contract when
    # the DB has nothing past since_id: last_id stays at since_id (so the
    # client knows it is "caught up" — None when no cursor was given).
    last_id = since_id
    for ev in raw_rows:
        ev_id = int(ev["id"])
        if last_id is None or ev_id > last_id:
            last_id = ev_id
        if not await _event_visible_to_agent(ev, caller_urn, caller_ctx, now):
            continue
        rows.append(ev)
    return {"count": len(rows), "events": rows, "last_id": last_id}


# ---------- minimal MCP shim ----------
# Maps a few common MCP-style JSON-RPC calls to the REST endpoints.
# Phase 2: replace with full mcp-server-sdk.


@app.post("/mcp")
async def mcp_rpc(body: dict, request: Request):
    method = body.get("method", "")
    params = body.get("params", {})
    handlers = {
        "agent.register": register,
        "agent.heartbeat": heartbeat,
        "agent.list": list_agents,
        "job.create": create_job,
        "job.list": list_jobs,
        "message.publish": publish_message,
        "message.list": list_messages,
    }
    if method not in handlers:
        return JSONResponse({"error": f"unknown method: {method}"}, status_code=404)
    # crude param coercion - production MCP would use proper validation
    try:
        if method == "agent.register":
            _require_registration_authority(request)
            return await register(AgentRegister(**params))
        if method == "agent.heartbeat":
            return await heartbeat(AgentHeartbeat(**params), request)
        if method == "agent.list":
            return await list_agents(**params)
        if method == "job.create":
            return await create_job(JobCreate(**params), request)
        if method == "job.list":
            return await list_jobs(request=request, **params)
        if method == "message.publish":
            return await publish_message(MessagePublish(**params), request)
        if method == "message.list":
            return await list_messages(**params)
    except HTTPException as e:
        return JSONResponse({"error": e.detail}, status_code=e.status_code)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
