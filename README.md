# mnemos-stiphos

**STIPHOS** (Στίφος — "a marshaled host") — the GRAEAE Hive Mind: fleet-wide agent coordination, a claim/triage job queue, and cost-tier dispatch for the MNEMOS family. A disciplined swarm of worker agents, marshaled and dispatched.

> **Status: beta (v0.2.0 / Phase 1).** This subsystem is carved out of `mnemos-core` precisely so its faster, less-settled release cadence does not destabilize the stable core. It is **not** part of the core umbrella image; deploy it as its own service.

## What's here

| Concern | Module |
|---|---|
| REST service | `mnemos/hive_mind/service.py` (FastAPI: `/v1/agents/*`, `/v1/jobs/*`, `/v1/messages/*`, `/v1/events`, `/v1/stats/costs`) |
| Sqlite repository (default) | `mnemos/hive_mind/repository.py` |
| Oracle 23ai repository | `mnemos/hive_mind/oracle_repository.py` (extra: `oracle`) |
| MCP-SSE bridge | `mnemos/hive_mind/mcp_bridge.py` (extra: `mcp`) |
| System watcher / triage | `mnemos/hive_mind/system_watcher.py`, `mnemos/hive_mind/zeroclaw_triage.py` |
| Self-contained Oracle DDL | `mnemos/hive_mind/oracle_ddl.sql` |

## Roles & cost tiers

- **Orchestrators** (submit + claim): `claude-code`, `claude-cli`, `human`, `mnemos`
- **Workers** (claim-only): `opencode`, `goose`, `codex`, `hermes`, `zeroclaw`, `openclaw`, `ic-engine`
- **Cost tiers**: A = free (NGC/local), B = cheap paid, C = reserve. Jobs default `max_cost_tier="A"` — token-miser by design.

## Install

```bash
pip install mnemos-stiphos            # sqlite repository, REST service
pip install "mnemos-stiphos[oracle]"  # + experimental Oracle repository API
pip install "mnemos-stiphos[mcp]"     # + MCP-SSE bridge
```

`mnemos-stiphos` contributes the `mnemos.hive_mind` subpackage to the shared PEP 420 `mnemos.*` namespace. It is self-contained and can coexist with other `mnemos.*` namespace packages. (The distribution is named STIPHOS; the module path stays `mnemos.hive_mind`.)

## Run

```bash
export HIVE_BUS_TOKEN="$(openssl rand -hex 32)"
export HIVE_ADMIN_TOKEN="$(openssl rand -hex 32)"

# From source / pip install
HIVE_BUS_TOKEN="$HIVE_BUS_TOKEN" HIVE_ADMIN_TOKEN="$HIVE_ADMIN_TOKEN" \
  uvicorn mnemos.hive_mind.service:app --host 0.0.0.0 --port 8080

# Or the published image (multi-arch amd64 + arm64)
docker run -e HIVE_BUS_TOKEN="$HIVE_BUS_TOKEN" -e HIVE_ADMIN_TOKEN="$HIVE_ADMIN_TOKEN" \
  -v stiphos-data:/data \
  -p 127.0.0.1:8080:8080 ghcr.io/ncz-os/mnemos-stiphos:latest
```

The fleet bearer token is a registration bootstrap credential. Registration
returns a distinct `agent_token` once; the server stores only its SHA-256 digest,
and workers use that token for heartbeat, claim, update, submission, and message
requests. Fleet-global job reads and routing changes require the separate
`HIVE_ADMIN_TOKEN` (or `MNEMOS_HIVE_ADMIN_TOKEN`), which must not equal the
bootstrap token. `/health` and the dashboard shell at `/` are public. Set
`HIVE_BUS_AUTH_REQUIRED=0` only for isolated local development or tests.

The dashboard shell at `/` is public so a browser can load it. It prompts for
the admin token, keeps it only in tab-scoped `sessionStorage`, and sends it as a
bearer header on API and SSE requests. The protected data routes remain closed.

Performance bounds are configurable. Their defaults preserve prior runtime
behavior except for newly bounded result and claim pages:

| Variable | Default | Purpose |
|---|---:|---|
| `HIVE_CLAIM_CANDIDATE_PAGE_SIZE` | `256` | Lightweight rows inspected per SQLite claim page |
| `HIVE_MAX_RESULT_PAGE_SIZE` | `1000` | Maximum jobs, messages, or event-log rows per JSON response |
| `HIVE_SSE_CATCHUP_PAGE_SIZE` | `500` | Persisted events read per SSE catch-up batch |
| `HIVE_SSE_EVENT_QUEUE_SIZE` | `1000` | Buffered live events per subscriber |
| `HIVE_DASHBOARD_ANALYTICS_REFRESH_SECONDS` | `5` | Cost/cache refresh interval; set `30`–`60` to reduce scans |
| `HIVE_SYSTEM_LOAD_RETAIN_HOURS` | `0` | Optional `system.load` retention; `0` keeps messages indefinitely |
| `SYSTEM_LOAD_INTERVAL` | heartbeat interval | Watcher load-message interval; heartbeat cadence is unchanged |
| `ZEROCLAW_TRIAGE_IDLE_MAX_POLL_INTERVAL` | poll interval | Optional maximum idle polling backoff |

The REST service currently supports SQLite only and stores its database at
`/data/agents.db` in the published image. The Oracle repository is an
experimental library API; the REST service does not silently select it from an
Oracle DSN. For a fresh schema used directly with that repository, apply
`mnemos/hive_mind/oracle_ddl.sql`. The canonical
convention is the shared `agents`/`jobs` ledger plus STIPHOS-owned `hive_*`
tables, recorded as `stiphos-oracle` schema version 2 in
`hive_schema_metadata`. The Oracle repository validates that marker and the
required tables during `init()` and refuses to write to an unversioned,
incomplete, or deprecated schema.

If `HIVE_REPOSITORY_BACKEND` or `MNEMOS_PERSISTENCE_BACKEND` requests a
non-SQLite service backend, startup fails explicitly instead of falling back to
SQLite.

Before replacing an older container that used the previous default path, stop
the old container and copy the database into the mounted volume:

```bash
docker cp OLD_CONTAINER:/srv/agent-bus/agents.db ./agents.db
docker run --rm -v stiphos-data:/data -v "$PWD:/backup:ro" busybox \
  cp /backup/agents.db /data/agents.db
```

Keep the old container and backup until the new service reports healthy and
the expected agents and jobs are visible.

Older installations that have `memory_agents`, `memory_jobs`,
`memory_messages`, or `memory_events` use the deprecated convention. Back up
and test the database, then run
`migrations/oracle/002_memory_to_canonical.sql` explicitly as the schema owner.
The migration is idempotent, creates the canonical structures, copies legacy
rows through explicit column mappings, validates the resulting schema, and
records version 2 only after validation. It is never run automatically. The
wheel installs it under `share/mnemos-stiphos/migrations/oracle`.

## Relationship to the rest of MNEMOS

- **mnemos-core** — memory substrate used by the wider MNEMOS deployment; not required by STIPHOS.
- **mnemos-knemon** — the hybrid model router / hive *ledger* views. Reads the shared `agents`/`subscription_pools` schema (with graceful fallback when absent).
- **graeae-hive / graeae-hive-mind** — the separate build-fabric / worker-daemon track.


## Build infrastructure & partners

Continuous integration and package distribution for this project are generously
supported by our open-source infrastructure partners:

- **[GitLab](https://gitlab.com/)** — canonical source hosting and CI pipelines
  (format / lint / test gates), via the
  [GitLab for Open Source](https://about.gitlab.com/solutions/open-source/) program.
- **[Buildkite](https://buildkite.com/)** — CI/CD orchestration with hosted macOS
  and Linux agents, and our APT package registry host
  (`packages.buildkite.com/ncz-os/ncz`), via the
  [Buildkite Open Source](https://buildkite.com/pricing) program.

Thank you to both for backing open-source software.
