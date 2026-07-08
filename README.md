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
pip install "mnemos-stiphos[oracle]"  # + Oracle 23ai backend
pip install "mnemos-stiphos[mcp]"     # + MCP-SSE bridge
```

`mnemos-stiphos` contributes the `mnemos.hive_mind` subpackage to the shared PEP 420 `mnemos.*` namespace and depends on `mnemos-core` for config and persistence helpers. (The distribution is named STIPHOS; the module path stays `mnemos.hive_mind`.)

## Run

```bash
export HIVE_BUS_TOKEN="$(openssl rand -hex 32)"

# From source / pip install
HIVE_BUS_TOKEN="$HIVE_BUS_TOKEN" uvicorn mnemos.hive_mind.service:app --host 0.0.0.0 --port 8080

# Or the published image (multi-arch amd64 + arm64)
docker run -e HIVE_BUS_TOKEN="$HIVE_BUS_TOKEN" -p 127.0.0.1:8080:8080 ghcr.io/ncz-os/mnemos-stiphos:latest
```

All routes except `/health` require `Authorization: Bearer $HIVE_BUS_TOKEN` by default. Set `HIVE_BUS_AUTH_REQUIRED=0` only for isolated local development or tests. Workers and bridges read `HIVE_BUS_TOKEN` or `MNEMOS_HIVE_BUS_TOKEN` and attach the bearer token automatically.

The sqlite repository self-creates its schema at runtime; for Oracle, apply `mnemos/hive_mind/oracle_ddl.sql`. The shared hive *ledger* tables (`agents`, `jobs`, `subscription_pools`, …) are owned by `mnemos-core`'s migration runner and shared with KNEMON.

## Relationship to the rest of MNEMOS

- **mnemos-core** — memory substrate, config, persistence. Hard dependency.
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
