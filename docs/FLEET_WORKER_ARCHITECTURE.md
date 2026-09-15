# Fleet worker architecture: hive_worker.py

STIPHOS (this repo) is the bus only — `service.py`, the repository backends,
the claim/dequeue logic in `POST /v1/jobs/next`. It has no opinion about what
runs the actual coding work. That's `hive_worker.py`, a separate thin
claim/execute script living in `ncz-os/graeae-hive-mind`
(`https://gitlab.com/ncz-os/graeae-hive-mind.git`, canonical push target
`ssh://root@<ARGONAS>/mnt/datapool/git/graeae-hive.git`). This doc covers both
halves together because neither is comprehensible alone: the bus's kind-
matching semantics only make sense in light of what the worker actually does
with a claimed job, and the worker's env vars only make sense in light of what
the bus PATCHes back and forth.

Everything below reflects the state as of 2026-09-15, after a full-fleet
revival session (dead worker architecture replaced, every real host brought
to parity, several real bugs found and fixed live). Treat dates in this doc
as "true as of," not "true forever" — re-verify before trusting anything
here against a running system, the same discipline this doc's own findings
were earned by.

## 1. Why hive_worker.py exists

Before 2026-09-14 the fleet ran a heavier architecture per host: a
`zc-gateway` podman container plus one or more `zc-worker@N` containers
running `zeroclaw_wss_worker_v2.py`, which duplicated a full cost-tiered
provider-routing layer ("KNEMON") independently of the bus's own KNEMON copy
in `agent_bus.py`. Two independent routing implementations for the same
decision, one per host and one server-side, is exactly the kind of
redundancy a GRAEAE architecture consult (7-muse consensus, winner
MiniMax-M3, score 0.95) was asked to evaluate. The consult's conclusion,
which `hive_worker.py`'s own docstring states directly:

> the hive's job-claim path should be a THIN wrapper that shells out to
> `zoder`/`codex` and lets THEM own model selection via their own per-host
> config — not a second independent routing layer (KNEMON, or an in-process
> build harness).

So `hive_worker.py` does none of that. It claims a job, clones the job's
repo fresh, execs `zoder loop` or `codex exec` with no model opinion of its
own beyond a single fixed fallback rung, reports the result, and deletes the
workspace. Roughly 300 lines of stdlib Python. No podman, no quadlets, no
server-side model routing for these job kinds at all.

## 2. The worker loop

One `hive_worker.py` process claims **exactly one kind** — `zeroclaw` or
`codex` — set via `WORKER_KIND`. A host that runs both kinds runs two
separate processes (two systemd units), not one process claiming both.

```
register (POST /v1/agents/register, kind=WORKER_KIND)
  -> heartbeat loop (PATCH /v1/agents/<urn>, ~20s, background thread)
  -> poll loop (POST /v1/jobs/next?agent_urn=<urn>, ~5s):
       200  -> job claimed -> run_job()
       204  -> nothing eligible, sleep, poll again
  -> run_job():
       resolve_repo_url(job)          # REPO_MAP lookup, no arbitrary URLs
       fresh clone into WORKSPACE_ROOT/<job_id>
       exec zoder-loop or codex-exec inside the clone
       patch_job(status, result)      # MUST echo claimed_by or the PATCH 403s
       rm -rf the workspace (unless KEEP_WORKSPACE=1)
```

Every job gets a **fresh clone**, never a shared mutable workspace — that was
the old `zc_oneshot.py` architecture's bug (a dirty shared workspace could
leak state between unrelated jobs), fixed here the same way
`zc_native_build.py` fixed it earlier for direct-dispatch builds.

`run_job()` is wrapped so every code path — clone failure, driver-not-found,
subprocess timeout, exit code >0 — ends in a `patch_job("failed", ...)` call.
A job can never be silently abandoned mid-claim; every claim reaches a
terminal PATCH.

### Env vars (all optional except `WORKER_KIND`)

| Var | Default | Notes |
|---|---|---|
| `HIVE_URL` | `http://192.168.207.67:5005` | |
| `AGENT_HOST` | `hostname -s` | |
| `WORKER_KIND` | — (required) | `zeroclaw` or `codex` |
| `POLL_INTERVAL` | 5s | |
| `HEARTBEAT_INTERVAL` | 20s | |
| `JOB_TIMEOUT` | 7200s | hard kill of the driving subprocess |
| `WORKSPACE_ROOT` | `~/hive-workspaces` | |
| `KEEP_WORKSPACE` | unset | `1` disables cleanup, for debugging a failed job in place |
| `CODEX_MODEL` | `gpt-5.6-luna` | see §5 — **not** the escalation ladder's documented default, deliberately |
| `GIT_FLEET_SSH_PASSWORD` | unset | see §6 — enables the sshpass fallback for `root@ARGONAS` clones |

## 3. Kind matching — `eligible_kinds`, and the union that used to break it

A job carries `eligible_kinds: ["zeroclaw"]` or `["codex"]` (set at
submission). The bus's `/v1/jobs/next` claim query (`agent_bus.py`,
`agent_kind_aliases()`) used to union `claude`/`codex`/`zeroclaw`/`opencode`
into **mutual eligibility** — a policy dating to 2026-06-01, back when every
one of those kinds routed through the same server-side KNEMON layer and
"kind" was just a dispatch hint, not a statement about which tool would
actually run the job.

That premise died the day `hive_worker.py` replaced KNEMON-routed workers.
`WORKER_KIND` now directly picks the execution tool — there is no shared
routing layer downstream to paper over a kind mismatch. With the old union
still active, a `codex`-kind worker could (and did, reproduced live
2026-09-15) claim a job with `eligible_kinds=["zeroclaw"]` and run it via
`codex exec` instead of `zoder loop`. The union was removed
(`agent_bus.py` commit `10c3418`, 2026-09-15) — kind matching is strict
again. If you're reading this and a job is running through the "wrong" tool
for its kind, that regression came back; check `agent_kind_aliases()` first.

## 4. Repo resolution — `REPO_MAP`

`resolve_repo_url(job)` only resolves a job's `repo` field against a fixed
allowlist (`REPO_MAP` in `hive_worker.py`) — `riskyeats`, `zeroclaw`,
`mnemos`, `cix-installer`, `meta-cix`, `ic-engine`, `florida-licenses`,
`graeae-hive`, `zoder`, `artistpack` as of this writing. An unmapped `repo`
value causes the worker to decline the job (`release_job(..., "no_repo_mapping")`)
rather than clone an arbitrary URL. This is a deliberate constraint, not a
gap: a hive job cannot point a worker at attacker-controlled git infrastructure.

If you add a new repo to the fleet's active set, it needs an entry here or
every submission against it will sit `queued` forever with every worker
silently declining at poll time — that failure mode looks identical to "no
worker is up" from the submitter's side. Check `hive_worker.py`'s own
`REPO_MAP` dict before assuming a stuck job means a dead fleet.

## 5. Execution — `zoder loop` vs `codex exec`, and the CODEX_MODEL gotcha

```python
if WORKER_KIND == "zeroclaw":
    cmd = [_find_driver("zoder"), "loop", description, "--agent-timeout", str(JOB_TIMEOUT)]
else:
    cmd = [_find_driver("codex"), "exec", "--skip-git-repo-check", "-m", CODEX_MODEL, description]
```

**zeroclaw jobs get no `--agent`/`-m` at all, by design.** The intent is
that each host's own `~/.zoder/config.toml` escalation ladder governs model
selection — the worker has zero model opinion. In practice (§7) this
delegation only works cleanly when a host's zoder config is actually sane;
several hosts' weren't, and "bare `zoder loop` picks something sensible" is
not something to assume without testing on a given host.

**codex jobs get a single fixed model, `CODEX_MODEL`.** As of 2026-09-15
this is `gpt-5.6-luna`, **not** `gpt-5.3-codex-spark` (the escalation
ladder's documented default rung, see
`~/.claude/rules/00-AGENT-DIRECTIVES.md` on STUDIO). This is deliberate and
the reason is load-bearing enough to repeat here: **this account's live
codex model catalog does not serve a spark variant.** Queried directly
(`chatgpt-api.com/backend-api/codex/models`) and reproduced by direct
`codex exec` call on two independent hosts, two different codex-cli
versions (0.144.1 and 0.153.3) — same 400 both times:

```
{"type":"error","status":400,"error":{"type":"invalid_request_error",
 "message":"The 'gpt-5.3-codex-spark' model is not supported when using
 Codex with a ChatGPT account."}}
```

Every codex-kind hive job failed on this, fleet-wide, until the default was
changed. If the account's catalog ever adds a spark-class model back, this
default should move back up — don't just trust the escalation ladder doc
without re-querying the live catalog first (directive already exists
elsewhere: "probe, never cache," same principle that governs TYDEUS's
model-slot rotation).

Per-host override: set `CODEX_MODEL` in `/etc/hive-worker-env` (see §6).

`_find_driver(name)` resolves `zoder`/`codex` to an absolute path
(`shutil.which`, then `~/.local/bin/<name>`, `/usr/local/bin/<name>`,
`/opt/homebrew/bin/<name>`) because a bare PATH lookup silently
`FileNotFoundError`s under systemd/non-interactive SSH, which don't source
an interactive shell's PATH. **This fallback list is not exhaustive** — a
codex npm install lands at `~/.npm-global/bin/codex` on at least one host
(ULTRA), which isn't in the list. If a worker fails with
`driver_not_found`, check where the binary actually lives before assuming
it's missing.

## 6. Git auth — `GIT_FLEET_SSH_PASSWORD` and the host-key trap

Every `REPO_MAP` entry that isn't `zoder`/`artistpack` (GitLab HTTPS) is
`ssh://root@<ARGONAS>/mnt/datapool/git/<repo>.git`. Most fleet hosts do
**not** have their pubkey in ARGONAS root's `authorized_keys` — the fleet's
established convention (CLAUDE.md Gotchas) is password auth via `sshpass`
for this specific path, not pubkey provisioning per host. `hive_worker.py`
implements this as a fallback:

```python
clone_env = os.environ.copy()
fleet_pw = os.environ.get("GIT_FLEET_SSH_PASSWORD")
if fleet_pw and repo_url.startswith("ssh://root@"):
    clone_env["GIT_SSH_COMMAND"] = (
        f"sshpass -p {fleet_pw!r} ssh -o PubkeyAuthentication=no -o StrictHostKeyChecking=no"
    )
```

Two things have to independently be true for this to work, and a session
this large (2026-09-15) found both missing on a majority of hosts at some
point:

1. **`/etc/hive-worker-env` must actually declare `GIT_FLEET_SSH_PASSWORD`.**
   This is an `EnvironmentFile=-/etc/hive-worker-env` in the systemd unit
   (leading `-` = optional, missing file is not a startup error — which
   means a missing var fails *silently*, as a plain clone error, not as a
   config error). As of 2026-09-15, 4 of 8 real hosts (ACHILLES, HYDRA,
   MINOS, CERBERUS) had no such file at all; every `codex`/`zeroclaw` job
   on those hosts failed `clone_failed` with a generic "exit status 128"
   that gave no hint why.
2. **`sshpass` must be installed.** MINOS didn't have it (`apt-get install
   sshpass` fixed it) even after the env file was created — the failure
   mode there is `sshpass: not found` surfacing *through* `git`'s own error
   text (`GIT_SSH_COMMAND` failing to exec), easy to misread as a network
   problem.

**The host-key trap**, hit independently on MINOS, ACHILLES, and both
TYPHON LXC CTs the same session: `-o StrictHostKeyChecking=no` alone is
**not sufficient** when a host's `known_hosts` has a *stale* entry for
ARGONAS (i.e. the key present doesn't match what ARGONAS presents now).
OpenSSH disables password/keyboard-interactive auth entirely — not just the
interactive host-key prompt — when it detects a key mismatch, as an
additional MITM guard. `StrictHostKeyChecking=no` suppresses the *prompt*;
it does not suppress this auth-method-disable behavior. The fix needs
**both** flags:

```
-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
```

`hive_worker.py`'s own clone command already carries both (confirmed in the
current source). If you're testing the clone path manually and only pass
`StrictHostKeyChecking=no`, you will get a confusing "Permission denied"
that looks like a credentials problem and isn't — verify the real ARGONAS
host key independently first (`ssh-keyscan` from a host you know is
healthy) before spending time on the credential itself.

## 7. GitOps self-deployment

`hive_worker.py` and its systemd units are supposed to be reconciled
automatically, not hand-pushed per host. The mechanism:

- **`deploy/hive-worker-deploy.sh`** (POSIX sh, runs as root, in
  `graeae-hive`): clones/fetches the pinned `graeae-hive` ref into
  `/var/lib/graeae-hive`, and for a host with a
  `hosts/<hostname>/hive-worker.conf` (`KINDS="zeroclaw codex"`, optionally
  `RUN_AS="root"` for the TYPHON LXC CTs which have no `jasonperlow` user)
  installs `hive_worker.py` to `/opt/graeae/hive_worker.py` and the matching
  systemd unit(s), restarting only the units that actually changed. A host
  with no `hosts/<name>/hive-worker.conf` is treated as unmanaged and the
  script no-ops — **silently**, with only a log line (`no $CONF; not a
  hive-worker-managed host`), which looks identical to "already up to date"
  unless you're reading stdout carefully.
- **Gate**: `/etc/hive-worker-deploy.conf` must set `ENABLED=1`, or the
  whole script no-ops regardless of the conf file's presence.
- **Self-update**: the script diffs `graeae-hive/deploy/hive-worker-deploy.sh`
  against its own *installed* copy (`/usr/local/bin/hive-worker-deploy.sh`)
  and re-execs itself if different. Without this, a fix to the deploy
  script itself needs a manual re-push to every host forever — found the
  hard way (2026-09-15) when a real bugfix sat in the repo doing nothing on
  two hosts because their installed copy was still pre-fix, and the "no
  changes" log output gave zero indication why.
- **Timer**: `hive-worker-deploy.timer` (`OnBootSec=2min`,
  `OnUnitActiveSec=5min`, `RandomizedDelaySec=30s`) fires the oneshot
  service on that cadence.

**As of 2026-09-15, this mechanism itself needs verifying per host before
trusting it** — 4 of 8 real hosts had the worker and its systemd units
installed manually (during the initial fleet-uniformity rollout) but were
never given a `hosts/<name>/hive-worker.conf` entry, meaning the timer had
been running on those hosts the whole time and silently doing nothing. A
fix landing in the repo does not reach a host unless BOTH (a) that host's
`hosts/<name>/hive-worker.conf` exists and (b) `/etc/hive-worker-deploy.conf`
has `ENABLED=1` there. Check both before assuming "it's gitops-managed" is
actually true for a given host — `systemctl status hive-worker-deploy.timer`
being active proves the timer fires, not that it does anything.

## 8. Fleet topology (verified live, 2026-09-15)

| Host | Kinds | Notes |
|---|---|---|
| ACHILLES | zeroclaw, codex | fully migrated off the legacy zc-gateway/zc-worker@ stack this session |
| HYDRA | zeroclaw, codex | |
| TYDEUS | zeroclaw, codex | see §9 — zoder config had unrelated pre-existing bugs |
| MINOS | zeroclaw, codex | fleet-ops host (see MEMORY.md); also runs `zeroclaw-fleet-validate` for the legacy-architecture hosts, if any remain |
| PROTEUS | zeroclaw, codex | |
| ULTRA (`fedora` in the bus) | zeroclaw, codex | Fedora Asahi aarch64; codex installed via npm to `~/.npm-global/bin` — see §5's `_find_driver` gap |
| ARGOS | zeroclaw, codex | production; do not oversubscribe (CLAUDE.md gotcha) |
| CERBERUS | zeroclaw, codex | see §9 |
| forge-worker-01 (TYPHON CT110) | zeroclaw only | `RUN_AS=root` unit variant, no `jasonperlow` user in the CT |
| typhon-worker (TYPHON CT130) | zeroclaw only | renamed from `proteus-worker` 2026-09-15 (was confusingly named after an unrelated physical host at a different IP) |
| PYTHIA | — (none) | **deliberately excluded.** Hosts the bus itself (`agent_bus.py`) plus MNEMOS — explicit operator directive: "pythia should not be job eligible... it is strictly the doctor." |

## 9. Known live bugs in zoder itself (not this repo, not hive_worker.py)

Two independent hosts (TYDEUS, CERBERUS) hit the **same underlying zoder
bug** via different symptoms during this session's fleet-wide smoke test,
both traced to actual zoder source
(`crates/zoder-cli/src/main.rs`, `ncz-os/zoder`):

An agent-pinned model (`--agent X`, which `hive_worker.py`'s bare `zoder
loop` invocation relies on resolving via each host's own config) correctly
resolves to a MODEL STRING via `configured_model_for_agent()` ->
`model_provider` -> `[providers.models.custom.X].model`. But the PROVIDER
used for billing-identity verification is then resolved **separately and
independently** via `real_provider_for_model()`, which does a global
`serves`-prefix scan across every declared provider with no knowledge of
which agent was actually selected. A standalone `[providers.models.custom.X]`
block (referenced directly via `model_provider`, the normal pattern for a
host's own local model) typically has **no `serves` field at all** — so it
can never win that scan, and whatever *other*, unrelated provider's
`serves` list happens to prefix-match the same model string wins instead,
often landing on a broken or router-only provider with no engine profile.
Surfaces as:

```
zoder: error: zoder provider "<unrelated-provider-id>" has no resolved
engine_provider_ref; set it explicitly when its Zeroclaw profile cannot be
derived unambiguously at config-load time
```

This is a genuine zoder bug, not fleet config drift — reproduced from a
minimal repro independent of any one host's config. A fix was dispatched to
`ncz-os/zoder` the same session (see that repo's git log around
2026-09-15 for the actual commit — this doc intentionally doesn't hardcode
a SHA that may have been superseded by the time you're reading it). If a
`zeroclaw`-kind hive job fails with `engine_provider_ref` or `"agent ...
has no authoritative model route in the running daemon"`, check whether the
worker's `zoder --version` predates that fix before re-diagnosing from
scratch.

Separately, and specific to each host: TYDEUS's `~/.zoder/config.toml` had
two functionally-identical duplicate `[agents.*]` blocks both serving
`MiniMax-M3` (no `[acp].default_agent` set to disambiguate), and a stale
`[profile].primary_model = "coder"` referencing an abstract slot alias with
no literal matching `[agents.coder]` block. CERBERUS's config pointed at a
coder model service (`:8083`, Qwen3-14B-FP8) that had been stopped/retired
at the infrastructure level — the only genuinely-live local model there is
a Gemma-4-12B RiskyEats narrator on `:8080`, and per operator decision this
was left as a known gap (CERBERUS treated identically to every other
zeroclaw host, no special local-model pin) rather than repurposing a
narration-dedicated service for hive coding load. Neither of these is a
zoder bug — they're config drift, worth re-checking if either host acts up
again.

## 10. Operational checklist — diagnosing a stuck job

In rough order of likelihood, cheapest check first:

1. Is `eligible_kinds` on the job a real kind (`zeroclaw`/`codex`), and does
   at least one WORKER of that kind show `status: online` in
   `GET /v1/agents`?
2. Is the job's `repo` field a real `REPO_MAP` key (§4)? An unmapped repo
   causes silent, permanent decline at every worker's poll — the job just
   sits `queued` forever with no error surfaced anywhere.
3. Pull the job's `result` field after it transitions to `failed` — the
   worker always populates `{"via": "hive_worker", "error": ...}` (or
   `{"driver": ..., "exit_code": ..., "tail": ...}` for a driver failure)
   before returning it to `queued` for retry. Don't assume a
   `queued`->`claimed`->`queued` bounce means nothing happened; the
   evidence is in the last `result` before it cycled back.
4. If the failure is `clone_failed` with exit status 128: check
   `/etc/hive-worker-env` on that host for `GIT_FLEET_SSH_PASSWORD` (§6)
   before assuming a network problem.
5. If the failure is `driver_not_found`: check where the binary actually
   lives on that host (§5) — `_find_driver`'s fallback list is not
   exhaustive.
6. If a `codex`-kind job fails with an OpenAI 400 naming an unsupported
   model: re-query the live catalog before assuming `CODEX_MODEL` (§5) is
   still correct — it drifts as the account's served models change.
7. If a `zeroclaw`-kind job fails with `engine_provider_ref` or "no
   authoritative model route": that's the zoder bug in §9, not this repo.
