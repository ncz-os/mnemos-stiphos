# MNEMOS Stiphos Audit Findings V2 Fix Report

Date: 2026-09-09

## Outcome

All four requested security/correctness findings are fixed and covered by passing regression tests. The complete repository test suite passes with 94 passed, 4 skipped, and 0 failed. No remote was pushed.

## Repository and branch state

The required initial inspection showed:

```text
## fix/audit-findings...origin/fix/audit-findings
?? SOL_REVIEW.md
fix/audit-findings
* fix/audit-findings
  main
```

`origin/HEAD` resolved to `origin/main`. Local `main` and `origin/main` both resolved to `494f2afbd39f68965517355dc29ddfe58fb972d2` (`security(deploy): stop quoting the leaked credential in the rotation notice`), confirming that `main` was the current default branch. The old branch had no modified tracked files; the pre-existing untracked `SOL_REVIEW.md` was preserved untouched.

Work was performed on a new branch created from `main`:

```text
fix/audit-findings-v2
```

The repository was not re-cloned, and no work from the old `fix/audit-findings` branch was used as the implementation base.

## Critical 1 — agent authentication was self-attested

Status: **fixed**

### Root cause

The shared fleet bearer token authenticated access to the Hive service, but the effective caller identity still came from request-controlled URN fields. A holder of the shared token could copy another agent's URN and use it for heartbeat, claim, update, scheduling, or message mutations. There was no server-side credential-to-agent binding.

### Fix applied

- `mnemos/hive_mind/service.py`
  - Added the `agent_credentials` table and SHA-256 token-digest lookup.
  - Registration now generates a cryptographically random per-agent token with `secrets.token_urlsafe(32)`, stores only its digest, and returns the raw token once in the registration response.
  - Authentication middleware resolves the authenticated agent URN from the bearer-token digest instead of trusting a request field.
  - Added `_require_agent`, `_require_owner_or_admin`, and registration/admin authority checks. Heartbeats and mutating routes reject a claimed URN that differs from the credential-bound URN.
  - Requires the admin token to be distinct from the shared registration/bootstrap token.
- `mnemos/hive_mind/repository.py`
  - Added the persistent `agent_credentials` schema to the repository initialization path.
- `mnemos/hive_mind/mcp_bridge.py`
  - Agent MCP tools require an explicit `writeOnly` `agent_token`; no process-global agent-token map is retained. Tokens are removed from request bodies/query parameters and used only as bearer credentials.
  - MCP agent registration remains bootstrap/admin authenticated, preventing an existing agent token from minting another identity.
- `deploy/zeroclaw-fanout/zeroclaw_worker.py` and `mnemos/hive_mind/system_watcher.py`
  - Registration uses the bootstrap token, then the client retains and uses its issued per-agent token for subsequent operations.
- `README.md` and `mnemos/hive_mind/README.md`
  - Documented bootstrap, per-agent, and distinct admin credential roles.

### Regression tests

- `tests/test_audit_security_v2.py::test_agent_token_is_bound_to_one_urn_and_impersonation_is_rejected`
  - Verifies that worker B's token cannot heartbeat, claim, or mutate as worker A; verifies only a token digest is stored; verifies an agent credential cannot register a forged new identity.
  - Result: **1 passed, 0 failed**.
  - Vulnerable behavior would accept the attacker credential plus victim URN; the fixed behavior returns 403.
- `tests/test_mcp_bridge_contract.py::test_agent_tools_require_and_forward_explicit_per_agent_token`
  - Verifies explicit token requirements and forwarding without an unsafe global token cache.

### Follow-up beyond the minimal fix

The implemented per-agent bearer token is the requested minimal cryptographic identity binding. A fuller deployment should add token expiry, rotation, revocation, and ideally mTLS or signed short-lived workload identities. Those are defense-in-depth follow-ups, not prerequisites for closing this impersonation path.

## Critical 2 — jobs and routing lacked per-agent authorization

Status: **fixed**

### Root cause

`GET /v1/jobs`, `PATCH /v1/jobs/{id}/routing`, `POST /v1/jobs/next`, and `POST /v1/jobs/{id}/claim` relied on shared transport authentication and client-provided identity. Job inventory was globally visible, routing was not admin-gated, and claim decisions were not tied to a credential-bound principal.

### Fix applied

- `mnemos/hive_mind/service.py`
  - `GET /v1/jobs` now requires an agent or admin credential in authenticated mode. Agents see jobs they submitted or claimed plus only currently eligible unowned queued/offered jobs; admins may list globally. A forged agent filter is rejected.
  - Eligibility checks cover kind, required capabilities, cost tier, subscription throttling/pools, dependencies, and retry backoff.
  - Both claim routes derive the claimant from the per-agent credential and reject a mismatching asserted URN.
  - `PATCH /v1/jobs/{id}/routing` is admin-only.
  - Generic job PATCH can no longer transition queued/offered jobs into `claimed`, so claim authorization cannot be bypassed.
  - Job creation, schedule ownership, and message publication also derive identity from the authenticated principal.
- `mnemos/hive_mind/zeroclaw_triage.py`, `deploy/zeroclaw-fanout/resubmit_non_code_jobs.py`, `mnemos/hive_mind/dashboard.html`, and related documentation/tests
  - Administrative inventory/routing clients now use a distinct admin credential; job submission uses an agent credential.

### Regression tests

- `tests/test_audit_security_v2.py::test_job_listing_claims_and_routing_enforce_agent_and_admin_authority`
  - Verifies scoped listing, bootstrap-only listing denial, forged filter denial, ineligible-claim denial, generic PATCH claim-bypass denial, agent routing denial, and successful admin routing.
  - Result: **1 passed, 0 failed**.
  - Vulnerable behavior exposed the global list and accepted unauthorized routing/claim operations; the fixed behavior returns 403 or 409 as appropriate.
- `tests/test_audit_security_v2.py::test_schedule_submitter_is_derived_from_agent_credential`
  - Verifies scheduled job ownership is credential-derived rather than copied from an untrusted template field.
- `tests/test_deployment_contract.py` and `tests/test_mcp_bridge_contract.py`
  - Verify the updated credential contract for administrative scripts and MCP job operations.

## High 1 — job claim race lacked a proven single atomic winner

Status: **fixed**

### Root cause

The audited service contract allowed claim-related authorization and state mutation to be reached through multiple paths, including a generic status update, without an end-to-end regression proving one atomic winner. Although the current `main` repository layer already contained conditional claim SQL, the service still needed to force every claim through that compare-and-swap path and prevent the generic PATCH bypass.

### Fix applied

- `mnemos/hive_mind/service.py`
  - Both dequeue and direct-claim endpoints bind the claimant to its credential and delegate the state transition to the repository claim operation.
  - Generic job PATCH rejects queued/offered-to-claimed transitions.
- `mnemos/hive_mind/repository.py`
  - The claim path uses an immediate transaction and a conditional `UPDATE ... WHERE id=? AND status IN ('queued','offered') AND claimed_by IS NULL`, then requires `rowcount == 1`. A losing concurrent claimant receives a conflict and cannot overwrite the winner.
  - The dequeue operation likewise selects and conditionally updates within an immediate transaction.
- `mnemos/hive_mind/service.py`
  - Repeated terminal updates now return idempotent success before repeating event/accounting side effects.

### Regression tests

- `tests/test_audit_security_v2.py::test_direct_claim_is_single_winner_atomic_compare_and_swap`
  - Starts two simultaneous claims by eligible agents and verifies exactly one 200, one 409, and one persisted owner.
  - Result: **1 passed, 0 failed**.
  - A split authorize-then-mutate implementation could allow two successes or overwrite ownership; the conditional update permits exactly one winner.
- `tests/test_audit_security_v2.py::test_terminal_retry_is_idempotent_and_does_not_repeat_side_effects`
  - Verifies a repeated terminal acknowledgment does not duplicate accounting or event side effects.

## High 2 — heartbeat freshness was decoupled from requeue

Status: **fixed**

### Root cause

The reaper treated claim age as the primary stale-work signal instead of atomically coupling requeue to the current owner's heartbeat freshness. A healthy worker could therefore keep executing while its job was requeued for another worker, enabling duplicate execution. The zeroclaw worker's job-progress cadence was also longer than the stale-agent threshold.

### Fix applied

- `mnemos/hive_mind/service.py`
  - Added `_reap_stale_agents_and_jobs(now)`, which marks stale agents offline with a conditional update and considers a claimed/running job reclaimable only when its owner is absent or its current heartbeat is stale.
  - Requeue is a compare-and-swap update guarded by job ID, exact current owner, claimed/running state, and a `NOT EXISTS` check for a fresh heartbeat. Events are emitted only when `rowcount == 1`.
- `deploy/zeroclaw-fanout/zeroclaw_worker.py`
  - Long-running child processes now send agent heartbeats every wait-loop cycle, independently of the less frequent job-progress callback. The heartbeat function retains its own configured throttling interval.

### Regression tests

- `tests/test_audit_security_v2.py::test_reaper_uses_heartbeat_freshness_and_atomic_owner_guard`
  - Verifies a long-running job owned by a freshly heartbeating worker stays assigned while a job owned by a stale worker is atomically returned to queued/unowned state.
  - Result: **1 passed, 0 failed**.
  - The old claim-age behavior could requeue the fresh worker's job; the fixed heartbeat predicate preserves it.
- `tests/test_zeroclaw_worker_defects.py::test_per_attempt_timeout_advances_provider_chain`
  - Extended to verify multiple agent heartbeat calls occur during a long provider attempt independently of job-progress cadence.

## Verification results

Finding-specific commands were each run independently with `PYTHONPATH=.` and the existing compatible test environment:

| Finding | Test result |
|---|---:|
| Critical 1 | 1 passed, 0 failed |
| Critical 2 | 1 passed, 0 failed |
| High 1 | 1 passed, 0 failed |
| High 2 | 1 passed, 0 failed |

Supporting security/compatibility regressions were run together:

```text
4 passed, 0 failed, 2 warnings in 0.45s
```

The full real suite was run as:

```text
PYTHONPATH=. /private/tmp/fix-oauth-v2/venv/bin/pytest -q
```

Actual result:

```text
94 passed, 4 skipped, 0 failed, 2 warnings in 5.80s
```

The two warnings are dependency deprecation warnings from FastAPI/Starlette test-client internals. A first finding-specific invocation without `PYTHONPATH=.` failed during collection because the borrowed virtual environment did not add this checkout to `sys.path`; it ran no tests. The corrected invocations above collected and passed, and the full suite result is the authoritative test count.

Additional checks:

- Python `compileall`: passed.
- `git diff --check`: passed.
- Ruff: 0 findings on added lines. The checked files retain 220 pre-existing baseline findings; none were introduced by this change.
- Security review: APPROVE. The required primary zoder review was attempted but its configured endpoint was unavailable due a network error, so the secondary Codex security reviewer performed the adversarial review, requested fixes, and approved the corrected result.

## Commits

- `053e45683acd0206f1ae77218afc1910c33f3e1e` — `security(hive): bind agent identity and claims`
- `docs(audit): record v2 security fix verification` — report-only commit; its final object ID cannot be embedded in the file whose contents determine that same ID. The exact SHA is included in the final handoff.

Both commits use author and committer identity `Jason Perlow <jperlow@gmail.com>` and contain no attribution trailers.

## Final git status

After committing this report, the expected and subsequently verified status is:

```text
## fix/audit-findings-v2
?? SOL_REVIEW.md
```

`SOL_REVIEW.md` is the pre-existing untracked file present at the start of the task. It was not modified or committed. All task-authored code, tests, documentation, and this report are committed. No push was performed.
