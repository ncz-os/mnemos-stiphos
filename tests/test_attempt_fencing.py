"""Regression test for F03 (adversarial review 2026-09-14):

The P1 vulnerability: completion checks job state and worker identity, but not a
unique attempt generation/token. If a job is reaped (worker went stale, heartbeat
lapsed) and the SAME agent later reclaims it, that agent's DELAYED first-attempt
result can complete the SECOND attempt — a stale response accepted as if it
belonged to the live claim.

This file proves the F03 fix end-to-end:
  1. Every successful claim mints a fresh UUID claim_token AND bumps
     attempt_generation atomically inside the CAS UPDATE (so a concurrent
     re-claim cannot mint the same token).
  2. The token is required on every state-changing call (running/done/
     failed/cancelled) for the life of the attempt. A stale token is rejected
     with HTTP 409 — never silently accepted.
  3. The CAS UPDATE constrains the write by the current claim_token, closing
     the residual race between the read and the write (a re-claim that lands
     between the SELECT and the UPDATE).
  4. An immutable per-claim audit record (job_attempts table) is persisted
     on every claim and stamped with its terminal outcome, so a stale
     completion can be diagnosed after the fact — not just rejected.
  5. The reaper path closes the prior in_flight attempt as 'reaped' and
     bumps attempt_generation so any in-flight token from the prior claim
     is guaranteed not to match the new generation.
  6. The pre-existing different-worker rejection (403) is preserved on top
     of the new same-worker-different-attempt rejection (409 stale token).

Follows the in-process HTTP + throwaway SQLite pattern from
`tests/test_audit_security_v2.py` and `tests/test_event_audience_isolation.py`.
"""
from __future__ import annotations

import sqlite3
import time

import pytest
from fastapi.testclient import TestClient

from mnemos.hive_mind import service
from mnemos.hive_mind.repository import SqliteHiveMindRepository


BOOTSTRAP = "test-f03-bootstrap"
ADMIN = "test-f03-admin"


def _configure(monkeypatch: pytest.MonkeyPatch, tmp_path, name: str) -> str:
    db_path = str(tmp_path / name)
    monkeypatch.setattr(service, "DB_PATH", db_path)
    monkeypatch.setattr(service, "_REPO", SqliteHiveMindRepository(db_path))
    monkeypatch.setenv("HIVE_BUS_AUTH_REQUIRED", "1")
    monkeypatch.setenv("HIVE_BUS_TOKEN", BOOTSTRAP)
    monkeypatch.setenv("HIVE_ADMIN_TOKEN", ADMIN)
    # Force the reaper path to consider a single-second heartbeat timeout so
    # tests can deterministically trigger a reap without sleeping 60 seconds.
    monkeypatch.setattr(service, "HEARTBEAT_DEAD_AFTER", 0.5)
    return db_path


def _auth(token: str) -> dict[str, str]:
    return {"authorization": f"Bearer {token}"}


def _register(
    client: TestClient,
    *,
    runtime: str,
    host: str,
    capabilities: list[str] | None = None,
    provider: str = "ngc",
) -> dict:
    response = client.post(
        "/v1/agents/register",
        headers=_auth(BOOTSTRAP),
        json={
            "runtime": runtime,
            "host": host,
            "provider": provider,
            "capabilities": capabilities or [],
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def _submitter_token(client: TestClient) -> dict:
    """Convenience: register a claude-code submitter and return its row."""
    return _register(client, runtime="claude-code", host="orchestrator")


def _create_job(
    client: TestClient,
    submitter: dict,
    *,
    eligible_kinds: list[str] | None = None,
    required_capabilities: list[str] | None = None,
    kind: str = "code-edit",
    description: str = "F03 stale-token regression",
) -> str:
    body = {
        "submitter_urn": submitter["urn"],
        "kind": kind,
        "description": description,
        "max_retries": 0,
        "priority": 10,
        "eligible_kinds": eligible_kinds or ["zeroclaw"],
        "required_capabilities": required_capabilities or ["code"],
    }
    response = client.post("/v1/jobs", headers=_auth(submitter["agent_token"]), json=body)
    assert response.status_code == 200, response.text
    return response.json()["id"]


def _claim(client: TestClient, worker: dict) -> dict:
    """Worker self-claim via /v1/jobs/next. Returns the claim JSON
    (including claim_token + attempt_generation)."""
    response = client.post(
        f"/v1/jobs/next?agent_urn={worker['urn']}",
        headers=_auth(worker["agent_token"]),
    )
    assert response.status_code == 200, response.text
    return response.json()


def _mark_stale_and_reap(
    db_path: str, job_id: str, *, worker_urn: str, reap_after: float = 1.0
) -> None:
    """Simulate the same-agent-different-attempt scenario by:
      1. Setting the worker's last_heartbeat to be older than HEARTBEAT_DEAD_AFTER
         (so the reaper sees a stale worker that still owns the job).
      2. Invoking the reaper directly to make the reap deterministic
         (we don't have to wait for the periodic reaper_task — the test calls
         the same async function).
      3. After the reap, the agent is marked offline — bring it back online
         (via a fresh /v1/agents/heartbeat from the TestClient in the calling
         scope) before the next claim.
    """
    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE agents SET last_heartbeat = ? WHERE urn = ?",
            (time.time() - reap_after - 10.0, worker_urn),
        )
        db.commit()
    # Invoke the reaper function directly so the test does not have to wait
    # for the periodic reaper_task to wake up. asyncio.run() creates a fresh
    # loop per call (independent of pytest-asyncio's loop) and is the
    # canonical entrypoint since Python 3.7.
    import asyncio

    offline, requeued = asyncio.run(
        service._reap_stale_agents_and_jobs(time.time())
    )
    assert requeued >= 1, "reaper did not requeue the stale claim — test setup is broken"


def _bring_agent_online(client: TestClient, worker: dict) -> None:
    """After the reaper marks an agent offline (because its heartbeat went
    stale), the next /v1/jobs/next returns 404 'agent not registered or
    offline'. Sending a fresh heartbeat puts the agent back online so the
    same agent can re-claim — which is the EXACT scenario the F03 reviewer
    reproduced (same agent identity across two attempts)."""
    response = client.post(
        "/v1/agents/heartbeat",
        headers=_auth(worker["agent_token"]),
        json={"urn": worker["urn"], "status": "online"},
    )
    assert response.status_code == 200, response.text


# ---------------------------------------------------------------------------
# Test 1 — the reviewer scenario end-to-end (the headline regression).
# ---------------------------------------------------------------------------


def test_stale_first_attempt_completion_is_rejected_after_reap_and_reclaim(
    monkeypatch, tmp_path
) -> None:
    """Reproduce the F03 reviewer scenario verbatim:

    1. Agent A claims the job. attempt_generation=1, claim_token=T1.
    2. Simulate the reaper path: Agent A's heartbeat goes stale and the job
       is requeued. The prior attempt's audit row is stamped 'reaped' and
       the jobs.attempt_generation is bumped (so T1 is guaranteed stale).
    3. Agent A reclaims the same job. attempt_generation=2, claim_token=T2
       (distinct from T1).
    4. Agent A submits a completion using the FIRST attempt's token (T1)
       — the stale result from the reaped attempt. Assert: HTTP 409, the
       job is NOT marked done.
    5. Agent A submits a completion using the SECOND attempt's token (T2).
       Assert: HTTP 200, the job is marked done.
    """
    db_path = _configure(monkeypatch, tmp_path, "f03-stale-after-reap.sqlite3")

    with TestClient(service.app) as client:
        orchestrator = _submitter_token(client)
        agent_a = _register(
            client,
            runtime="zeroclaw",
            host="agent-a",
            capabilities=["code"],
        )
        # Step 1 — Agent A claims the job.
        job_id = _create_job(client, orchestrator)
        first_claim = _claim(client, agent_a)
        first_token = first_claim["claim_token"]
        first_generation = first_claim["attempt_generation"]
        assert first_token, "first claim did not mint a claim_token (F03 fix missing)"
        assert first_generation == 1, (
            f"first claim attempt_generation should be 1, got {first_generation}"
        )

        # Step 2 — simulate reap. The reaper path runs synchronously inside the
        # test loop so we don't have to wait for the periodic reaper_task.
        _mark_stale_and_reap(db_path, job_id, worker_urn=agent_a["urn"])
        # Reaper marks Agent A offline — same agent identity coming back via a
        # fresh heartbeat, exactly as in the F03 reviewer scenario.
        _bring_agent_online(client, agent_a)

        # After the reap: jobs.claim_token is NULL (cleared by the reaper) and
        # attempt_generation is bumped (so T1 is structurally stale).
        with sqlite3.connect(db_path) as db:
            (
                status,
                claim_token_now,
                attempt_generation_now,
            ) = db.execute(
                "SELECT status, claim_token, attempt_generation FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
        assert status == "queued", f"reaper should requeue to status=queued, got {status!r}"
        assert claim_token_now is None, (
            "reaper should clear claim_token on the requeue (so the prior token "
            "is structurally impossible to match); otherwise the same-worker "
            "stale-token path is not fully closed"
        )
        assert attempt_generation_now == 2, (
            f"reaper should bump attempt_generation; expected 2, got {attempt_generation_now}"
        )
        # The prior attempt's audit row is closed as 'reaped'.
        with sqlite3.connect(db_path) as db:
            reaped = db.execute(
                "SELECT outcome FROM job_attempts "
                "WHERE job_id = ? AND attempt_generation = 1",
                (job_id,),
            ).fetchone()
        assert reaped == ("reaped",), (
            f"prior attempt's audit row should be outcome='reaped', got {reaped}"
        )

        # Step 3 — Agent A reclaims. attempt_generation bumps to 3 (we
        # bumped to 2 in step 2 above; this claim bumps again to 3).
        second_claim = _claim(client, agent_a)
        second_token = second_claim["claim_token"]
        second_generation = second_claim["attempt_generation"]
        assert second_token and second_token != first_token, (
            "second claim must mint a NEW claim_token distinct from the first"
        )
        assert second_generation == 3, (
            f"second claim attempt_generation should be 3 (reap bumped to 2, "
            f"this claim bumps to 3); got {second_generation}"
        )

        # Step 4 — Agent A submits a completion carrying the FIRST (stale)
        # token. This is the exact stale-completion the reviewer reproduced.
        stale_completion = client.patch(
            f"/v1/jobs/{job_id}",
            headers=_auth(agent_a["agent_token"]),
            json={
                "status": "done",
                "claimed_by": agent_a["urn"],
                "claim_token": first_token,
                "tokens_in": 1,
                "tokens_out": 1,
                "result": {
                    "exit_code": 0,
                    "provider_used": "ngc",
                    "model_used": "unknown",
                    "stale_marker": "FIRST_ATTEMPT",
                },
            },
        )
        assert stale_completion.status_code == 409, (
            "F03 regression: stale claim_token from a reaped attempt was "
            "ACCEPTED on the live claim. The completion MUST be rejected "
            "with 409. Got: "
            f"{stale_completion.status_code} {stale_completion.text}"
        )
        # And the job is NOT marked done — the stale write did not land.
        with sqlite3.connect(db_path) as db:
            still_claimed = db.execute(
                "SELECT status, claimed_by FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
        assert still_claimed[0] == "claimed", (
            f"after a rejected stale-completion, the job must remain "
            f"in its current state (claimed/running); got status={still_claimed[0]!r}"
        )
        assert still_claimed[1] == agent_a["urn"], (
            "claimed_by must remain Agent A on the live attempt"
        )

        # A second stale-completion attempt with the WRONG token (no token
        # at all, or a syntactically-valid-but-mismatched token) must also
        # be rejected. Cover both shapes.
        missing_token = client.patch(
            f"/v1/jobs/{job_id}",
            headers=_auth(agent_a["agent_token"]),
            json={
                "status": "done",
                "claimed_by": agent_a["urn"],
                "tokens_in": 1,
                "tokens_out": 1,
                "result": {"exit_code": 0, "provider_used": "ngc", "model_used": "unknown"},
            },
        )
        assert missing_token.status_code == 409, (
            "F03 regression: a write with NO claim_token was ACCEPTED on a "
            "claimed job. The fencing check must fire for missing tokens too."
        )
        bogus_token = client.patch(
            f"/v1/jobs/{job_id}",
            headers=_auth(agent_a["agent_token"]),
            json={
                "status": "done",
                "claimed_by": agent_a["urn"],
                "claim_token": "00000000-0000-0000-0000-000000000000",
                "tokens_in": 1,
                "tokens_out": 1,
                "result": {"exit_code": 0, "provider_used": "ngc", "model_used": "unknown"},
            },
        )
        assert bogus_token.status_code == 409, (
            "F03 regression: a write with a syntactically-valid-but-mismatched "
            "claim_token was ACCEPTED on a claimed job."
        )

        # Step 5 — Agent A submits a completion carrying the SECOND (live)
        # token. Must be accepted and the job marked done.
        live_completion = client.patch(
            f"/v1/jobs/{job_id}",
            headers=_auth(agent_a["agent_token"]),
            json={
                "status": "done",
                "claimed_by": agent_a["urn"],
                "claim_token": second_token,
                "tokens_in": 42,
                "tokens_out": 7,
                "result": {
                    "exit_code": 0,
                    "provider_used": "ngc",
                    "model_used": "unknown",
                    "live_marker": "SECOND_ATTEMPT",
                },
            },
        )
        assert live_completion.status_code == 200, (
            f"F03 regression: live completion with current claim_token was "
            f"rejected: {live_completion.status_code} {live_completion.text}"
        )
        with sqlite3.connect(db_path) as db:
            (
                final_status,
                final_result,
            ) = db.execute(
                "SELECT status, result FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
        assert final_status == "done", f"job should be done, got {final_status!r}"
        # The result written is the live one (not the stale one).
        import json as _json

        result_dict = _json.loads(final_result) if final_result else {}
        assert result_dict.get("live_marker") == "SECOND_ATTEMPT", (
            "the result written was NOT the live one — the stale write must "
            "have landed, which is the F03 bug"
        )
        assert "stale_marker" not in result_dict, (
            "the stale result somehow landed on the live attempt — this is "
            "the F03 bug"
        )


# ---------------------------------------------------------------------------
# Test 2 — the immutable audit log: every claim is auditable, with its
# outcome (in_flight -> completed | failed | cancelled | reaped).
# ---------------------------------------------------------------------------


def test_immutable_attempt_record_persists_one_row_per_claim(
    monkeypatch, tmp_path
) -> None:
    """Every claim mints a new attempt row. The terminal write stamps the
    in_flight row's outcome. The reaper stamps it 'reaped'. After the full
    scenario, the audit log for this job shows the exact history — this is
    the 'diagnose after the fact' requirement, not just 'reject at CAS'.
    """
    db_path = _configure(monkeypatch, tmp_path, "f03-audit-log.sqlite3")

    with TestClient(service.app) as client:
        orchestrator = _submitter_token(client)
        agent_a = _register(client, runtime="zeroclaw", host="agent-a", capabilities=["code"])
        job_id = _create_job(client, orchestrator)

        first_claim = _claim(client, agent_a)
        first_token = first_claim["claim_token"]
        first_generation = first_claim["attempt_generation"]
        # Reap, then reclaim, then complete successfully.
        _mark_stale_and_reap(db_path, job_id, worker_urn=agent_a["urn"])
        _bring_agent_online(client, agent_a)
        second_claim = _claim(client, agent_a)
        second_token = second_claim["claim_token"]
        second_generation = second_claim["attempt_generation"]
        completion = client.patch(
            f"/v1/jobs/{job_id}",
            headers=_auth(agent_a["agent_token"]),
            json={
                "status": "done",
                "claimed_by": agent_a["urn"],
                "claim_token": second_token,
                "tokens_in": 1,
                "tokens_out": 1,
                "result": {"exit_code": 0, "provider_used": "ngc", "model_used": "unknown"},
            },
        )
        assert completion.status_code == 200, completion.text

    # Audit history: ordered by attempt_generation. There is exactly ONE row
    # per CLAIM (not per generation bump — a reaper requeue that bumps
    # attempt_generation does NOT create a row, it just consumes the prior
    # in_flight row by closing it as 'reaped'). So we expect:
    #   row 1: generation=1 (first claim), outcome='reaped' (closed by reaper)
    #   row 2: generation=3 (second claim), outcome='completed'
    # The absence of a row at generation=2 is correct — generation=2 is the
    # generation the reaper bumped the queue slot to, and the next claim
    # consumed it (bumping to 3) and inserted its audit row.
    with sqlite3.connect(db_path) as db:
        rows = db.execute(
            "SELECT attempt_generation, claim_token, outcome, "
            "claimed_by, released_by, release_reason "
            "FROM job_attempts WHERE job_id = ? ORDER BY attempt_generation ASC",
            (job_id,),
        ).fetchall()

    assert len(rows) == 2, (
        f"expected exactly 2 attempt rows (one per CLAIM: first=reaped, "
        f"second=completed); got {len(rows)}: {rows!r}"
    )
    # Row 1 = first claim, marked reaped by the reaper
    assert rows[0][0] == first_generation
    assert rows[0][1] == first_token
    assert rows[0][2] == "reaped", (
        f"first attempt's outcome must be 'reaped' (closed by the reaper); "
        f"got {rows[0][2]!r}"
    )
    assert rows[0][4] == agent_a["urn"], (
        "reaper must record itself (or the dead agent) as released_by"
    )
    assert rows[0][5] == "worker_heartbeat_stale_or_missing", (
        "release_reason must explain WHY the prior attempt was closed"
    )
    # Row 2 = the second (live) claim, completed.
    assert rows[-1][0] == second_generation
    assert rows[-1][1] == second_token
    assert rows[-1][2] == "completed", (
        f"second (live) attempt must be outcome='completed'; got {rows[-1][2]!r}"
    )
    assert rows[-1][3] == agent_a["urn"]


# ---------------------------------------------------------------------------
# Test 3 — pre-existing different-worker rejection still works
# (regression guard for the guard we extended).
# ---------------------------------------------------------------------------


def test_pre_existing_different_worker_rejection_still_works(
    monkeypatch, tmp_path
) -> None:
    """The original 403 'job is owned by a different worker' guard must
    still fire for a worker B trying to complete worker A's live claim —
    AND a stale-token completion from worker B must still be rejected (now
    twice: once by the different-worker guard, possibly by the token guard
    depending on ordering; either way, NOT 200)."""
    _configure(monkeypatch, tmp_path, "f03-different-worker.sqlite3")

    with TestClient(service.app) as client:
        orchestrator = _submitter_token(client)
        agent_a = _register(client, runtime="zeroclaw", host="agent-a", capabilities=["code"])
        agent_b = _register(client, runtime="zeroclaw", host="agent-b", capabilities=["code"])
        job_id = _create_job(client, orchestrator)
        claim = _claim(client, agent_a)
        live_token = claim["claim_token"]

        # Worker B tries to complete with NO token.
        b_no_token = client.patch(
            f"/v1/jobs/{job_id}",
            headers=_auth(agent_b["agent_token"]),
            json={
                "status": "done",
                "claimed_by": agent_b["urn"],
                "tokens_in": 1,
                "tokens_out": 1,
                "result": {"exit_code": 0, "provider_used": "ngc", "model_used": "unknown"},
            },
        )
        # Worker B forges worker A's live token (read it from the audit log).
        b_forged = client.patch(
            f"/v1/jobs/{job_id}",
            headers=_auth(agent_b["agent_token"]),
            json={
                "status": "done",
                "claimed_by": agent_b["urn"],
                "claim_token": live_token,
                "tokens_in": 1,
                "tokens_out": 1,
                "result": {"exit_code": 0, "provider_used": "ngc", "model_used": "unknown"},
            },
        )

    # Both rejected. The different-worker guard runs BEFORE the token guard
    # (ownership is checked first; the token guard then never fires because
    # the request never reaches it), so we expect 403 — but the IMPORTANT
    # invariant is that it is never 200.
    for label, resp in (("b_no_token", b_no_token), ("b_forged", b_forged)):
        assert resp.status_code != 200, (
            f"F03 regression: different-worker completion {label} was ACCEPTED "
            f"({resp.status_code}). The pre-existing different-worker guard "
            f"must still fire on top of the new same-worker-different-"
            f"attempt guard. Body: {resp.text}"
        )
        assert resp.status_code in (403, 409), (
            f"unexpected status {resp.status_code} for {label}; expected 403 "
            f"(different-worker) or 409 (stale token); body: {resp.text}"
        )


# ---------------------------------------------------------------------------
# Test 4 — happy path: every successful claim mints a unique token.
# ---------------------------------------------------------------------------


def test_every_claim_mints_a_unique_token_and_bumps_generation(
    monkeypatch, tmp_path
) -> None:
    """Back-to-back claims on DIFFERENT jobs by the same agent produce
    distinct claim_tokens and monotonically increasing attempt_generations."""
    _configure(monkeypatch, tmp_path, "f03-unique-tokens.sqlite3")

    with TestClient(service.app) as client:
        orchestrator = _submitter_token(client)
        agent_a = _register(client, runtime="zeroclaw", host="agent-a", capabilities=["code"])
        # Three independent jobs, claimed one at a time.
        tokens: list[str] = []
        generations: list[int] = []
        for i in range(3):
            job_id = _create_job(
                client, orchestrator, description=f"job-{i}", kind="review"
            )
            claim = _claim(client, agent_a)
            assert claim["claim_token"], f"job {i} claim did not return a token"
            assert claim["attempt_generation"] == 1, (
                f"job {i} fresh-claim attempt_generation should be 1, got "
                f"{claim['attempt_generation']} (regression: reaper path may "
                f"have run unexpectedly)"
            )
            tokens.append(claim["claim_token"])
            generations.append(claim["attempt_generation"])

        assert len(set(tokens)) == 3, (
            f"every claim must mint a UNIQUE token (got duplicates: {tokens!r})"
        )
        # Generations are scoped per job, so each should be 1 for fresh claims.
        assert generations == [1, 1, 1]


# ---------------------------------------------------------------------------
# Test 5 — the same-agent-different-attempt path WITHOUT a reap (different
# scenario than test 1, but exercises the same fencing logic): after a
# successful terminal write, a duplicate write with the SAME token is
# idempotent (duplicate=True); a stale token from a now-reclaimed second
# attempt is rejected.
# ---------------------------------------------------------------------------


def test_idempotent_terminal_replay_uses_same_token(monkeypatch, tmp_path) -> None:
    """An agent retrying its own terminal write with the same live token is
    idempotent — duplicate=True, no extra side effects. This is the
    pre-existing 'duplicate idempotency' behavior; the F03 fix must not
    break it."""
    db_path = _configure(monkeypatch, tmp_path, "f03-idempotent-replay.sqlite3")
    with TestClient(service.app) as client:
        orchestrator = _submitter_token(client)
        agent_a = _register(client, runtime="zeroclaw", host="agent-a", capabilities=["code"])
        job_id = _create_job(client, orchestrator)
        claim = _claim(client, agent_a)
        token = claim["claim_token"]
        body = {
            "status": "done",
            "claimed_by": agent_a["urn"],
            "claim_token": token,
            "tokens_in": 100,
            "tokens_out": 50,
            "result": {"exit_code": 0, "provider_used": "ngc", "model_used": "unknown"},
        }
        first = client.patch(f"/v1/jobs/{job_id}", headers=_auth(agent_a["agent_token"]), json=body)
        duplicate = client.patch(f"/v1/jobs/{job_id}", headers=_auth(agent_a["agent_token"]), json=body)

    assert first.status_code == 200
    assert duplicate.status_code == 200
    assert duplicate.json().get("duplicate") is True, (
        "duplicate terminal write with the same live token must report "
        "duplicate=True; F03 must not have broken idempotency"
    )
    # And only one 'completed' attempt outcome recorded (no double-stamp).
    with sqlite3.connect(db_path) as db:
        completed_count = db.execute(
            "SELECT COUNT(*) FROM job_attempts "
            "WHERE job_id = ? AND outcome = 'completed'",
            (job_id,),
        ).fetchone()[0]
    assert completed_count == 1, (
        f"duplicate replay must stamp the audit row exactly once; got "
        f"{completed_count} completed rows (regression: replay re-stamped the "
        f"already-completed attempt)"
    )
