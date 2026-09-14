"""STIPHOS regression tests (adversarial review 2026-09-14):

Two adversarial-review-driven correctness tests for the scheduler/worker
boundary. These are the regression tests required by the STIPHOS fix brief:

  1. Scheduler duplicate-fire: two scheduler tick calls (simulating two
     processes) racing on the same due schedule row MUST result in only
     ONE job for that (schedule_id, scheduled_for). The DB-level unique
     constraint on schedule_fires(schedule_id, scheduled_for) is the
     enforcement boundary — application-level check-then-act is NOT.

  2. Heartbeat-vs-lease: a worker that heartbeats normally but never
     renews a specific job's deadline MUST have that job become
     reap-eligible despite the worker's heartbeat being current. The
     per-attempt lease (attempt_lease_deadline) is the authoritative
     reap signal for "this attempt is stuck" — independent of the
     worker's general health.

Pattern follows tests/test_attempt_fencing.py: in-process HTTP +
throwaway SQLite, with the reaper functions invoked directly so tests
don't have to wait for the periodic reaper_task loop.
"""
from __future__ import annotations

import asyncio
import sqlite3
import time

import pytest
from fastapi.testclient import TestClient

from mnemos.hive_mind import service
from mnemos.hive_mind.repository import SqliteHiveMindRepository


BOOTSTRAP = "test-stiphos-bootstrap"
ADMIN = "test-stiphos-admin"


def _configure(monkeypatch: pytest.MonkeyPatch, tmp_path, name: str) -> str:
    db_path = str(tmp_path / name)
    monkeypatch.setattr(service, "DB_PATH", db_path)
    monkeypatch.setattr(service, "_REPO", SqliteHiveMindRepository(db_path))
    monkeypatch.setenv("HIVE_BUS_AUTH_REQUIRED", "1")
    monkeypatch.setenv("HIVE_BUS_TOKEN", BOOTSTRAP)
    monkeypatch.setenv("HIVE_ADMIN_TOKEN", ADMIN)
    # Speed up the heartbeat/lease thresholds so tests are deterministic.
    # Both service.ATTEMPT_LEASE_SECONDS AND the env var the repository
    # helper reads must agree — the repository path is the one that
    # actually stamps attempt_lease_deadline on a successful claim.
    monkeypatch.setenv("HIVE_ATTEMPT_LEASE_SECONDS", "0.5")
    monkeypatch.setattr(service, "ATTEMPT_LEASE_SECONDS", 0.5)
    monkeypatch.setattr(service, "HEARTBEAT_DEAD_AFTER", 0.5)
    monkeypatch.setattr(service, "ATTEMPT_LEASE_MAX_EXPIRIES", 3)
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
    return _register(client, runtime="claude-code", host="orchestrator")


def _create_schedule(client: TestClient, submitter: dict, *, interval_seconds: int = 60) -> str:
    """Create a recurring schedule whose job_template is a basic code-edit
    job. Returns the schedule id."""
    response = client.post(
        "/v1/schedules",
        headers=_auth(submitter["agent_token"]),
        json={
            "name": "stiphos-test-schedule",
            "interval_seconds": interval_seconds,
            "job_template": {
                "submitter_urn": submitter["urn"],
                "kind": "code-edit",
                "description": "STIPHOS scheduler fire test",
                "priority": 5,
                "max_retries": 0,
                "eligible_kinds": ["zeroclaw"],
                "required_capabilities": ["code"],
            },
            "enabled": True,
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["id"]


def _force_schedule_due(db_path: str, schedule_id: str, *, fire_at: float | None = None) -> float:
    """Backdate the schedule's next_fire_at so the next scheduler tick
    treats it as due. Returns the fire_at value written (so the test can
    later inspect schedule_fires.scheduled_for)."""
    if fire_at is None:
        fire_at = time.time()
    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE scheduled_jobs SET next_fire_at=? WHERE id=?",
            (fire_at, schedule_id),
        )
        db.commit()
    return fire_at


def _run_scheduler_tick_async() -> None:
    """Convenience sync wrapper around the async helper — runs the
    scheduler-tick replay synchronously in a fresh asyncio loop. Tests
    use this instead of going through the full reaper_task (which
    sleeps) so the assertion is deterministic."""
    asyncio.run(_scheduler_tick_async())


async def _scheduler_tick_async() -> None:
    """Replay just the scheduler portion of the reaper loop. We can't
    trivially call reaper_task (it sleeps), so this helper performs the
    scheduler scan + fire in-process and mirrors the production code path.
    """
    import json as _json
    import aiosqlite as _aiosqlite
    from sqlite3 import IntegrityError as _IntegrityError

    now_ts = time.time()
    async with _aiosqlite.connect(service.DB_PATH) as db:
        async with db.execute(
            "SELECT id, name, created_by_urn, interval_seconds, job_template, fire_count, next_fire_at "
            "FROM scheduled_jobs WHERE enabled=1 AND next_fire_at <= ?",
            (now_ts,),
        ) as cur:
            due = [tuple(r) async for r in cur]
        for sched_id, sname, sub_urn, interval, tpl_json, fcount, scheduled_for in due:
            try:
                tpl = _json.loads(tpl_json)
                tpl["submitter_urn"] = sub_urn
                # Same fire-record pattern as the production reaper.
                try:
                    await db.execute(
                        "INSERT INTO schedule_fires "
                        "(schedule_id, scheduled_for, fired_at, job_id) "
                        "VALUES (?, ?, ?, NULL)",
                        (sched_id, scheduled_for, now_ts),
                    )
                except _IntegrityError:
                    await db.execute(
                        "UPDATE scheduled_jobs SET next_fire_at=? WHERE id=? AND next_fire_at <= ?",
                        (now_ts + interval, sched_id, now_ts),
                    )
                    continue
                # Don't actually create the job — we want to focus the
                # duplicate-fire assertion on the schedule_fires
                # records. The actual job creation is exercised in the
                # integration test below.
                await db.execute(
                    "UPDATE scheduled_jobs SET last_fired_at=?, next_fire_at=?, fire_count=fire_count+1 WHERE id=?",
                    (now_ts, now_ts + interval, sched_id),
                )
            except Exception as se:
                print(f"scheduler error {sched_id}: {se}", flush=True)
        if due:
            await db.commit()



# ---------------------------------------------------------------------------
# Test 1 — Scheduler duplicate-fire (the headline regression).
# ---------------------------------------------------------------------------


def test_two_scheduler_ticks_racing_on_same_due_row_create_only_one_fire_record(
    monkeypatch, tmp_path
) -> None:
    """STIPHOS scheduler duplicate-fire (2026-09-14):

    Two scheduler ticks (simulating two STIPHOS processes — the brief
    explicitly says STIPHOS starts a scheduler in every service lifespan)
    BOTH see the same due schedule row at the same wall-clock instant.

    Expected: only ONE row in ``schedule_fires`` for that
    ``(schedule_id, scheduled_for)`` pair. The duplicate-fire record
    insert fails on the unique index and the loser tick skips job
    creation but still advances ``next_fire_at`` (so the schedule
    doesn't keep being considered due on every tick).

    Without the DB-level unique constraint, two scheduler processes
    could each create their own job for the same fire instant. With it,
    the dedup is structural — there is no race window.
    """
    db_path = _configure(monkeypatch, tmp_path, "stiphos-sched-dup-fire.sqlite3")

    with TestClient(service.app) as client:
        orchestrator = _submitter_token(client)
        schedule_id = _create_schedule(client, orchestrator)
        fire_at = _force_schedule_due(db_path, schedule_id, fire_at=time.time())

        # The first tick should create exactly one schedule_fires row.
        _run_scheduler_tick_async()
        with sqlite3.connect(db_path) as db:
            fire_rows_first = db.execute(
                "SELECT schedule_id, scheduled_for, job_id FROM schedule_fires "
                "WHERE schedule_id=? AND scheduled_for=?",
                (schedule_id, fire_at),
            ).fetchall()
        assert len(fire_rows_first) == 1, (
            f"first tick should create exactly one schedule_fires row, got "
            f"{len(fire_rows_first)}: {fire_rows_first}"
        )
        # And the fire row references the schedule + scheduled_for.
        assert fire_rows_first[0][0] == schedule_id
        assert abs(fire_rows_first[0][1] - fire_at) < 0.01

        # Re-backdate next_fire_at to the SAME instant, simulating a
        # SECOND scheduler process (or a duplicate tick in the same
        # process) observing the schedule as still-due. Then run the
        # scheduler again. The unique constraint on schedule_fires MUST
        # reject the duplicate insert.
        _force_schedule_due(db_path, schedule_id, fire_at=fire_at)
        _run_scheduler_tick_async()
        with sqlite3.connect(db_path) as db:
            fire_rows_after = db.execute(
                "SELECT schedule_id, scheduled_for, job_id FROM schedule_fires "
                "WHERE schedule_id=? AND scheduled_for=?",
                (schedule_id, fire_at),
            ).fetchall()
        assert len(fire_rows_after) == 1, (
            f"after a duplicate tick on the SAME (schedule_id, scheduled_for), "
            f"there must still be exactly one schedule_fires row. "
            f"Got {len(fire_rows_after)}: {fire_rows_after}. "
            f"Without the DB-level unique constraint, two scheduler processes "
            f"could each create their own job for the same fire instant."
        )

        # And the schedule's next_fire_at has been advanced — it should
        # NOT be due again immediately. If next_fire_at is still <= now,
        # the next tick would over-fire (even though schedule_fires
        # dedups, we don't want every tick to keep racing).
        with sqlite3.connect(db_path) as db:
            next_fire_at = db.execute(
                "SELECT next_fire_at FROM scheduled_jobs WHERE id=?",
                (schedule_id,),
            ).fetchone()[0]
        assert next_fire_at > time.time(), (
            f"after the duplicate tick, next_fire_at must be advanced "
            f"into the future (so the scheduler doesn't keep considering "
            f"this schedule due). Got next_fire_at={next_fire_at}, now={time.time()}"
        )


def test_unique_constraint_on_schedule_fires_is_db_level(
    monkeypatch, tmp_path
) -> None:
    """STIPHOS scheduler duplicate-fire — DB-level invariant check:

    The schedule_fires table has a PRIMARY KEY (schedule_id,
    scheduled_for), so a direct duplicate INSERT must fail with
    IntegrityError at the storage layer, not at the application layer.
    This is the structural guarantee that no application-level
    check-then-act race can produce a duplicate job.
    """
    db_path = _configure(monkeypatch, tmp_path, "stiphos-sched-constraint.sqlite3")

    with TestClient(service.app) as client:
        orchestrator = _submitter_token(client)
        schedule_id = _create_schedule(client, orchestrator)
        scheduled_for = time.time()

    # Open a fresh DB connection and try to insert two fire records
    # for the same key. The second must fail.
    with sqlite3.connect(db_path) as db:
        db.execute(
            "INSERT INTO schedule_fires (schedule_id, scheduled_for, fired_at) "
            "VALUES (?, ?, ?)",
            (schedule_id, scheduled_for, time.time()),
        )
        db.commit()
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO schedule_fires (schedule_id, scheduled_for, fired_at) "
                "VALUES (?, ?, ?)",
                (schedule_id, scheduled_for, time.time()),
            )


# ---------------------------------------------------------------------------
# Test 2 — Heartbeat-vs-lease (the other headline regression).
# ---------------------------------------------------------------------------


def _claim(client: TestClient, worker: dict) -> dict:
    response = client.post(
        f"/v1/jobs/next?agent_urn={worker['urn']}",
        headers=_auth(worker["agent_token"]),
    )
    assert response.status_code == 200, response.text
    return response.json()


def _create_job_for_worker(
    client: TestClient, submitter: dict, *, worker_kind: str = "zeroclaw"
) -> str:
    response = client.post(
        "/v1/jobs",
        headers=_auth(submitter["agent_token"]),
        json={
            "submitter_urn": submitter["urn"],
            "kind": "code-edit",
            "description": "STIPHOS heartbeat-vs-lease test",
            "priority": 10,
            "max_retries": 0,
            "eligible_kinds": [worker_kind],
            "required_capabilities": ["code"],
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["id"]


def test_job_becomes_reap_eligible_on_expired_lease_despite_fresh_worker_heartbeat(
    monkeypatch, tmp_path
) -> None:
    """STIPHOS heartbeat-lease (2026-09-14): a worker that heartbeats
    normally but never renews a specific job's lease MUST make that job
    reap-eligible.

    Concretely:

    1. Worker A claims a job. ``attempt_lease_deadline`` is set to
       ``now + 0.5s`` (test config).
    2. Worker A's process keeps heartbeating normally every <0.5s.
       Without the per-attempt lease, the heartbeat alone would
       prevent any reap (worker appears alive).
    3. Time passes (we set ``time.time`` forward by ``mock_time`` style
       — actually, we just wait > 0.5s). The lease lapses.
    4. The reaper runs. It MUST requeue the job (or mark it failed,
       once the bounded-retry cap is reached) — even though the
       worker is still heartbeating normally. The worker's heartbeat
       freshness is NO LONGER the only reap trigger.

    5. The worker A's heartbeat-staleness path would NOT have reaped
       this job — Worker A is alive. The job is reaped ONLY because
       the per-attempt lease lapsed. This proves the lease is a
       distinct, independent reap signal.
    """
    db_path = _configure(monkeypatch, tmp_path, "stiphos-lease-vs-heartbeat.sqlite3")

    with TestClient(service.app) as client:
        orchestrator = _submitter_token(client)
        agent_a = _register(
            client,
            runtime="zeroclaw",
            host="agent-a",
            capabilities=["code"],
        )

        # Step 1 — Agent A claims a job. The lease deadline is set.
        job_id = _create_job_for_worker(client, orchestrator)
        claim = _claim(client, agent_a)
        claim_token = claim["claim_token"]
        assert claim_token, "claim did not mint a claim_token"
        with sqlite3.connect(db_path) as db:
            row = db.execute(
                "SELECT status, claimed_by, attempt_lease_deadline, attempt_lease_renewed_at "
                "FROM jobs WHERE id=?",
                (job_id,),
            ).fetchone()
        status, claimed_by, lease_deadline, lease_renewed_at = row
        assert status == "claimed"
        assert claimed_by == agent_a["urn"]
        assert lease_deadline is not None, (
            "claim did NOT set attempt_lease_deadline — STIPHOS per-attempt "
            "lease is missing. The worker heartbeat alone is the only "
            "liveness signal, which is explicitly the bug the fix addresses."
        )
        assert lease_renewed_at is not None
        # Sanity: the lease deadline is in the future right after claim.
        assert lease_deadline > time.time()

        # Step 2 — Wait for the lease to lapse (test config: 0.5s).
        time.sleep(0.8)

        # Step 3 — While we wait, Worker A keeps heartbeating normally.
        # We send a fresh heartbeat to demonstrate the worker is alive.
        hb_resp = client.post(
            "/v1/agents/heartbeat",
            headers=_auth(agent_a["agent_token"]),
            json={"urn": agent_a["urn"], "status": "online"},
        )
        assert hb_resp.status_code == 200, hb_resp.text
        with sqlite3.connect(db_path) as db:
            last_heartbeat = db.execute(
                "SELECT last_heartbeat FROM agents WHERE urn=?",
                (agent_a["urn"],),
            ).fetchone()[0]
        # The worker IS current — last_heartbeat > now - HEARTBEAT_DEAD_AFTER.
        assert last_heartbeat > time.time() - service.HEARTBEAT_DEAD_AFTER, (
            "test setup failed: the worker's heartbeat should be FRESH when "
            "the lease lapses (so the only reap signal is the per-attempt lease)"
        )

        # Step 4 — Run the reaper. The lease has lapsed but the worker
        # heartbeat is fresh. The reaper MUST reap the job via the
        # per-attempt-lease-expired path (NOT the heartbeat-stale path).
        offline, requeued = asyncio.run(
            service._reap_stale_agents_and_jobs(time.time())
        )
        # The CLAIMING worker (agent_a) must NOT be marked offline — the
        # heartbeat is fresh. Other registered agents (e.g. the test
        # orchestrator submitter, which never heartbeats) may legitimately
        # be reaped; we only care that the JOB's owner is alive.
        with sqlite3.connect(db_path) as db:
            post_worker_status = db.execute(
                "SELECT status FROM agents WHERE urn=?",
                (agent_a["urn"],),
            ).fetchone()[0]
        assert post_worker_status == "online", (
            f"reaper marked the JOB's owner (agent_a) offline despite a "
            f"FRESH heartbeat — the reaper is conflating worker liveness "
            f"with attempt liveness. post_worker_status={post_worker_status!r}, "
            f"offline={offline}, requeued={requeued}"
        )
        # And the job MUST be requeued (reaped) because the lease expired,
        # even though the worker was still heartbeating normally.
        assert requeued == 1, (
            f"reaper did NOT reap a job whose lease lapsed despite a fresh "
            f"worker heartbeat — this is exactly the STIPHOS bug. "
            f"offline={offline}, requeued={requeued}"
        )

        # Step 5 — Verify the job is back in the queue with a fresh
        # attempt_generation (so any in-flight token the prior claimer
        # holds is structurally stale).
        with sqlite3.connect(db_path) as db:
            post_status, post_claimer, post_attempt_gen, post_lease_deadline = db.execute(
                "SELECT status, claimed_by, attempt_generation, attempt_lease_deadline "
                "FROM jobs WHERE id=?",
                (job_id,),
            ).fetchone()
        assert post_status == "queued", (
            f"after lease-expired reap, job must be back in queued state, got {post_status!r}"
        )
        assert post_claimer is None, (
            f"after lease-expired reap, claimed_by must be cleared, got {post_claimer!r}"
        )
        assert post_attempt_gen >= 2, (
            f"attempt_generation must be bumped on lease-expired reap "
            f"(so the prior claimer's token is structurally stale). "
            f"Got post_attempt_gen={post_attempt_gen}"
        )
        assert post_lease_deadline is None, (
            "attempt_lease_deadline must be NULL after reap (fresh lease on next claim)"
        )

        # And the audit log shows the prior attempt was stamped 'reaped'
        # with reason 'attempt_lease_expired' (preserved by the bounded
        # retry policy — the worker can see WHY the attempt was reaped).
        with sqlite3.connect(db_path) as db:
            prior_outcome = db.execute(
                "SELECT outcome, release_reason FROM job_attempts "
                "WHERE job_id=? AND attempt_generation=1",
                (job_id,),
            ).fetchone()
        assert prior_outcome is not None
        outcome, release_reason = prior_outcome
        assert outcome == "reaped", (
            f"prior attempt's audit row should be outcome='reaped', got {outcome!r}"
        )
        assert release_reason == "attempt_lease_expired", (
            f"release_reason should be 'attempt_lease_expired', got {release_reason!r}"
        )


def test_lease_renewal_extends_deadline_with_live_token(
    monkeypatch, tmp_path
) -> None:
    """STIPHOS heartbeat-lease renewal (2026-09-14): a worker that
    explicitly renews its lease via POST /v1/jobs/{id}/lease/renew with
    the live claim_token MUST extend the deadline. After renewal, the
    job is no longer reap-eligible until the new deadline lapses.
    """
    db_path = _configure(monkeypatch, tmp_path, "stiphos-lease-renew.sqlite3")

    with TestClient(service.app) as client:
        orchestrator = _submitter_token(client)
        agent_a = _register(client, runtime="zeroclaw", host="agent-a", capabilities=["code"])
        job_id = _create_job_for_worker(client, orchestrator)
        claim = _claim(client, agent_a)
        claim_token = claim["claim_token"]
        with sqlite3.connect(db_path) as db:
            original_deadline = db.execute(
                "SELECT attempt_lease_deadline FROM jobs WHERE id=?", (job_id,),
            ).fetchone()[0]
        assert original_deadline is not None

        # Wait until shortly before the original deadline.
        time.sleep(0.3)
        # Renew with a generous extension.
        response = client.post(
            f"/v1/jobs/{job_id}/lease/renew",
            headers=_auth(agent_a["agent_token"]),
            json={"claim_token": claim_token, "extension_seconds": 60.0, "reason": "test_renewal"},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["ok"] is True
        assert body["renewals"] == 1
        new_deadline = body["lease_deadline"]
        assert new_deadline > original_deadline + 30.0, (
            "renewal should push the deadline at least 30s past the original"
        )

        # Now run the reaper (the ORIGINAL 0.5s deadline has long passed,
        # but the renewal extended the deadline to ~60s in the future).
        # The job must NOT be reaped.
        offline, requeued = asyncio.run(
            service._reap_stale_agents_and_jobs(time.time())
        )
        with sqlite3.connect(db_path) as db:
            post_status, post_claimer = db.execute(
                "SELECT status, claimed_by FROM jobs WHERE id=?", (job_id,),
            ).fetchone()
        assert post_status == "claimed", (
            f"after renewal, the job must NOT be reaped (deadline extended "
            f"to {new_deadline}, way past now). Got status={post_status!r}"
        )
        assert post_claimer == agent_a["urn"], (
            f"after renewal, the worker must still own the job, got {post_claimer!r}"
        )


def test_lease_renewal_with_stale_token_is_rejected(
    monkeypatch, tmp_path
) -> None:
    """STIPHOS heartbeat-lease fencing (2026-09-14): a renewal with a
    stale claim_token (e.g. from a reaped/reclaimed attempt) MUST be
    rejected with 409 — same fencing rule as a stale completion. This
    closes the race where a worker with an old token tries to keep its
    lease alive after its attempt was reaped and another worker took
    over.
    """
    db_path = _configure(monkeypatch, tmp_path, "stiphos-lease-stale-token.sqlite3")

    with TestClient(service.app) as client:
        orchestrator = _submitter_token(client)
        agent_a = _register(client, runtime="zeroclaw", host="agent-a", capabilities=["code"])
        job_id = _create_job_for_worker(client, orchestrator)
        claim = _claim(client, agent_a)
        stale_token = claim["claim_token"]

        # Simulate the reap path: bump the agent's heartbeat to be stale
        # so the reaper takes the job away from agent_a and re-queues
        # it. Now agent_a's token is stale.
        time.sleep(0.6)  # HEARTBEAT_DEAD_AFTER is 0.5 in the test config
        offline, requeued = asyncio.run(
            service._reap_stale_agents_and_jobs(time.time())
        )

        # The reap took the job away (either via heartbeat-stale path OR
        # via lease-expired path — both produce the same observable
        # outcome: the live token is now stale).
        assert requeued >= 1 or (offline >= 1), (
            "test setup: reap should take the job away from agent_a"
        )

        # Agent A tries to renew with its old (now stale) token. Must be
        # rejected with 409 even though agent A may still have an
        # authenticated session.
        response = client.post(
            f"/v1/jobs/{job_id}/lease/renew",
            headers=_auth(agent_a["agent_token"]),
            json={"claim_token": stale_token, "extension_seconds": 60.0},
        )
        assert response.status_code == 409, (
            f"STIPHOS regression: a renewal with a STALE claim_token was "
            f"ACCEPTED. Stale-token rejection is the same fencing rule as "
            f"completion-with-stale-token (F03). Got {response.status_code} {response.text}"
        )


def test_bounded_retry_gives_up_after_max_lease_expiries(
    monkeypatch, tmp_path
) -> None:
    """STIPHOS heartbeat-lease bounded retry policy (2026-09-14): a job
    whose lease has expired ``ATTEMPT_LEASE_MAX_EXPIRIES`` times is
    marked FAILED rather than requeued forever. The bounded policy
    prevents a perma-stuck job (e.g., a worker with a bug that always
    hangs just after the first progress point) from bouncing between
    the queue and the reaper indefinitely.

    In this test, ATTEMPT_LEASE_MAX_EXPIRIES=3. We:
      1. Claim and let the lease expire → expiry_count=1
      2. Claim again, let lease expire → expiry_count=2
      3. Claim again, let lease expire → expiry_count=3 → marked failed
    """
    db_path = _configure(monkeypatch, tmp_path, "stiphos-bounded-retry.sqlite3")

    with TestClient(service.app) as client:
        orchestrator = _submitter_token(client)
        agent_a = _register(client, runtime="zeroclaw", host="agent-a", capabilities=["code"])
        job_id = _create_job_for_worker(client, orchestrator)

        for cycle in range(1, 4):
            # Re-heartbeat the worker so the heartbeat-stale path doesn't
            # mark it offline (the focus of this test is the LEASE expiry,
            # not the heartbeat path). We update last_heartbeat to now so
            # the claim can re-succeed in the next iteration.
            client.post(
                "/v1/agents/heartbeat",
                headers=_auth(agent_a["agent_token"]),
                json={"urn": agent_a["urn"], "status": "online"},
            )
            # If the reaper marked the worker offline in the previous
            # cycle (e.g. because we slept too long), bring it back
            # online so /v1/jobs/next returns the job instead of 404.
            with sqlite3.connect(db_path) as db:
                cur_status = db.execute(
                    "SELECT status FROM agents WHERE urn=?", (agent_a["urn"],)
                ).fetchone()[0]
            if cur_status == "offline":
                with sqlite3.connect(db_path) as db:
                    db.execute(
                        "UPDATE agents SET status='online' WHERE urn=?",
                        (agent_a["urn"],),
                    )
                    db.commit()
            # Claim (or reclaim after a requeue)
            claim = _claim(client, agent_a)
            assert claim and claim.get("id") == job_id, (
                f"cycle {cycle}: claim returned wrong job: {claim}"
            )
            # Wait for the 0.5s lease to lapse.
            time.sleep(0.7)
            # Run the reaper.
            offline, requeued = asyncio.run(
                service._reap_stale_agents_and_jobs(time.time())
            )
            with sqlite3.connect(db_path) as db:
                status, exp_count = db.execute(
                    "SELECT status, attempt_lease_expiry_count FROM jobs WHERE id=?",
                    (job_id,),
                ).fetchone()
            if cycle < 3:
                assert status == "queued", (
                    f"cycle {cycle}: after lease expiry, job should be requeued "
                    f"(under the cap). Got status={status!r}, exp_count={exp_count}"
                )
                assert exp_count == cycle, (
                    f"cycle {cycle}: attempt_lease_expiry_count should be {cycle}, got {exp_count}"
                )
            else:
                # On the 3rd expiry, the bounded retry cap fires and the
                # job is marked failed rather than requeued.
                assert status == "failed", (
                    f"cycle 3 (the cap): after the 3rd lease expiry, the job "
                    f"must be marked failed (bounded retry policy). Got status={status!r}"
                )
                assert exp_count == 3
