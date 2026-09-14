from __future__ import annotations

import hashlib
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from mnemos.hive_mind import service
from mnemos.hive_mind.repository import SqliteHiveMindRepository

BOOTSTRAP = "test-fleet-bootstrap"
ADMIN = "test-distinct-admin"


def _configure(monkeypatch: pytest.MonkeyPatch, tmp_path, name: str) -> str:
    db_path = str(tmp_path / name)
    monkeypatch.setattr(service, "DB_PATH", db_path)
    monkeypatch.setattr(service, "_REPO", SqliteHiveMindRepository(db_path))
    monkeypatch.setenv("HIVE_BUS_AUTH_REQUIRED", "1")
    monkeypatch.setenv("HIVE_BUS_TOKEN", BOOTSTRAP)
    monkeypatch.setenv("HIVE_ADMIN_TOKEN", ADMIN)
    return db_path


def _auth(token: str) -> dict[str, str]:
    return {"authorization": f"Bearer {token}"}


def _register(
    client: TestClient, *, runtime: str, host: str, capabilities=None
) -> dict:
    response = client.post(
        "/v1/agents/register",
        headers=_auth(BOOTSTRAP),
        json={
            "runtime": runtime,
            "host": host,
            "provider": "ngc",
            "capabilities": capabilities or [],
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def _create_job(client: TestClient, submitter: dict, **overrides) -> str:
    body = {
        "submitter_urn": submitter["urn"],
        "kind": "code-edit",
        "description": "audit regression",
        "max_retries": 0,
    }
    body.update(overrides)
    response = client.post(
        "/v1/jobs", headers=_auth(submitter["agent_token"]), json=body
    )
    assert response.status_code == 200, response.text
    return response.json()["id"]


def test_agent_token_is_bound_to_one_urn_and_impersonation_is_rejected(
    monkeypatch, tmp_path
) -> None:
    db_path = _configure(monkeypatch, tmp_path, "agent-binding.sqlite3")
    with TestClient(service.app) as client:
        submitter = _register(client, runtime="claude-code", host="orchestrator")
        victim = _register(
            client, runtime="zeroclaw", host="victim", capabilities=["code"]
        )
        attacker = _register(
            client, runtime="zeroclaw", host="attacker", capabilities=["code"]
        )
        job_id = _create_job(
            client,
            submitter,
            required_capabilities=["code"],
            eligible_kinds=["zeroclaw"],
        )

        forged_heartbeat = client.post(
            "/v1/agents/heartbeat",
            headers=_auth(attacker["agent_token"]),
            json={"urn": victim["urn"]},
        )
        forged_claim = client.post(
            "/v1/jobs/next",
            headers=_auth(attacker["agent_token"]),
            params={"agent_urn": victim["urn"]},
        )
        victim_claim = client.post(
            "/v1/jobs/next",
            headers=_auth(victim["agent_token"]),
            params={"agent_urn": victim["urn"]},
        )
        forged_update = client.patch(
            f"/v1/jobs/{job_id}",
            headers=_auth(attacker["agent_token"]),
            json={"status": "running", "claimed_by": victim["urn"]},
        )
        forged_registration = client.post(
            "/mcp",
            headers=_auth(attacker["agent_token"]),
            json={
                "method": "agent.register",
                "params": {"runtime": "claude-code", "host": "forged"},
            },
        )

    assert forged_heartbeat.status_code == 403
    assert forged_claim.status_code == 403
    assert victim_claim.status_code == 200
    assert forged_update.status_code == 403
    assert forged_registration.status_code == 403
    with sqlite3.connect(db_path) as db:
        stored = db.execute(
            "SELECT token_hash FROM agent_credentials WHERE urn=?", (victim["urn"],)
        ).fetchone()[0]
    assert stored == hashlib.sha256(victim["agent_token"].encode()).hexdigest()
    assert stored != victim["agent_token"]


def test_job_listing_claims_and_routing_enforce_agent_and_admin_authority(
    monkeypatch, tmp_path
) -> None:
    _configure(monkeypatch, tmp_path, "job-authorization.sqlite3")
    with TestClient(service.app) as client:
        submitter = _register(client, runtime="claude-code", host="orchestrator")
        eligible = _register(
            client, runtime="zeroclaw", host="eligible", capabilities=["code"]
        )
        ineligible = _register(
            client, runtime="codex", host="ineligible", capabilities=["review"]
        )
        eligible_id = _create_job(
            client,
            submitter,
            required_capabilities=["code"],
            eligible_kinds=["zeroclaw"],
        )
        hidden_id = _create_job(
            client,
            submitter,
            description="not visible to zeroclaw",
            required_capabilities=["review"],
            eligible_kinds=["codex"],
        )

        bootstrap_list = client.get("/v1/jobs", headers=_auth(BOOTSTRAP))
        scoped = client.get("/v1/jobs", headers=_auth(eligible["agent_token"]))
        forged_filter = client.get(
            "/v1/jobs",
            headers=_auth(eligible["agent_token"]),
            params={"agent_urn": ineligible["urn"]},
        )
        wrong_claim = client.post(
            f"/v1/jobs/{eligible_id}/claim",
            headers=_auth(ineligible["agent_token"]),
            params={"by": ineligible["urn"]},
        )
        patch_claim_bypass = client.patch(
            f"/v1/jobs/{eligible_id}",
            headers=_auth(submitter["agent_token"]),
            json={"status": "claimed", "claimed_by": submitter["urn"]},
        )
        agent_route = client.patch(
            f"/v1/jobs/{eligible_id}/routing",
            headers=_auth(eligible["agent_token"]),
            json={"priority": 1},
        )
        admin_route = client.patch(
            f"/v1/jobs/{eligible_id}/routing",
            headers=_auth(ADMIN),
            json={"eligible_kinds": ["zeroclaw"]},
        )

    assert bootstrap_list.status_code == 403
    assert scoped.status_code == 200
    assert {job["id"] for job in scoped.json()["jobs"]} == {eligible_id}
    assert hidden_id not in {job["id"] for job in scoped.json()["jobs"]}
    assert forged_filter.status_code == 403
    assert wrong_claim.status_code == 403
    assert patch_claim_bypass.status_code == 409
    assert agent_route.status_code == 403
    assert admin_route.status_code == 200


def test_direct_claim_is_single_winner_atomic_compare_and_swap(
    monkeypatch, tmp_path
) -> None:
    _configure(monkeypatch, tmp_path, "claim-cas.sqlite3")
    with TestClient(service.app) as client:
        submitter = _register(client, runtime="claude-code", host="orchestrator")
        worker_a = _register(
            client, runtime="zeroclaw", host="worker-a", capabilities=["code"]
        )
        worker_b = _register(
            client, runtime="zeroclaw", host="worker-b", capabilities=["code"]
        )
        job_id = _create_job(
            client,
            submitter,
            required_capabilities=["code"],
            eligible_kinds=["zeroclaw"],
        )

        def claim(worker: dict) -> int:
            with TestClient(service.app) as race_client:
                return race_client.post(
                    f"/v1/jobs/{job_id}/claim",
                    headers=_auth(worker["agent_token"]),
                    params={"by": worker["urn"]},
                ).status_code

        with ThreadPoolExecutor(max_workers=2) as pool:
            statuses = list(pool.map(claim, (worker_a, worker_b)))

    assert sorted(statuses) == [200, 409]
    with sqlite3.connect(service.DB_PATH) as db:
        status, owner = db.execute(
            "SELECT status, claimed_by FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
    assert status == "claimed"
    assert owner in {worker_a["urn"], worker_b["urn"]}


def test_terminal_retry_is_idempotent_and_does_not_repeat_side_effects(
    monkeypatch, tmp_path
) -> None:
    db_path = _configure(monkeypatch, tmp_path, "terminal-idempotency.sqlite3")
    with TestClient(service.app) as client:
        submitter = _register(client, runtime="claude-code", host="orchestrator")
        worker = _register(
            client, runtime="zeroclaw", host="worker", capabilities=["code"]
        )
        job_id = _create_job(
            client,
            submitter,
            required_capabilities=["code"],
            eligible_kinds=["zeroclaw"],
        )
        claim_resp = client.post(
            f"/v1/jobs/{job_id}/claim",
            headers=_auth(worker["agent_token"]),
            params={"by": worker["urn"]},
        )
        assert claim_resp.status_code == 200, claim_resp.text
        # F03 (2026-09-14): the /v1/jobs/{job_id}/claim response now carries
        # the freshly minted claim_token. Echo it on every state-changing
        # call (including the idempotent duplicate replay) so the server's
        # fencing check accepts the write.
        claim_token = claim_resp.json().get("claim_token")
        payload = {
            "status": "done",
            "claimed_by": worker["urn"],
            "claim_token": claim_token,
            "tokens_in": 100,
            "tokens_out": 50,
            "result": {"exit_code": 0, "provider_used": "ngc", "model_used": "test"},
        }
        first = client.patch(
            f"/v1/jobs/{job_id}", headers=_auth(worker["agent_token"]), json=payload
        )
        duplicate = client.patch(
            f"/v1/jobs/{job_id}", headers=_auth(worker["agent_token"]), json=payload
        )

    assert first.status_code == 200
    assert duplicate.status_code == 200
    assert duplicate.json()["duplicate"] is True
    with sqlite3.connect(db_path) as db:
        done_events = db.execute(
            "SELECT COUNT(*) FROM events WHERE kind='job.done' AND json_extract(payload, '$.id')=?",
            (job_id,),
        ).fetchone()[0]
        success_count = db.execute(
            "SELECT success_count FROM worker_kind_stats WHERE urn=? AND kind='code-edit'",
            (worker["urn"],),
        ).fetchone()[0]
    assert done_events == 1
    assert success_count == 1


def test_schedule_submitter_is_derived_from_agent_credential(
    monkeypatch, tmp_path
) -> None:
    db_path = _configure(monkeypatch, tmp_path, "schedule-owner.sqlite3")
    with TestClient(service.app) as client:
        submitter = _register(client, runtime="claude-code", host="scheduler")
        created = client.post(
            "/v1/schedules",
            headers=_auth(submitter["agent_token"]),
            json={
                "name": "owned schedule",
                "interval_seconds": 60,
                "job_template": {"kind": "review", "description": "scheduled"},
            },
        )

    assert created.status_code == 200, created.text
    with sqlite3.connect(db_path) as db:
        owner, template = db.execute(
            "SELECT created_by_urn, job_template FROM scheduled_jobs WHERE id=?",
            (created.json()["id"],),
        ).fetchone()
    assert owner == submitter["urn"]
    assert submitter["urn"] in template


@pytest.mark.asyncio
async def test_reaper_uses_heartbeat_freshness_and_atomic_owner_guard(
    monkeypatch, tmp_path
) -> None:
    db_path = _configure(monkeypatch, tmp_path, "heartbeat-reaper.sqlite3")
    now = time.time()
    repo = service._REPO
    await repo.init()
    with sqlite3.connect(db_path) as db:
        db.executescript(service.SCHEMA)
        for urn, heartbeat in (("fresh-worker", now), ("stale-worker", now - 120)):
            db.execute(
                "INSERT INTO agents (urn,kind,host,session_id,started_at,last_heartbeat,status) "
                "VALUES (?, 'zeroclaw', 'host', ?, ?, ?, 'online')",
                (urn, urn, now - 600, heartbeat),
            )
            db.execute(
                "INSERT INTO jobs (id,submitter_urn,kind,status,claimed_by,claimed_at,started_at) "
                "VALUES (?, 'submitter', 'work', 'running', ?, ?, ?)",
                (f"job-{urn}", urn, now - 600, now - 600),
            )
        db.commit()

    offline, requeued = await service._reap_stale_agents_and_jobs(now)

    assert (offline, requeued) == (1, 1)
    with sqlite3.connect(db_path) as db:
        fresh = db.execute(
            "SELECT status, claimed_by FROM jobs WHERE id='job-fresh-worker'"
        ).fetchone()
        stale = db.execute(
            "SELECT status, claimed_by FROM jobs WHERE id='job-stale-worker'"
        ).fetchone()
    assert fresh == ("running", "fresh-worker")
    assert stale == ("queued", None)
