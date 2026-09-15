import asyncio
import importlib.util
import json
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

import aiosqlite
import pytest
from fastapi.testclient import TestClient
from mnemos.hive_mind import service
from mnemos.hive_mind.repository import SqliteHiveMindRepository


@pytest.fixture
def client(monkeypatch, tmp_path):
    path = str(tmp_path / "bus.db")
    monkeypatch.setattr(service, "DB_PATH", path)
    monkeypatch.setattr(service, "_REPO", SqliteHiveMindRepository(path))
    monkeypatch.setenv("HIVE_BUS_AUTH_REQUIRED", "1")
    monkeypatch.setenv("HIVE_BUS_TOKEN", "bootstrap")
    monkeypatch.setenv("HIVE_ADMIN_TOKEN", "admin")
    with TestClient(service.app) as client:
        yield client


def register(client, runtime):
    result = client.post(
        "/v1/agents/register",
        headers={"Authorization": "Bearer bootstrap"},
        json={"runtime": runtime, "host": "test", "provider": "local"},
    )
    assert result.status_code == 200
    agent = result.json()
    return agent, {"Authorization": "Bearer " + agent["agent_token"]}


def schedule(client):
    owner, auth = register(client, "claude-code")
    result = client.post(
        "/v1/schedules",
        headers=auth,
        json={
            "name": "dispatch",
            "interval_seconds": 60,
            "job_template": {
                "submitter_urn": owner["urn"],
                "kind": "review",
                "description": "test",
            },
        },
    )
    assert result.status_code == 200, result.text
    sid = result.json()["id"]
    with sqlite3.connect(service.DB_PATH) as db:
        db.execute(
            "UPDATE scheduled_jobs SET next_fire_at=? WHERE id=?",
            (time.time() - 1, sid),
        )
    return sid


def test_real_dispatch_concurrent_contenders_create_one_job(client):
    sid = schedule(client)

    async def run():
        await asyncio.wait_for(
            asyncio.gather(
                *[service._fire_due_schedules(time.time()) for _ in range(4)]
            ),
            5,
        )

    asyncio.run(run())
    with sqlite3.connect(service.DB_PATH) as db:
        jobs = db.execute("SELECT id FROM jobs").fetchall()
        assert len(jobs) == 1
        assert (
            db.execute(
                "SELECT job_id FROM schedule_fires WHERE schedule_id=?", (sid,)
            ).fetchall()
            == jobs
        )
        assert db.execute(
            "SELECT fire_count FROM scheduled_jobs WHERE id=?", (sid,)
        ).fetchone() == (1,)


@pytest.mark.parametrize("after_insert", [False, True])
def test_dispatch_recovers_failed_or_unlinked_intent(client, monkeypatch, after_insert):
    sid = schedule(client)
    create = service._create_job

    async def fail(*args, **kwargs):
        if after_insert:
            await create(*args, **kwargs)
        raise RuntimeError("simulated interruption")

    monkeypatch.setattr(service, "_create_job", fail)
    asyncio.run(service._fire_due_schedules(time.time()))
    with sqlite3.connect(service.DB_PATH) as db:
        assert db.execute(
            "SELECT job_id FROM schedule_fires WHERE schedule_id=?", (sid,)
        ).fetchone() == (None,)
    monkeypatch.setattr(service, "_create_job", create)
    asyncio.run(service._fire_due_schedules(time.time()))
    with sqlite3.connect(service.DB_PATH) as db:
        assert db.execute("SELECT COUNT(*) FROM jobs").fetchone() == (1,)
        assert db.execute(
            "SELECT job_id IS NOT NULL FROM schedule_fires"
        ).fetchone() == (1,)


def test_bundled_worker_echoes_attempt_and_renews_lease(client):
    owner, auth = register(client, "claude-code")
    worker_agent, worker_auth = register(client, "zeroclaw")
    client.post(
        "/v1/jobs", headers=auth, json={"submitter_urn": owner["urn"], "kind": "review"}
    )
    claim = client.post("/v1/jobs/next", headers=worker_auth).json()
    spec = importlib.util.spec_from_file_location(
        "test_worker",
        Path(__file__).parents[1] / "deploy/zeroclaw-fanout/zeroclaw_worker.py",
    )
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    worker._urn = worker_agent["urn"]
    worker._running = True

    def http(method, path, body):
        response = client.request(method, path, headers=worker_auth, json=body)
        return response.status_code, response.json()

    worker._http = http
    assert worker.update_job(
        claim["id"], "running", {}, require_ack=True, claim_token=claim["claim_token"]
    )
    assert worker.renew_job_lease(claim["id"], claim["claim_token"])
    assert not worker.update_job(
        claim["id"], "running", {}, require_ack=True, claim_token="stale"
    )
    with sqlite3.connect(service.DB_PATH) as db:
        assert db.execute(
            "SELECT attempt_lease_renewals FROM jobs WHERE id=?", (claim["id"],)
        ).fetchone() == (1,)


def test_mcp_message_reader_keeps_authenticated_audience(client):
    sender, auth = register(client, "claude-code")
    recipient, _ = register(client, "human")
    _, unrelated = register(client, "zeroclaw")
    client.post(
        "/v1/messages",
        headers=auth,
        json={
            "from_urn": sender["urn"],
            "to_urn": recipient["urn"],
            "topic": "private",
            "payload": {"marker": "private-content"},
        },
    )
    response = client.post(
        "/mcp", headers=unrelated, json={"method": "message.list", "params": {}}
    )
    assert response.status_code == 200
    assert "private-content" not in response.text


def test_sse_overflow_and_external_commit_replay_in_order(client, monkeypatch):
    monkeypatch.setattr(service, "SSE_EVENT_QUEUE_SIZE", 1)

    class Request:
        state = SimpleNamespace(hive_auth_role="admin")

        @property
        def headers(self):
            return getattr(self, "_headers", {})

        @headers.setter
        def headers(self, value):
            self._headers = value

        async def is_disconnected(self):
            return False

    async def run():
        response = await service.stream_events(Request())
        async with aiosqlite.connect(service.DB_PATH) as db:
            for n in range(3):
                await service.emit_event(db, "notice", {"n": n})
            # Simulate a different process: persisted commit, no local notification.
            await db.execute(
                "INSERT INTO events(ts,kind,payload) VALUES(?,?,?)",
                (time.time(), "notice", json.dumps({"n": 3})),
            )
            await db.commit()
        received = [
            await asyncio.wait_for(anext(response.body_iterator), 2) for _ in range(4)
        ]
        assert [
            json.loads(event["data"])["payload"]["n"] for event in received
        ] == list(range(4))
        ids = [int(event["id"]) for event in received]
        assert ids == sorted(set(ids))
        await response.body_iterator.aclose()
        request = Request()
        request.headers = {"last-event-id": str(ids[1])}
        resumed = await service.stream_events(request)
        assert int((await anext(resumed.body_iterator))["id"]) == ids[2]
        await resumed.body_iterator.aclose()

    asyncio.run(run())


def test_schedule_metadata_follows_private_job_audience(client):
    owner, auth = register(client, "claude-code")
    _worker, worker_auth = register(client, "zeroclaw")
    response = client.post(
        "/v1/schedules",
        headers=auth,
        json={
            "name": "PRIVATE-SCHEDULE-CANARY",
            "interval_seconds": 60,
            "job_template": {
                "submitter_urn": owner["urn"],
                "kind": "review",
                "eligible_kinds": ["human"],
            },
        },
    )
    assert response.status_code == 200, response.text
    with sqlite3.connect(service.DB_PATH) as db:
        db.execute("UPDATE scheduled_jobs SET next_fire_at=?", (time.time() - 1,))
    asyncio.run(service._fire_due_schedules(time.time()))
    assert (
        "PRIVATE-SCHEDULE-CANARY"
        not in client.get("/v1/events/log", headers=worker_auth).text
    )
    assert "PRIVATE-SCHEDULE-CANARY" in client.get("/v1/events/log", headers=auth).text


def test_live_sse_refreshes_capabilities_and_honors_token_revocation(client):
    from starlette.requests import Request

    owner, auth = register(client, "claude-code")
    worker, worker_auth = register(client, "zeroclaw")
    with sqlite3.connect(service.DB_PATH) as db:
        db.execute(
            "UPDATE agents SET capabilities=? WHERE urn=?",
            (json.dumps(["private-code"]), worker["urn"]),
        )
    job = client.post(
        "/v1/jobs",
        headers=auth,
        json={
            "submitter_urn": owner["urn"],
            "kind": "review",
            "required_capabilities": ["private-code"],
        },
    ).json()

    async def run():
        request = Request(
            {
                "type": "http",
                "headers": [(b"authorization", worker_auth["Authorization"].encode())],
            }
        )
        request.state.hive_auth_role = "agent"
        request.state.agent_urn = worker["urn"]

        async def connected():
            return False

        request.is_disconnected = connected
        response = await service.stream_events(request)
        async with aiosqlite.connect(service.DB_PATH) as db:
            await db.execute(
                "UPDATE agents SET capabilities=? WHERE urn=?", ("[]", worker["urn"])
            )
            await db.commit()
            await service.emit_event(
                db, "job.created", {"id": job["id"], "marker": "PRIVATE"}
            )
            await service.emit_event(db, "notice", {"marker": "PUBLIC"})
        event = await asyncio.wait_for(anext(response.body_iterator), 2)
        assert event["event"] == "notice"
        assert "PRIVATE" not in event["data"]
        async with aiosqlite.connect(service.DB_PATH) as db:
            await db.execute(
                "DELETE FROM agent_credentials WHERE urn=?", (worker["urn"],)
            )
            await db.commit()
            await service.emit_event(db, "notice", {"marker": "REVOKED"})
        with pytest.raises(StopAsyncIteration):
            await anext(response.body_iterator)
        await response.body_iterator.aclose()

    asyncio.run(run())
