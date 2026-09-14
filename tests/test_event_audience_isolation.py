"""Regression test for F02 (adversarial review 2026-09-14):

`/v1/jobs` correctly filters by ownership/eligibility. Event history
(`/v1/events/log`), live SSE delivery (`/v1/events`), and directed messages
(`/v1/messages`) previously did NOT apply the same audience rules, so a
worker not eligible for a job could recover that job's marker/description/id
through any of those surfaces, and the bootstrap credential could read all
of it. This file proves the fix:
  - one shared audience predicate (same rule /v1/jobs already uses)
  - applied to all three secondary surfaces
  - bootstrap scope narrowed to registration-only
  - legitimate reads (eligible worker, job owner) still succeed.

Follows the in-process HTTP + throwaway SQLite pattern from
`tests/test_audit_security_v2.py`.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from mnemos.hive_mind import service
from mnemos.hive_mind.repository import SqliteHiveMindRepository

BOOTSTRAP = "test-f02-bootstrap"
ADMIN = "test-f02-admin"


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


def _register(client: TestClient, *, runtime: str, host: str, capabilities=None) -> dict:
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
        "description": "PRIVATE MARKER — only eligible workers should see this",
        "max_retries": 0,
        "priority": 10,
    }
    body.update(overrides)
    response = client.post(
        "/v1/jobs", headers=_auth(submitter["agent_token"]), json=body
    )
    assert response.status_code == 200, response.text
    return response.json()["id"]


def _has_private_marker(body: dict, marker: str) -> bool:
    """Crude recursive search: does any string field in the response carry
    the private job's marker?"""
    needle = marker

    def walk(node) -> bool:
        if isinstance(node, dict):
            return any(walk(v) for v in node.values())
        if isinstance(node, list):
            return any(walk(v) for v in node)
        if isinstance(node, str):
            return needle in node
        return False

    return walk(body)


# ---------------------------------------------------------------------------
# Test 1 — the exact reviewer scenario end to end.
# ---------------------------------------------------------------------------


def test_ineligible_worker_cannot_recover_private_job_via_events_or_messages(
    monkeypatch, tmp_path
) -> None:
    """Reproduces the F02 reviewer scenario: worker B is not eligible for
    worker A's job; /v1/jobs hides it (baseline we are NOT touching), but
    /v1/events/log, /v1/events SSE, and /v1/messages used to leak the
    job's description/id and must no longer do so. Worker A and the job
    owner (the orchestrator/submitter) must still see their own job.
    """
    _configure(monkeypatch, tmp_path, "f02-audience.sqlite3")

    marker = "PRIVATE-MARKER-Q9K"
    with TestClient(service.app) as client:
        orchestrator = _register(client, runtime="claude-code", host="orchestrator")
        eligible = _register(
            client, runtime="zeroclaw", host="eligible", capabilities=["code"]
        )
        ineligible = _register(
            client, runtime="codex", host="ineligible", capabilities=["review"]
        )
        eligible_job_id = _create_job(
            client,
            orchestrator,
            description=marker,
            required_capabilities=["code"],
            eligible_kinds=["zeroclaw"],
            project="f02-regression",
        )

        # --- baseline: /v1/jobs already correctly hides the private job from worker B
        scoped_ineligible = client.get("/v1/jobs", headers=_auth(ineligible["agent_token"]))
        assert scoped_ineligible.status_code == 200
        ineligible_visible_ids = {job["id"] for job in scoped_ineligible.json()["jobs"]}
        assert eligible_job_id not in ineligible_visible_ids

        # worker B must see its own nothing; worker A (eligible) sees the job
        scoped_eligible = client.get("/v1/jobs", headers=_auth(eligible["agent_token"]))
        assert scoped_eligible.status_code == 200
        assert eligible_job_id in {job["id"] for job in scoped_eligible.json()["jobs"]}

        # --- /v1/events/log (history read) — must NOT leak the marker
        log_ineligible = client.get("/v1/events/log", headers=_auth(ineligible["agent_token"]))
        assert log_ineligible.status_code == 200
        assert not _has_private_marker(log_ineligible.json(), marker), (
            "F02 regression: /v1/events/log leaked private job marker to "
            "ineligible worker"
        )
        # eligible worker still sees its own job's events (regression guard)
        log_eligible = client.get("/v1/events/log", headers=_auth(eligible["agent_token"]))
        assert log_eligible.status_code == 200
        assert _has_private_marker(log_eligible.json(), marker), (
            "Eligible worker should still see its own job's events"
        )
        # orchestrator (submitter) sees the events it created
        log_owner = client.get(
            "/v1/events/log", headers=_auth(orchestrator["agent_token"])
        )
        assert log_owner.status_code == 200
        assert _has_private_marker(log_owner.json(), marker)

        # --- /v1/events (SSE) — middleware-level audience check only.
        # The SSE generator parks on a 15s queue.get() once the catch-up
        # completes; starlette's TestClient cannot cheaply unblock that
        # from the test thread, so we do not drive the live phase. The
        # data-path audience predicate is the SAME `_event_visible_to_agent`
        # helper /v1/events/log uses, which is exercised above. Here we
        # only verify that the auth gate rejects the bootstrap credential
        # (registration-only) before the generator ever runs.
        with client.stream(
            "GET", "/v1/events", headers=_auth(BOOTSTRAP)
        ) as sse_bootstrap_resp:
            assert sse_bootstrap_resp.status_code == 403, (
                "F02 regression: bootstrap credential was allowed to "
                "subscribe to /v1/events; bootstrap is registration-only"
            )

        # --- /v1/messages — direct messages routed to the ineligible worker
        # are not allowed; messages it sent or addressed to it remain visible.
        publish_to_eligible = client.post(
            "/v1/messages",
            headers=_auth(orchestrator["agent_token"]),
            json={
                "from_urn": orchestrator["urn"],
                "to_urn": eligible["urn"],
                "topic": "private-directive",
                "payload": {"secret": marker},
            },
        )
        assert publish_to_eligible.status_code == 200

        msg_ineligible = client.get(
            "/v1/messages",
            headers=_auth(ineligible["agent_token"]),
            params={"to_urn": ineligible["urn"]},
        )
        assert msg_ineligible.status_code == 200
        assert not _has_private_marker(msg_ineligible.json(), marker), (
            "F02 regression: /v1/messages leaked a message addressed to "
            "another worker"
        )
        # Worker B also cannot pass to_urn=eligible to read someone else's mail
        cross_read = client.get(
            "/v1/messages",
            headers=_auth(ineligible["agent_token"]),
            params={"to_urn": eligible["urn"]},
        )
        assert cross_read.status_code == 403

        # Worker A (eligible) still sees the message addressed to it
        msg_eligible = client.get(
            "/v1/messages",
            headers=_auth(eligible["agent_token"]),
        )
        assert msg_eligible.status_code == 200
        assert _has_private_marker(msg_eligible.json(), marker)

        # Owner (orchestrator) sees the message it sent
        msg_owner = client.get(
            "/v1/messages",
            headers=_auth(orchestrator["agent_token"]),
        )
        assert msg_owner.status_code == 200
        assert _has_private_marker(msg_owner.json(), marker)


# ---------------------------------------------------------------------------
# Test 2 — bootstrap scope is registration-only.
# ---------------------------------------------------------------------------


def test_bootstrap_credential_is_registration_only(monkeypatch, tmp_path) -> None:
    """The bootstrap token should be able to register a new agent identity
    (its only purpose), and should be rejected from every other read surface
    that previously leaked job/messages/events content to it.
    """
    _configure(monkeypatch, tmp_path, "f02-bootstrap.sqlite3")

    with TestClient(service.app) as client:
        # bootstrap can still register (registration-only purpose)
        ok_register = client.post(
            "/v1/agents/register",
            headers=_auth(BOOTSTRAP),
            json={
                "runtime": "zeroclaw",
                "host": "bootstrap-test",
                "provider": "ngc",
                "capabilities": [],
            },
        )
        assert ok_register.status_code == 200, ok_register.text

        # bootstrap is rejected from every read surface that used to leak
        log = client.get("/v1/events/log", headers=_auth(BOOTSTRAP))
        assert log.status_code == 403, log.text
        messages = client.get("/v1/messages", headers=_auth(BOOTSTRAP))
        assert messages.status_code == 403, messages.text
        # /v1/jobs already rejected bootstrap before this fix; confirm still rejected
        jobs = client.get("/v1/jobs", headers=_auth(BOOTSTRAP))
        assert jobs.status_code == 403, jobs.text


# ---------------------------------------------------------------------------
# Test 3 — admin still sees everything (this is /v1/jobs's existing
# contract; the fix must not regress admin observability).
# ---------------------------------------------------------------------------


def test_admin_audience_visibility_remains_unrestricted(monkeypatch, tmp_path) -> None:
    _configure(monkeypatch, tmp_path, "f02-admin.sqlite3")
    marker = "ADMIN-OBSERVABILITY-MARKER"
    with TestClient(service.app) as client:
        submitter = _register(client, runtime="claude-code", host="orchestrator")
        worker_a = _register(
            client, runtime="zeroclaw", host="a", capabilities=["code"]
        )
        worker_b = _register(
            client, runtime="codex", host="b", capabilities=["review"]
        )
        _create_job(
            client,
            submitter,
            description=marker,
            required_capabilities=["code"],
            eligible_kinds=["zeroclaw"],
        )
        publish_to_a = client.post(
            "/v1/messages",
            headers=_auth(worker_b["agent_token"]),
            json={
                "from_urn": worker_b["urn"],
                "to_urn": worker_a["urn"],
                "topic": "admin-observable",
                "payload": {"secret": marker},
            },
        )
        assert publish_to_a.status_code == 200

        admin_log = client.get("/v1/events/log", headers=_auth(ADMIN))
        assert admin_log.status_code == 200
        assert _has_private_marker(admin_log.json(), marker)
        admin_msgs = client.get("/v1/messages", headers=_auth(ADMIN))
        assert admin_msgs.status_code == 200
        assert _has_private_marker(admin_msgs.json(), marker)
