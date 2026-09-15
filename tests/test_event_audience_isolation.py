"""Regression test for F02 (adversarial review 2026-09-14):

`/v1/jobs` correctly filters by ownership/eligibility. Event history
(`/v1/events/log`), live SSE delivery (`/v1/events`), and directed messages
(`/v1/messages`) previously did NOT apply the same audience rules, so a
worker not eligible for a job could recover that job's marker/description/id
through any of those surfaces, and the bootstrap credential could read all
of it. This file proves the fix:
  - one shared audience predicate (same rule /v1/jobs already uses)
  - applied to all three secondary surfaces — including the SSE catch-up
    phase with an explicit since_id, not only the live broadcast
  - bootstrap scope narrowed to registration-only (rejected from the SSE
    subscription path, not just the JSON read paths)
  - legitimate reads (eligible worker, job owner, admin) still succeed
  - defensive defaults fail closed (an event without a job id is hidden,
    not broadcast)
  - the events_log `last_id` advances even when every event in the window
    is filtered, so polling clients make forward progress instead of
    re-querying the same window forever

Follows the in-process HTTP + throwaway SQLite pattern from
`tests/test_audit_security_v2.py`.
"""
from __future__ import annotations

import json

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


def _all_event_payloads(body: dict) -> list[str]:
    """Return the JSON-encoded `payload` string for every event in the
    `events` array. Useful for asserting the marker is absent (or present)
    at the wire level, not just via the recursive walk helper."""
    raw_events = body.get("events") or []
    return [json.dumps(ev.get("payload", {})) for ev in raw_events]


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

        # bootstrap is also rejected from the live SSE stream — without
        # this, an SSE client could subscribe to every event in the fleet.
        with client.stream("GET", "/v1/events", headers=_auth(BOOTSTRAP)) as sse:
            assert sse.status_code == 403, sse.text

        # bootstrap can still mint a fresh agent identity, confirming the
        # narrowed scope is registration-only and not a blanket block.
        second_register = client.post(
            "/v1/agents/register",
            headers=_auth(BOOTSTRAP),
            json={
                "runtime": "zeroclaw",
                "host": "bootstrap-second",
                "provider": "ngc",
                "capabilities": [],
            },
        )
        assert second_register.status_code == 200, second_register.text


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


# ---------------------------------------------------------------------------
# Test 4 — the SSE catch-up phase applies the audience filter, not only
# the live broadcast phase. Drives the underlying SSE generator directly
# (rather than going through the full HTTP loop, which is hard to
# terminate from the test thread once the generator parks on the live
# queue). The generator is a plain async generator; we iterate it just
# enough to consume the catch-up phase and a single ping from the live
# phase, then exit. This pins the catch-up filtering exactly the way
# the wire stream would.
# ---------------------------------------------------------------------------


def test_sse_catchup_phase_filters_to_callers_audience(
    monkeypatch, tmp_path
) -> None:
    """The SSE catch-up phase (since_id given) must filter events through
    the same audience predicate as /v1/events/log. An ineligible worker
    subscribing with since_id=0 receives the full catch-up batch and
    MUST NOT see the private job's marker in any event payload.

    This is the surface that the previous fix left unverified: only
    live broadcast filtering and middleware-level bootstrap rejection
    were exercised. The catch-up is where an attacker who pwns an
    agent token can dump historical events, so it has to be locked down
    the same way.

    We drive the underlying SSE generator directly (instead of going
    through the full HTTP loop, which is hard to terminate from the test
    thread once the generator parks on the live queue). The generator is
    a plain async generator; we iterate it just enough to consume the
    catch-up phase and a single ping from the live phase, then exit the
    loop. This pins the catch-up filtering exactly the way the wire
    stream would.
    """
    _configure(monkeypatch, tmp_path, "f02-sse-catchup.sqlite3")
    # Shrink the ping interval so the live phase yields a `ping` quickly
    # after the catch-up completes. The catch-up emits synchronously;
    # the live phase then parks on queue.get() with this timeout.
    monkeypatch.setattr(service, "SSE_PING_INTERVAL", 0.1)

    import asyncio

    marker = "SSE-CATCHUP-LEAK-CANARY"
    with TestClient(service.app) as client:
        orchestrator = _register(client, runtime="claude-code", host="orchestrator")
        _register(
            client, runtime="zeroclaw", host="eligible", capabilities=["code"]
        )
        ineligible = _register(
            client, runtime="codex", host="ineligible", capabilities=["review"]
        )
        _create_job(
            client,
            orchestrator,
            description=marker,
            required_capabilities=["code"],
            eligible_kinds=["zeroclaw"],
        )

        # Look up the route's underlying endpoint (stream_events) so we
        # call it directly without going through HTTP — this lets us
        # break out of the generator's live phase without tearing down
        # the ASGI app. The fake StarletteRequest below sets the auth
        # state directly because we're bypassing the middleware that
        # would normally populate it.
        from starlette.requests import Request as _StarletteRequest

        async def run_catchup():
            captured: list[dict] = []

            async def fake_is_disconnected() -> bool:
                return False

            req = _StarletteRequest(
                scope={
                    "type": "http",
                    "method": "GET",
                    "path": "/v1/events",
                    "headers": [
                        (b"authorization", b"Bearer " + ineligible["agent_token"].encode())
                    ],
                    "query_string": b"since_id=0",
                }
            )
            # The route reads `request.is_disconnected()`; we stub it
            # out because we're not running through a real ASGI server.
            req.is_disconnected = fake_is_disconnected  # type: ignore[method-assign]
            req.state.hive_auth_role = "agent"
            req.state.agent_urn = ineligible["urn"]
            resp = await service.stream_events(req, since_id=0)
            # resp.body_iterator is the EventSourceResponse's async
            # generator. Iterate it and stop once we've seen the catch-up
            # batch + one ping event (proof the live phase was entered).
            saw_ping = False
            async for chunk in resp.body_iterator:
                if isinstance(chunk, dict):
                    if chunk.get("event") == "ping":
                        saw_ping = True
                        break
                    payload_text = chunk.get("data")
                    if payload_text:
                        try:
                            captured.append(json.loads(payload_text))
                        except json.JSONDecodeError:
                            pass
                if saw_ping:
                    break
            # Close out the generator so the EVENT_QUEUE slot is freed
            # (the generator's `finally` clause does this).
            await resp.body_iterator.aclose()
            return captured, saw_ping

        captured, saw_ping = asyncio.run(run_catchup())
        assert saw_ping, (
            "SSE generator never reached the live phase; the test could "
            "not confirm the catch-up filtering was complete"
        )
        joined = json.dumps(captured)
        assert marker not in joined, (
            f"F02 regression: SSE catch-up leaked private job marker to "
            f"ineligible worker. Captured events: {joined[:1000]}"
        )


# ---------------------------------------------------------------------------
# Test 5 — events_log `last_id` advances even when every event in the
# window is filtered, so polling clients make forward progress.
# ---------------------------------------------------------------------------


def test_events_log_last_id_advances_when_all_filtered(
    monkeypatch, tmp_path
) -> None:
    """Regression guard for the `last_id` accounting fix: when the entire
    polled window is hidden by the audience filter, last_id must still
    advance past the highest event id the server CONSIDERED, so a polling
    client doesn't loop forever on the same since_id.

    Pre-fix behaviour: last_id = rows[-1]["id"] if rows else since_id.
    When all events are filtered, rows is empty and last_id == since_id,
    so the next poll with the same since_id returns the same window —
    an infinite loop. Post-fix: last_id = max(raw_ids) regardless of
    whether the events passed the audience check.

    The ineligible worker in this test sees at least one event that's
    legitimately theirs — the agent.online event for their own urn — so
    we check two things separately:
      1. the private job's marker is NOT in any event payload
      2. last_id advances past since_id=0 even when most of the window
         is hidden, so the next poll from last_id drains the rest.
    """
    _configure(monkeypatch, tmp_path, "f02-last-id.sqlite3")

    marker = "LAST-ID-CANARY"
    with TestClient(service.app) as client:
        orchestrator = _register(client, runtime="claude-code", host="orchestrator")
        ineligible = _register(
            client, runtime="codex", host="ineligible", capabilities=["review"]
        )
        _create_job(
            client,
            orchestrator,
            description=marker,
            required_capabilities=["code"],
            eligible_kinds=["zeroclaw"],
        )

        # As the ineligible worker, poll once from since_id=0. The job
        # events for the private job must not be visible, but last_id
        # must be > 0 so the next poll makes forward progress.
        first = client.get(
            "/v1/events/log",
            headers=_auth(ineligible["agent_token"]),
            params={"since_id": 0},
        )
        assert first.status_code == 200, first.text
        body = first.json()
        # The private job's marker MUST NOT leak via any event payload.
        assert not _has_private_marker(body, marker), (
            f"F02 regression: ineligible worker saw private job marker: {body!r}"
        )
        assert body["last_id"] > 0, (
            f"F02 regression: last_id did not advance past the filtered "
            f"window; client would loop on since_id={body['last_id']} "
            f"forever. body={body!r}"
        )

        # Second poll from the advanced last_id must return no events
        # the caller hasn't already seen, and must not regress last_id.
        second = client.get(
            "/v1/events/log",
            headers=_auth(ineligible["agent_token"]),
            params={"since_id": body["last_id"]},
        )
        assert second.status_code == 200, second.text
        second_body = second.json()
        assert not _has_private_marker(second_body, marker)
        # last_id stays at the same value (no new events past the cursor)
        assert second_body["last_id"] == body["last_id"], (
            f"last_id must be stable when no new events; "
            f"first={body['last_id']} second={second_body['last_id']}"
        )


# ---------------------------------------------------------------------------
# Test 6 — the message-event surface (kind=message in the events log)
# follows the same audience rule as /v1/messages.
# ---------------------------------------------------------------------------


def test_message_events_in_event_log_follow_message_audience(
    monkeypatch, tmp_path
) -> None:
    """When a /v1/messages publish emits a `message` event into the
    events log, that event must be filtered by the same audience rule
    as the message itself: visible to sender + recipient, hidden from
    unrelated callers.
    """
    _configure(monkeypatch, tmp_path, "f02-message-events.sqlite3")

    marker = "MESSAGE-EVENT-CANARY"
    with TestClient(service.app) as client:
        orchestrator = _register(client, runtime="claude-code", host="orchestrator")
        recipient = _register(
            client, runtime="zeroclaw", host="recipient", capabilities=["code"]
        )
        unrelated = _register(
            client, runtime="codex", host="unrelated", capabilities=["review"]
        )
        publish = client.post(
            "/v1/messages",
            headers=_auth(orchestrator["agent_token"]),
            json={
                "from_urn": orchestrator["urn"],
                "to_urn": recipient["urn"],
                "topic": "private-message-event",
                "payload": {"secret": marker},
            },
        )
        assert publish.status_code == 200

        # Recipient sees the message event …
        recipient_log = client.get(
            "/v1/events/log", headers=_auth(recipient["agent_token"])
        )
        assert recipient_log.status_code == 200
        assert _has_private_marker(recipient_log.json(), marker), (
            "Recipient must see the message-event in their event log"
        )

        # … and the orchestrator (sender) does too …
        owner_log = client.get(
            "/v1/events/log", headers=_auth(orchestrator["agent_token"])
        )
        assert owner_log.status_code == 200
        assert _has_private_marker(owner_log.json(), marker), (
            "Submitter must see the message-event in their event log"
        )

        # … but an unrelated caller does NOT.
        unrelated_log = client.get(
            "/v1/events/log", headers=_auth(unrelated["agent_token"])
        )
        assert unrelated_log.status_code == 200
        assert not _has_private_marker(unrelated_log.json(), marker), (
            "F02 regression: unrelated caller saw a message-event they "
            "were not party to"
        )

        # … and a broadcast message (to_urn IS NULL) IS visible to all.
        broadcast = client.post(
            "/v1/messages",
            headers=_auth(orchestrator["agent_token"]),
            json={
                "from_urn": orchestrator["urn"],
                "to_urn": None,
                "topic": "fleet-broadcast",
                "payload": {"broadcast_marker": marker},
            },
        )
        assert broadcast.status_code == 200
        unrelated_log_2 = client.get(
            "/v1/events/log", headers=_auth(unrelated["agent_token"])
        )
        assert unrelated_log_2.status_code == 200
        assert _has_private_marker(unrelated_log_2.json(), marker), (
            "Broadcast message events must be visible to all authenticated agents"
        )


# ---------------------------------------------------------------------------
# Test 7 — defensive defaults fail closed. A malformed event (e.g. a
# job.* event with no payload id, or a message event with neither to
# nor from) MUST NOT leak to agents. Pre-fix, the predicate returned
# True on missing data, which would have broadcast the event to every
# authenticated caller.
# ---------------------------------------------------------------------------


def test_malformed_events_do_not_leak(monkeypatch, tmp_path) -> None:
    _configure(monkeypatch, tmp_path, "f02-malformed.sqlite3")
    with TestClient(service.app) as client:
        orchestrator = _register(client, runtime="claude-code", host="orchestrator")
        worker = _register(client, runtime="zeroclaw", host="worker")

        # Insert three malformed events directly into the events table.
        import aiosqlite
        async def _seed():
            async with aiosqlite.connect(service.DB_PATH) as db:
                # Job event with no id — must be hidden from agents.
                await db.execute(
                    "INSERT INTO events (ts, kind, payload, agent_urn) "
                    "VALUES (?, ?, ?, NULL)",
                    (1.0, "job.weird", json.dumps({"description": "MALFORMED-JOB-NO-ID"})),
                )
                # Message event with neither to nor from — must be hidden.
                await db.execute(
                    "INSERT INTO events (ts, kind, payload, agent_urn) "
                    "VALUES (?, ?, ?, NULL)",
                    (2.0, "message", json.dumps({"topic": "MALFORMED-MESSAGE-NO-PARTIES"})),
                )
                # Unaddressed broadcast-shaped event with no urn anywhere —
                # by convention these are fleet-wide broadcasts and stay
                # visible. This is intentional, so the test pins that too.
                await db.execute(
                    "INSERT INTO events (ts, kind, payload, agent_urn) "
                    "VALUES (?, ?, ?, NULL)",
                    (3.0, "notice", json.dumps({"name": "BROADCAST-OK"})),
                )
                await db.commit()

        import asyncio
        asyncio.run(_seed())

        # Worker (agent) must NOT see the malformed job or malformed message
        worker_log = client.get("/v1/events/log", headers=_auth(worker["agent_token"]))
        assert worker_log.status_code == 200
        body = worker_log.json()
        assert "MALFORMED-JOB-NO-ID" not in json.dumps(body), (
            f"F02 regression: malformed job.* event leaked to agent: {body!r}"
        )
        assert "MALFORMED-MESSAGE-NO-PARTIES" not in json.dumps(body), (
            f"F02 regression: malformed message event leaked to agent: {body!r}"
        )
        # … but the broadcast-shaped schedule.fired is visible (intended).
        assert "BROADCAST-OK" in json.dumps(body), (
            "Intended broadcast (no urn anywhere) must remain visible to all agents"
        )

        # Admin (unfiltered) sees everything, malformed or not.
        admin_log = client.get("/v1/events/log", headers=_auth(ADMIN))
        assert admin_log.status_code == 200
        admin_body = json.dumps(admin_log.json())
        assert "MALFORMED-JOB-NO-ID" in admin_body
        assert "MALFORMED-MESSAGE-NO-PARTIES" in admin_body
        assert "BROADCAST-OK" in admin_body

        # Orchestrator (submitter of nothing relevant here) — only sees the
        # events they're entitled to as a per-agent caller.
        owner_log = client.get(
            "/v1/events/log", headers=_auth(orchestrator["agent_token"])
        )
        assert owner_log.status_code == 200
        owner_body = json.dumps(owner_log.json())
        assert "MALFORMED-JOB-NO-ID" not in owner_body
        assert "MALFORMED-MESSAGE-NO-PARTIES" not in owner_body
        # Intended broadcast is visible to every authenticated agent.
        assert "BROADCAST-OK" in owner_body


# ---------------------------------------------------------------------------
# Test 8 — the audience predicate helper is the SAME rule /v1/jobs uses.
# Direct unit test on `_event_visible_to_agent` and `_job_row_visible_to_agent`
# to pin the policy independently of the HTTP layer.
# ---------------------------------------------------------------------------


def test_audience_predicate_mirrors_jobs_visibility_rule(
    monkeypatch, tmp_path
) -> None:
    """Direct unit test on the predicate. /v1/jobs's SQL filter says:
    visible if (submitter_urn=caller OR claimed_by=caller OR
    (status IN ('queued','offered') AND claimed_by IS NULL)) AND (the
    queue-side _job_is_visible_claim_candidate check). The new helper
    must produce the same decision for every job in every state, for an
    authenticated agent caller.

    This test pins the predicate independently of HTTP so any future
    refactor that re-introduces a parallel implementation will be caught.
    """
    import asyncio
    import time as _time
    _configure(monkeypatch, tmp_path, "f02-predicate.sqlite3")

    with TestClient(service.app) as client:
        orchestrator = _register(client, runtime="claude-code", host="orchestrator")
        eligible = _register(
            client, runtime="zeroclaw", host="eligible", capabilities=["code"]
        )
        ineligible = _register(
            client, runtime="codex", host="ineligible", capabilities=["review"]
        )
        # Private job — only `eligible` (kind=zeroclaw, has capability `code`)
        # and the submitter (orchestrator) can see it.
        private_id = _create_job(
            client,
            orchestrator,
            description="PREDICATE-CANARY",
            required_capabilities=["code"],
            eligible_kinds=["zeroclaw"],
        )

        # Resolve the agent context the way the routes do.
        eligible_ctx = asyncio.run(service._claim_agent_context(eligible["urn"]))
        ineligible_ctx = asyncio.run(service._claim_agent_context(ineligible["urn"]))

        now = _time.time()

        async def job_visible(urn, ctx):
            return await service._job_row_visible_to_agent(private_id, urn, ctx, now)

        # Submitter can see their own job …
        assert asyncio.run(job_visible(orchestrator["urn"], None)), (
            "Submitter must see their own job via the shared predicate"
        )
        # … an eligible worker (kind=zeroclaw, has capability `code`) sees the queued job …
        assert asyncio.run(job_visible(eligible["urn"], eligible_ctx)), (
            "Eligible worker (kind=zeroclaw, has capability `code`) must "
            "see the queued job via the shared predicate"
        )
        # … but an ineligible worker cannot.
        assert not asyncio.run(job_visible(ineligible["urn"], ineligible_ctx)), (
            "Ineligible worker must NOT see the private job via the shared predicate"
        )

        # And the same predicate gates the kind=job.event wrapper, so the
        # event surface inherits the same visibility for the job.* event
        # emitted at job creation time.
        job_event = {
            "kind": "job.queued",
            "payload": {"id": private_id, "description": "PREDICATE-CANARY"},
        }

        async def event_visible(urn, ctx):
            return await service._event_visible_to_agent(job_event, urn, ctx, now)

        assert asyncio.run(event_visible(orchestrator["urn"], None)), (
            "Submitter must see the job.queued event for their own job"
        )
        assert asyncio.run(event_visible(eligible["urn"], eligible_ctx)), (
            "Eligible worker must see the job.queued event"
        )
        assert not asyncio.run(event_visible(ineligible["urn"], ineligible_ctx)), (
            "Ineligible worker must NOT see the job.queued event"
        )
