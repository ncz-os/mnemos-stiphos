from __future__ import annotations

import sqlite3
import sys
import types
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

try:
    from mnemos.core.config import agent_bus_db_env as _agent_bus_db_env
except ModuleNotFoundError:
    _stubbed_core = True
    core_module = types.ModuleType("mnemos.core")
    config_module = types.ModuleType("mnemos.core.config")
    config_module.agent_bus_db_env = lambda: str(Path("/tmp") / "mnemos-hive-test.sqlite3")
    sys.modules["mnemos.core"] = core_module
    sys.modules["mnemos.core.config"] = config_module
else:
    _stubbed_core = False

from mnemos.hive_mind import service
from mnemos.hive_mind.repository import SqliteHiveMindRepository

if _stubbed_core:
    sys.modules.pop("mnemos.core.config", None)
    sys.modules.pop("mnemos.core", None)


def _configure_service(monkeypatch, tmp_path, name: str) -> str:
    db_path = str(tmp_path / name)
    monkeypatch.setattr(service, "DB_PATH", db_path)
    monkeypatch.setattr(service, "_REPO", SqliteHiveMindRepository(db_path))
    return db_path


def _register_and_create(client: TestClient, *, max_retries: int = 0, max_cost_tier: str = "A"):
    submitter = client.post("/v1/agents/register", json={"runtime": "claude-code", "host": "studio"})
    worker = client.post(
        "/v1/agents/register",
        json={"runtime": "zeroclaw", "host": "worker", "provider": "ngc", "capabilities": ["code-edit"]},
    )
    created = client.post(
        "/v1/jobs",
        json={
            "submitter_urn": submitter.json()["urn"],
            "kind": "code-edit",
            "description": "fix it",
            "max_retries": max_retries,
            "max_cost_tier": max_cost_tier,
        },
    )
    claimed = client.post(f"/v1/jobs/next?agent_urn={worker.json()['urn']}")
    assert claimed.status_code == 200, claimed.text
    return worker.json()["urn"], created.json()["id"]


def test_retryable_failure_is_counted_for_original_worker(monkeypatch, tmp_path) -> None:
    db_path = _configure_service(monkeypatch, tmp_path, "retry-stats.sqlite3")
    with TestClient(service.app) as client:
        worker_urn, job_id = _register_and_create(client, max_retries=1)
        response = client.patch(
            f"/v1/jobs/{job_id}",
            json={
                "status": "failed",
                "claimed_by": worker_urn,
                "result": {"exit_code": 1, "provider_used": "ngc", "model_used": "unknown"},
            },
        )
        assert response.status_code == 200, response.text

    with sqlite3.connect(db_path) as db:
        row = db.execute(
            "SELECT fail_count FROM worker_kind_stats WHERE urn=? AND kind='code-edit'", (worker_urn,)
        ).fetchone()
    assert row == (1,)


def test_reported_provider_is_revalidated_and_used_for_accounting(monkeypatch, tmp_path) -> None:
    db_path = _configure_service(monkeypatch, tmp_path, "provider-accounting.sqlite3")
    with TestClient(service.app) as client:
        worker_urn, job_id = _register_and_create(client, max_retries=0, max_cost_tier="A")
        response = client.patch(
            f"/v1/jobs/{job_id}",
            json={
                "status": "done",
                "claimed_by": worker_urn,
                "tokens_in": 100,
                "tokens_out": 50,
                "result": {"exit_code": 0, "provider_used": "openai", "model_used": "gpt-5"},
            },
        )
        assert response.status_code == 200, response.text

    with sqlite3.connect(db_path) as db:
        row = db.execute(
            "SELECT status, claimed_provider, claimed_cost_tier, result FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
    assert row[:3] == ("failed", "openai", "C")
    assert "cost_tier_violation" in row[3]


def test_health_fails_when_reaper_is_not_running(monkeypatch, tmp_path) -> None:
    _configure_service(monkeypatch, tmp_path, "health.sqlite3")

    class StoppedTask:
        def done(self):
            return True

    with TestClient(service.app) as client:
        service.app.state.reaper_task = StoppedTask()
        response = client.get("/health")

    assert response.status_code == 503
    assert "reaper task is not running" in response.text


def test_stale_worker_cannot_overwrite_requeued_job(monkeypatch, tmp_path) -> None:
    db_path = _configure_service(monkeypatch, tmp_path, "ownership.sqlite3")
    with TestClient(service.app) as client:
        worker_urn, job_id = _register_and_create(client)
        with sqlite3.connect(db_path) as db:
            db.execute(
                "UPDATE jobs SET status='queued', claimed_by=NULL, claimed_at=NULL WHERE id=?",
                (job_id,),
            )
        response = client.patch(
            f"/v1/jobs/{job_id}",
            json={
                "status": "done",
                "claimed_by": worker_urn,
                "result": {"exit_code": 0, "provider_used": "ngc", "model_used": "unknown"},
            },
        )

    assert response.status_code == 409
    with sqlite3.connect(db_path) as db:
        assert db.execute("SELECT status, claimed_by FROM jobs WHERE id=?", (job_id,)).fetchone() == ("queued", None)


def test_dashboard_loads_packaged_asset(monkeypatch, tmp_path) -> None:
    _configure_service(monkeypatch, tmp_path, "dashboard.sqlite3")
    with TestClient(service.app) as client:
        response = client.get("/")
        script = client.get("/dashboard.js")
    assert response.status_code == 200
    assert "<!doctype html>" in response.text.lower()
    assert "script-src 'self'" in response.headers["content-security-policy"]
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]
    assert script.status_code == 200
    assert script.headers["content-type"].startswith("application/javascript")
    assert "window.prompt" in script.text


def test_service_refuses_to_silently_ignore_oracle_backend_request(monkeypatch, tmp_path) -> None:
    _configure_service(monkeypatch, tmp_path, "must-not-create.sqlite3")
    monkeypatch.setenv("HIVE_REPOSITORY_BACKEND", "oracle")
    with pytest.raises(RuntimeError, match="supports only SQLite"):
        with TestClient(service.app):
            pass


def test_cache_equivalence_includes_context(monkeypatch, tmp_path) -> None:
    _configure_service(monkeypatch, tmp_path, "cache-context.sqlite3")
    with TestClient(service.app) as client:
        submitter = client.post("/v1/agents/register", json={"runtime": "claude-code", "host": "studio"}).json()
        worker = client.post(
            "/v1/agents/register", json={"runtime": "zeroclaw", "host": "worker", "provider": "ngc"}
        ).json()
        first = client.post(
            "/v1/jobs",
            json={"submitter_urn": submitter["urn"], "kind": "review", "description": "same", "mnemos_refs": ["mem_a"]},
        )
        job_id = first.json()["id"]
        assert client.post(f"/v1/jobs/next?agent_urn={worker['urn']}").status_code == 200
        assert client.patch(
            f"/v1/jobs/{job_id}",
            json={
                "status": "done",
                "claimed_by": worker["urn"],
                "result": {"exit_code": 0, "provider_used": "ngc", "model_used": "unknown"},
            },
        ).status_code == 200
        second = client.post(
            "/v1/jobs",
            json={"submitter_urn": submitter["urn"], "kind": "review", "description": "same", "mnemos_refs": ["mem_b"]},
        )

    assert second.status_code == 200
    assert second.json().get("cache_hit") is not True


def test_worker_submitter_is_rejected_before_cache_lookup(monkeypatch, tmp_path) -> None:
    db_path = _configure_service(monkeypatch, tmp_path, "cache-auth.sqlite3")
    with TestClient(service.app) as client:
        worker = client.post(
            "/v1/agents/register", json={"runtime": "zeroclaw", "host": "worker", "provider": "ngc"}
        ).json()
        key = service.cache_key_for("review", "same", "A", None, submitter_urn=worker["urn"])
        with sqlite3.connect(db_path) as db:
            db.execute(
                "INSERT INTO hive_cache (cache_key,result_json,source_job_id,cached_at) VALUES (?,?,?,?)",
                (key, '{"answer":"cached"}', "source", __import__("time").time()),
            )
        response = client.post(
            "/v1/jobs", json={"submitter_urn": worker["urn"], "kind": "review", "description": "same"}
        )

    assert response.status_code == 403


def test_cache_equivalence_distinguishes_explicit_default_cost_cap() -> None:
    omitted = service.cache_key_for("review", "same", "A", None, max_cost_tier_explicit=False)
    explicit = service.cache_key_for("review", "same", "A", None, max_cost_tier_explicit=True)
    assert omitted != explicit


def test_cache_is_off_by_default_even_for_identical_successful_jobs(monkeypatch, tmp_path) -> None:
    _configure_service(monkeypatch, tmp_path, "cache-default-off.sqlite3")
    with TestClient(service.app) as client:
        submitter = client.post("/v1/agents/register", json={"runtime": "claude-code", "host": "studio"}).json()
        worker = client.post(
            "/v1/agents/register", json={"runtime": "zeroclaw", "host": "worker", "provider": "ngc"}
        ).json()
        body = {"submitter_urn": submitter["urn"], "kind": "review", "description": "same pure-looking text"}
        first = client.post("/v1/jobs", json=body).json()
        assert client.post(f"/v1/jobs/next?agent_urn={worker['urn']}").status_code == 200
        assert client.patch(
            f"/v1/jobs/{first['id']}",
            json={
                "status": "done",
                "claimed_by": worker["urn"],
                "result": {"exit_code": 0, "provider_used": "ngc", "model_used": "unknown"},
            },
        ).status_code == 200
        second = client.post("/v1/jobs", json=body)

    assert second.status_code == 200
    assert second.json().get("cache_hit") is not True


def test_cacheable_repository_job_requires_immutable_identity(monkeypatch, tmp_path) -> None:
    _configure_service(monkeypatch, tmp_path, "cache-repo-identity.sqlite3")
    with TestClient(service.app) as client:
        response = client.post(
            "/v1/jobs",
            json={
                "submitter_urn": "human",
                "kind": "code-edit",
                "description": "[repo:https://example.test/repo base:main] edit",
                "cacheable": True,
                "pure": True,
                "idempotent": True,
            },
        )
    assert response.status_code == 422
    assert "immutable commit hash" in response.text


def test_cache_opt_in_requires_explicit_pure_and_idempotent(monkeypatch, tmp_path) -> None:
    _configure_service(monkeypatch, tmp_path, "cache-declarations.sqlite3")
    with TestClient(service.app) as client:
        response = client.post(
            "/v1/jobs",
            json={"submitter_urn": "human", "kind": "review", "description": "same", "cacheable": True},
        )
    assert response.status_code == 422
    assert "pure=true and idempotent=true" in response.text


def test_explicit_pure_idempotent_job_can_use_cache(monkeypatch, tmp_path) -> None:
    _configure_service(monkeypatch, tmp_path, "cache-explicit.sqlite3")
    with TestClient(service.app) as client:
        submitter = client.post("/v1/agents/register", json={"runtime": "claude-code", "host": "studio"}).json()
        worker = client.post(
            "/v1/agents/register", json={"runtime": "zeroclaw", "host": "worker", "provider": "ngc"}
        ).json()
        body = {
            "submitter_urn": submitter["urn"],
            "kind": "pure-summary",
            "description": "summarize immutable input",
            "cacheable": True,
            "pure": True,
            "idempotent": True,
        }
        first = client.post("/v1/jobs", json=body).json()
        assert client.post(f"/v1/jobs/next?agent_urn={worker['urn']}").status_code == 200
        assert client.patch(
            f"/v1/jobs/{first['id']}",
            json={
                "status": "done",
                "claimed_by": worker["urn"],
                "result": {"exit_code": 0, "provider_used": "ngc", "model_used": "unknown"},
            },
        ).status_code == 200
        second = client.post("/v1/jobs", json=body)

    assert second.status_code == 200
    assert second.json()["cache_hit"] is True


def test_requeue_clears_claim_and_terminal_state(monkeypatch, tmp_path) -> None:
    db_path = _configure_service(monkeypatch, tmp_path, "explicit-requeue.sqlite3")
    with TestClient(service.app) as client:
        worker_urn, job_id = _register_and_create(client)
        with sqlite3.connect(db_path) as db:
            db.execute(
                "UPDATE jobs SET result='{}', ended_at=1, tokens_in=2, estimated_cost_usd=3 WHERE id=?",
                (job_id,),
            )
        response = client.patch(
            f"/v1/jobs/{job_id}",
            json={"status": "queued", "claimed_by": worker_urn, "result": {"obsolete": True}},
        )
    assert response.status_code == 200, response.text
    with sqlite3.connect(db_path) as db:
        row = db.execute(
            "SELECT status,claimed_by,claimed_at,result,ended_at,tokens_in,estimated_cost_usd FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()
    assert row == ("queued", None, None, None, None, None, None)


def test_terminal_result_identity_is_required(monkeypatch, tmp_path) -> None:
    _configure_service(monkeypatch, tmp_path, "identity-required.sqlite3")
    with TestClient(service.app) as client:
        worker_urn, job_id = _register_and_create(client)
        response = client.patch(
            f"/v1/jobs/{job_id}",
            json={"status": "done", "claimed_by": worker_urn, "result": {"exit_code": 0}},
        )
    assert response.status_code == 422
    assert "provider_used and model_used" in response.text


def test_terminal_identity_must_match_knemon_decision(monkeypatch, tmp_path) -> None:
    _configure_service(monkeypatch, tmp_path, "identity-authorized.sqlite3")
    with TestClient(service.app) as client:
        worker_urn, job_id = _register_and_create(client, max_cost_tier="C")
        with sqlite3.connect(service.DB_PATH) as db:
            db.execute(
                "UPDATE jobs SET routing_metadata=? WHERE id=?",
                ('{"decision":{"provider":"groq","model_id":"qwen","fallback_chain":[]}}', job_id),
            )
        response = client.patch(
            f"/v1/jobs/{job_id}",
            json={
                "status": "done",
                "claimed_by": worker_urn,
                "result": {"exit_code": 0, "provider_used": "openai", "model_used": "gpt-5"},
            },
        )
    assert response.status_code == 409
    assert "outside the authorized KNEMON decision" in response.text
