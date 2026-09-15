from __future__ import annotations

import importlib.util
import json
import subprocess
import urllib.error
from pathlib import Path

import pytest

_WORKER_PATH = Path(__file__).parents[1] / "deploy" / "zeroclaw-fanout" / "zeroclaw_worker.py"
_SPEC = importlib.util.spec_from_file_location("zeroclaw_worker_under_test", _WORKER_PATH)
assert _SPEC and _SPEC.loader
worker = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(worker)


def test_cost_tier_chains_do_not_exceed_service_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker, "_load_registry", lambda: None)

    tier_a = worker._build_tier_chain("A")
    tier_b = worker._build_tier_chain("B")
    tier_c = worker._build_tier_chain("C")

    assert {worker._alias_to_provider(alias) for alias in tier_a} <= {"ngc", "nvidia", "local"}
    assert not {"anthropic", "openai", "gemini"} & {
        worker._alias_to_provider(alias) for alias in tier_b
    }
    assert {"anthropic", "openai", "gemini"} <= {
        worker._alias_to_provider(alias) for alias in tier_c
    }

    monkeypatch.setitem(worker._HARDCODED_FALLBACK, "A", ["hive_openai", worker._PRIMARY_BC])
    assert worker._build_tier_chain("A") == [worker._PRIMARY_BC]


def test_terminal_update_retries_until_hive_acknowledges(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = iter([(500, {"detail": "retry"}), (200, {"ok": True})])
    calls = []

    def fake_http(method, path, body):
        calls.append((method, path, body))
        return next(responses)

    monkeypatch.setattr(worker, "_http", fake_http)
    heartbeats = []
    monkeypatch.setattr(worker, "heartbeat", lambda: heartbeats.append(True))
    monkeypatch.setattr(worker.time, "sleep", lambda _delay: None)
    monkeypatch.setattr(worker, "_running", True)

    assert worker.update_job("job-1", "done", {"exit_code": 0}, require_ack=True)
    assert len(calls) == 2
    assert heartbeats == [True]


@pytest.mark.parametrize("status_code", [400, 404, 409, 422])
def test_terminal_update_does_not_retry_permanent_conflicts(monkeypatch: pytest.MonkeyPatch, status_code: int) -> None:
    calls = []
    monkeypatch.setattr(worker, "_http", lambda *args: (calls.append(args) or (status_code, {"detail": "no"})))
    monkeypatch.setattr(worker, "_running", True)

    assert not worker.update_job("job-1", "done", {}, require_ack=True)
    assert len(calls) == 1


def test_terminal_update_stops_on_auth_rejection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker, "_http", lambda *_args: (401, None))
    monkeypatch.setattr(worker, "_running", True)
    monkeypatch.setattr(worker, "_fatal_error", False)

    assert not worker.update_job("job-1", "done", {}, require_ack=True)
    assert worker._running is False
    assert worker._fatal_error is True


@pytest.mark.parametrize("operation", ["register", "heartbeat", "dequeue"])
def test_all_bus_operations_stop_on_auth_rejection(monkeypatch: pytest.MonkeyPatch, operation: str) -> None:
    monkeypatch.setattr(worker, "_http", lambda *_args: (401, None))
    monkeypatch.setattr(worker, "_detect_subscription_pools", lambda: [])
    monkeypatch.setattr(worker, "_zeroclaw_version", lambda: "test")
    monkeypatch.setattr(worker, "_running", True)
    monkeypatch.setattr(worker, "_fatal_error", False)
    monkeypatch.setattr(worker, "_last_heartbeat", 0.0)

    if operation == "register":
        assert worker.register() == ""
    elif operation == "heartbeat":
        worker.heartbeat()
    else:
        assert worker.claim_next_job() is None

    assert worker._running is False
    assert worker._fatal_error is True


def test_terminal_update_bounds_server_error_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []
    monkeypatch.setattr(worker, "_http", lambda *args: (calls.append(args) or (500, None)))
    monkeypatch.setattr(worker, "heartbeat", lambda: None)
    monkeypatch.setattr(worker.time, "sleep", lambda _delay: None)
    monkeypatch.setattr(worker, "_running", True)
    monkeypatch.setattr(worker, "_fatal_error", False)

    assert not worker.update_job("job-1", "done", {}, require_ack=True)
    assert len(calls) == 5
    assert worker._running is False
    assert worker._fatal_error is True


def test_failed_push_raises_instead_of_returning_local_commits(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_git(_workspace, *args, **_kwargs):
        if args[0] == "status":
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[0] == "log":
            return subprocess.CompletedProcess(args, 0, "deadbeef", "")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(worker, "_git", fake_git)
    monkeypatch.setattr(
        worker.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 1, "", "rejected"),
    )

    with pytest.raises(RuntimeError, match="git push failed"):
        worker._commit_and_push("/tmp/workspace", "job-1", "code-edit", "hive/job-1")


def test_repo_preparation_failure_is_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker, "_prepare_workspace", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("auth denied")))
    monkeypatch.setattr(worker.subprocess, "Popen", lambda *_args, **_kwargs: pytest.fail("agent must not run"))

    result = worker.run_zeroclaw("[repo:https://example.invalid/repo branch:fix] change it", job_id="job-1")

    assert result["exit_code"] != 0
    assert "auth denied" in result["error"]


def test_agent_output_is_redirected_to_files_not_blocking_pipes(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeProcess:
        returncode = 0

        def wait(self, timeout=None):
            return 0

    def fake_popen(_cmd, *, cwd, stdout, stderr, text, env):
        assert stdout is not subprocess.PIPE
        assert stderr is not subprocess.PIPE
        stdout.write(b"x" * 200_000)
        stderr.write(b"diagnostic")
        stdout.flush()
        stderr.flush()
        return FakeProcess()

    monkeypatch.setattr(worker, "_load_registry", lambda: None)
    monkeypatch.setattr(worker, "_HARDCODED_FALLBACK", {"A": [worker._PRIMARY_BC], "B": [worker._PRIMARY_BC], "C": [worker._PRIMARY_BC]})
    monkeypatch.setattr(worker.subprocess, "Popen", fake_popen)

    result = worker.run_zeroclaw("say a lot", max_cost_tier="A")

    assert result["exit_code"] == 0
    assert len(result["stdout"]) == 12_000


def test_per_attempt_timeout_advances_provider_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    processes = []
    heartbeats = []

    class HungProcess:
        returncode = None
        killed = False

        def wait(self, timeout=None):
            if timeout is not None and not self.killed:
                raise subprocess.TimeoutExpired("zeroclaw", timeout)
            self.returncode = -9
            return self.returncode

        def kill(self):
            self.killed = True

    def fake_popen(*_args, **_kwargs):
        proc = HungProcess()
        processes.append(proc)
        return proc

    tick = {"value": 0.0}

    def fake_time():
        tick["value"] += 6.0
        return tick["value"]

    monkeypatch.setattr(worker, "_load_registry", lambda: None)
    aliases = [worker._PRIMARY_BC, "hive_ngc_2"]
    monkeypatch.setattr(worker, "_HARDCODED_FALLBACK", {"A": aliases, "B": aliases, "C": aliases})
    monkeypatch.setattr(worker, "PER_ATTEMPT_TIMEOUT", 50)
    monkeypatch.setattr(worker, "ZEROCLAW_TIMEOUT", 300)
    monkeypatch.setattr(worker.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(worker.time, "time", fake_time)
    monkeypatch.setattr(worker, "heartbeat", lambda: heartbeats.append(True))

    result = worker.run_zeroclaw("hang", max_cost_tier="A")

    assert len(processes) == 2
    assert all(proc.killed for proc in processes)
    assert result["providers_attempted"] == aliases
    assert len(heartbeats) >= 2


def test_knemon_reservation_is_reconciled_with_execution_usage(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["body"] = json.loads(request.data)
        captured["authorization"] = request.headers["Authorization"]
        return Response()

    monkeypatch.setattr(worker, "MNEMOS_URL", "http://mnemos.test")
    monkeypatch.setattr(worker, "MNEMOS_TOKEN", "secret")
    monkeypatch.setattr(worker.urllib.request, "urlopen", fake_urlopen)
    job = {
        "id": "job-1",
        "kind": "review",
        "routing_metadata": {
            "decision": {
                "provider": "groq",
                "model_id": "qwen",
                "auth_method": "api",
                "path_kind": "api",
                "reservation_id": "r" * 36,
            }
        },
    }

    assert worker.record_knemon_usage(
        job,
        {"provider_used": "groq", "model_used": "qwen", "tokens_in": 10, "tokens_out": 2, "duration_sec": 1.5},
        "done",
    )
    assert captured["url"] == "http://mnemos.test/v1/ledger"
    assert captured["body"]["reservation_id"] == "r" * 36
    assert captured["body"]["session_id"] == "stiphos-job:job-1"
    assert captured["body"]["outcome"] == "ok"
    assert captured["authorization"] == "Bearer secret"


def test_knemon_decision_is_the_exclusive_execution_chain() -> None:
    entries = worker._decision_execution_entries(
        {
            "provider": "gemini",
            "model_id": "gemini-2.5-flash",
            "auth_method": "api",
            "path_kind": "api",
            "fallback_chain": [
                ["groq", "qwen3-32b", "api", "api", 0.1],
                ["xai", "grok-4.1-fast", "api", "api", 0.2],
            ],
        }
    )

    assert [(entry["provider"], entry["model"]) for entry in entries] == [
        ("gemini", "gemini-2.5-flash"),
        ("groq", "qwen3-32b"),
        ("xai", "grok-4.1-fast"),
    ]
    assert not {"anthropic", "openai"} & {entry["provider"] for entry in entries}


@pytest.mark.parametrize(
    "provider",
    ["groq", "xai", "deepseek", "deepseek-direct", "nvidia", "together", "gemini"],
)
def test_every_knemon_zeroclaw_provider_has_an_execution_alias(provider: str) -> None:
    entries = worker._decision_execution_entries(
        {"provider": provider, "model_id": "authorized-model", "auth_method": "api", "path_kind": "api"}
    )
    assert entries[0]["provider"] == provider
    assert entries[0]["alias"]


def test_subscription_fallback_requires_exact_plan_and_path_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZC_DECISION_ALIAS_OPENAI_CODEX", "hive_codex_subscription")
    entries = worker._decision_execution_entries(
        {
            "provider": "groq",
            "model_id": "qwen3-32b",
            "auth_method": "api",
            "fallback_chain": [
                ["openai", "gpt-5", "subscription", "codex", 0.0],
                ["openai", "gpt-5-mini", "subscription", "codex", 0.0, "codex_pro_200"],
            ],
        }
    )

    assert [(entry["model"], entry["plan_name"]) for entry in entries] == [
        ("qwen3-32b", ""),
        ("gpt-5-mini", "codex_pro_200"),
    ]


def test_run_uses_only_decision_aliases_and_pins_exact_models(monkeypatch: pytest.MonkeyPatch) -> None:
    commands = []

    class Process:
        def __init__(self, returncode):
            self.returncode = returncode

        def wait(self, timeout=None):
            return self.returncode

    def fake_popen(cmd, *, cwd, stdout, stderr, text, env):
        commands.append(cmd)
        if len(commands) == 1:
            stderr.write(b"rate limit exceeded")
            stderr.flush()
            return Process(1)
        return Process(0)

    monkeypatch.setattr(worker.subprocess, "Popen", fake_popen)
    result = worker.run_zeroclaw(
        "review",
        routing_decision={
            "provider": "gemini",
            "model_id": "gemini-2.5-flash",
            "auth_method": "api",
            "fallback_chain": [["groq", "qwen3-32b", "api", "api", 0.1]],
        },
    )

    assert [cmd[cmd.index("--model") + 1] for cmd in commands] == ["gemini-2.5-flash", "qwen3-32b"]
    assert [cmd[cmd.index("-a") + 1] for cmd in commands] == ["hive_gemini", "hive_groq_8b"]
    assert result["provider_used"] == "groq"
    assert result["model_used"] == "qwen3-32b"


def test_reconciled_reservation_conflict_is_idempotent_success(monkeypatch: pytest.MonkeyPatch) -> None:
    conflict = urllib.error.HTTPError("http://mnemos.test/v1/ledger", 409, "conflict", {}, None)
    conflict.read = lambda: json.dumps({"detail": "burn reservation abc is 'reconciled', expected 'pending'"}).encode()
    monkeypatch.setattr(worker, "MNEMOS_TOKEN", "secret")
    monkeypatch.setattr(worker.urllib.request, "urlopen", lambda *_args, **_kwargs: (_ for _ in ()).throw(conflict))
    monkeypatch.setattr(worker, "_running", True)
    monkeypatch.setattr(worker, "_fatal_error", False)
    job = {
        "id": "job-1",
        "kind": "review",
        "routing_metadata": {
            "decision": {
                "provider": "groq",
                "model_id": "qwen",
                "auth_method": "api",
                "reservation_id": "r" * 36,
            }
        },
    }

    assert worker.record_knemon_usage(
        job,
        {
            "provider_used": "groq",
            "model_used": "qwen",
            "_stiphos_execution_complete": True,
            "execution_id": "stiphos-job:job-1",
        },
        "done",
    )
    assert worker._running is True
    assert worker._fatal_error is False


def test_execution_result_is_persisted_before_cross_service_acknowledgements(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = []
    job = {
        "id": "job-12345678",
        "claim_token": "attempt-token",
        "kind": "review",
        "description": "review",
        "routing_metadata": {},
    }
    claims = iter([job, None])

    monkeypatch.setattr(worker, "register", lambda: "urn:worker")
    monkeypatch.setattr(worker, "heartbeat", lambda: None)
    monkeypatch.setattr(worker, "claim_next_job", lambda: next(claims))
    monkeypatch.setattr(worker.time, "sleep", lambda _delay: None)
    monkeypatch.setattr(
        worker,
        "run_zeroclaw",
        lambda *_args, **_kwargs: {
            "exit_code": 0,
            "provider_used": "ngc",
            "model_used": "kimi",
        },
    )

    def fake_update(_job_id, status, result, require_ack=False, claim_token=None):
        assert claim_token == "attempt-token"
        events.append(("hive", status, dict(result)))
        if status == "done":
            worker._running = False
        return True

    monkeypatch.setattr(worker, "update_job", fake_update)
    monkeypatch.setattr(worker, "record_knemon_usage", lambda *_args: (events.append(("ledger",)) or True))
    monkeypatch.setattr(worker, "_running", True)
    monkeypatch.setattr(worker, "_fatal_error", False)

    assert worker.main() == 0
    ledger_index = events.index(("ledger",))
    durable = [event for event in events[:ledger_index] if event[0:2] == ("hive", "running")]
    assert durable[-1][2]["_stiphos_execution_complete"] is True
    assert events[-1][0:2] == ("hive", "done")
