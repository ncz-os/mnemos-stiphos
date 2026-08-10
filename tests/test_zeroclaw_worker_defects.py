from __future__ import annotations

import importlib.util
import subprocess
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
    monkeypatch.setattr(worker.time, "sleep", lambda _delay: None)
    monkeypatch.setattr(worker, "_running", True)

    assert worker.update_job("job-1", "done", {"exit_code": 0}, require_ack=True)
    assert len(calls) == 2


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
