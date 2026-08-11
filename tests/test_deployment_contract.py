from __future__ import annotations

from pathlib import Path
import importlib.util

import pytest


ROOT = Path(__file__).parents[1]


def test_container_persists_default_sqlite_database() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text()
    readme = (ROOT / "README.md").read_text()
    assert "AGENT_BUS_DB=/data/agents.db" in dockerfile
    assert 'VOLUME ["/data"]' in dockerfile
    assert "-v stiphos-data:/data" in readme
    assert "/srv/agent-bus/agents.db" in readme


def test_oracle_operator_migration_is_included_in_wheel_configuration() -> None:
    pyproject = (ROOT / "pyproject.toml").read_text()
    assert '"share/mnemos-stiphos/migrations/oracle" = ["migrations/oracle/*.sql"]' in pyproject


def test_dashboard_uses_bearer_auth_for_api_and_event_stream() -> None:
    dashboard = (ROOT / "mnemos/hive_mind/dashboard.html").read_text()
    assert "sessionStorage" not in dashboard
    assert "'Authorization': `Bearer ${busToken}`" in dashboard
    assert "fetch(BUS+'/v1/events', {headers: authHeaders()})" in dashboard
    assert "new EventSource" not in dashboard
    assert "innerHTML" not in dashboard
    assert "pending.split(/\\r?\\n\\r?\\n/)" in dashboard
    assert 'script-src \'self\'' in dashboard


def test_resubmission_requests_use_configured_bus_token(monkeypatch) -> None:
    path = ROOT / "deploy" / "zeroclaw-fanout" / "resubmit_non_code_jobs.py"
    spec = importlib.util.spec_from_file_location("resubmit_under_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    requests = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b'{"jobs": []}'

    monkeypatch.setattr(module, "HIVE_BUS_TOKEN", "bus-secret")
    monkeypatch.setattr(module.urllib.request, "urlopen", lambda req, timeout: (requests.append(req) or Response()))
    module.http_get_jobs("failed")
    module.http_post_job({"kind": "review"})

    assert [request.headers["Authorization"] for request in requests] == ["Bearer bus-secret", "Bearer bus-secret"]


def test_resubmission_fails_fast_without_bus_token(monkeypatch, capsys) -> None:
    path = ROOT / "deploy" / "zeroclaw-fanout" / "resubmit_non_code_jobs.py"
    spec = importlib.util.spec_from_file_location("resubmit_without_token", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "HIVE_BUS_TOKEN", "")
    monkeypatch.setattr(module, "URN", "urn:agent:human:test")
    monkeypatch.setattr(module.urllib.request, "urlopen", lambda *_args: pytest.fail("must not call bus"))

    assert module.main() == 1
    assert "HIVE_BUS_TOKEN or MNEMOS_HIVE_BUS_TOKEN" in capsys.readouterr().out
