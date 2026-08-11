from __future__ import annotations

from pathlib import Path


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
    assert "sessionStorage" in dashboard
    assert "'Authorization': `Bearer ${busToken}`" in dashboard
    assert "fetch(BUS+'/v1/events', {headers: authHeaders()})" in dashboard
    assert "new EventSource" not in dashboard
