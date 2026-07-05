from __future__ import annotations

from fastapi.testclient import TestClient

from mnemos.hive_mind import service
from mnemos.hive_mind.repository import SqliteHiveMindRepository


def _client(monkeypatch, tmp_path):
    db_path = str(tmp_path / "hive-auth.sqlite3")
    monkeypatch.setattr(service, "DB_PATH", db_path)
    monkeypatch.setattr(service, "_REPO", SqliteHiveMindRepository(db_path))
    return TestClient(service.app)


def test_health_remains_public_when_auth_is_required_without_token(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("HIVE_BUS_AUTH_REQUIRED", "1")

    with _client(monkeypatch, tmp_path) as client:
        response = client.get("/health")

    assert response.status_code == 200


def test_bus_fails_closed_when_auth_required_without_token(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("HIVE_BUS_AUTH_REQUIRED", "1")

    with _client(monkeypatch, tmp_path) as client:
        response = client.get("/v1/agents")

    assert response.status_code == 503
    assert "HIVE_BUS_TOKEN" in response.json()["detail"]


def test_bus_rejects_missing_or_invalid_bearer_token(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("HIVE_BUS_AUTH_REQUIRED", "1")
    monkeypatch.setenv("HIVE_BUS_TOKEN", "correct-token")

    with _client(monkeypatch, tmp_path) as client:
        missing = client.get("/v1/agents")
        invalid = client.get("/v1/agents", headers={"authorization": "Bearer wrong-token"})
        valid = client.get("/v1/agents", headers={"authorization": "Bearer correct-token"})

    assert missing.status_code == 401
    assert invalid.status_code == 401
    assert valid.status_code == 200
    assert valid.json() == {"count": 0, "agents": []}


def test_bus_accepts_namespaced_token_env(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("HIVE_BUS_AUTH_REQUIRED", "1")
    monkeypatch.setenv("MNEMOS_HIVE_BUS_TOKEN", "namespaced-token")

    with _client(monkeypatch, tmp_path) as client:
        response = client.get("/v1/agents", headers={"authorization": "Bearer namespaced-token"})

    assert response.status_code == 200

