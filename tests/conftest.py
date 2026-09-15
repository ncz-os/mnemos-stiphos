"""Default auth fixture; integration dependencies use explicit test skips."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def disable_hive_bus_auth_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIVE_BUS_AUTH_REQUIRED", "0")
    monkeypatch.delenv("HIVE_BUS_TOKEN", raising=False)
    monkeypatch.delenv("MNEMOS_HIVE_BUS_TOKEN", raising=False)

