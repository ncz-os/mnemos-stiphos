from __future__ import annotations

import asyncio
from pathlib import Path

from mnemos.hive_mind import mcp_bridge


def test_mcp_dependency_is_constrained_to_supported_major_versions() -> None:
    text = (Path(__file__).parents[1] / "pyproject.toml").read_text()
    assert '"mcp>=1.0.0,<3.0.0"' in text


def test_graeae_tools_are_hidden_unless_upstream_is_explicitly_enabled(monkeypatch) -> None:
    monkeypatch.setattr(mcp_bridge, "GRAEAE_ENABLED", False)
    tools = asyncio.run(mcp_bridge.handle_list_tools())
    assert tools
    assert not any(tool.name.startswith("graeae.") for tool in tools)

    monkeypatch.setattr(mcp_bridge, "GRAEAE_ENABLED", True)
    enabled = asyncio.run(mcp_bridge.handle_list_tools())
    assert {tool.name for tool in enabled if tool.name.startswith("graeae.")} == {
        "graeae.consult", "graeae.muses", "graeae.modes", "graeae.get"
    }
