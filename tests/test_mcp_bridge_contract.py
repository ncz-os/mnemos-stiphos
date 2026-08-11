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


def test_memory_tools_use_core_v1_routes_and_preserve_tags_as_metadata(monkeypatch) -> None:
    calls = []

    async def fake_mnemos(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return {"ok": True}

    monkeypatch.setattr(mcp_bridge, "_mnemos", fake_mnemos)
    asyncio.run(mcp_bridge.handle_call_tool("mnemos.memory_search", {"query": "needle"}))
    asyncio.run(
        mcp_bridge.handle_call_tool(
            "mnemos.memory_create", {"content": "fact", "category": "facts", "tags": ["one", "two"]}
        )
    )
    asyncio.run(mcp_bridge.handle_call_tool("mnemos.memory_get", {"id": "mem_123"}))

    assert [call[:2] for call in calls] == [
        ("POST", "/v1/memories/search"),
        ("POST", "/v1/memories"),
        ("GET", "/v1/memories/mem_123"),
    ]
    assert calls[1][2]["json"] == {
        "content": "fact",
        "category": "facts",
        "metadata": {"tags": ["one", "two"]},
    }
