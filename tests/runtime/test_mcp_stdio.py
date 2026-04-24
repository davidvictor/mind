from __future__ import annotations

from pathlib import Path

from mind.mcp.models import SearchMemoryRequest
from mind.mcp.server import BrainMCPServer
from mind.mcp.stdio import TOOL_SPECS, dispatch_tool_call
from tests.runtime.test_mcp_surface import _write_config


def test_stdio_tool_catalog_exposes_expected_brain_tools() -> None:
    assert "get_runtime_status" in TOOL_SPECS
    assert "search_memory" in TOOL_SPECS
    assert "run_onboard" in TOOL_SPECS
    assert "start_dream_bootstrap" in TOOL_SPECS


def test_stdio_dispatch_returns_structured_tool_payload(tmp_path: Path) -> None:
    _write_config(tmp_path)
    server = BrainMCPServer(root=tmp_path)

    result = dispatch_tool_call(
        server,
        "search_memory",
        SearchMemoryRequest(query="tools for thought").model_dump(mode="json"),
    )

    assert result["isError"] is False
    assert result["structuredContent"][0]["page_id"] == "profile"
    assert "tools for thought" in result["content"][0]["text"]
