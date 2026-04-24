from __future__ import annotations

import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from .models import (
    AuthRequest,
    ClearStaleLockRequest,
    EnqueueLinksRequest,
    GraphHealthRequest,
    GenerateSkillRequest,
    ReadSkillRequest,
    RetryQueueItemRequest,
    RunOnboardRequest,
    SearchMemoryRequest,
    SetSkillStatusRequest,
    IngestReadinessRequest,
    StartArticleRepairRequest,
    StartDreamBootstrapRequest,
    StartDreamRequest,
    StartIngestRequest,
    StartReingestRequest,
)
from .server import BrainMCPServer


SUPPORTED_PROTOCOL_VERSIONS = {
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
}
DEFAULT_PROTOCOL_VERSION = "2025-06-18"
JSONRPC_VERSION = "2.0"


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    input_schema: dict[str, Any]
    handler: Callable[[BrainMCPServer, dict[str, Any]], Any]


def _serialize(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize(item) for key, item in value.items()}
    return value


def _auth_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "auth_token": {"type": "string"},
            "session_id": {"type": "string"},
        },
        "additionalProperties": False,
    }


def _model_schema(model: type[BaseModel]) -> dict[str, Any]:
    schema = model.model_json_schema()
    schema.setdefault("type", "object")
    schema.setdefault("properties", {})
    schema.setdefault("additionalProperties", False)
    return schema


def _auth_request(arguments: dict[str, Any]) -> AuthRequest:
    return AuthRequest.model_validate(arguments)


def _validate(model: type[BaseModel], arguments: dict[str, Any]) -> BaseModel:
    return model.model_validate(arguments)


def _handle_get_runtime_status(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.get_runtime_status(_auth_request(arguments))


def _handle_list_runs(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    request = _auth_request(arguments)
    limit = arguments.get("limit", 20)
    return server.list_runs(request, limit=limit)


def _handle_get_run(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    request = _auth_request(arguments)
    return server.get_run(arguments["run_id"], request)


def _handle_search_memory(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.search_memory(_validate(SearchMemoryRequest, arguments))


def _handle_list_skills(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.list_skills(_auth_request(arguments))


def _handle_read_skill(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.read_skill(_validate(ReadSkillRequest, arguments))


def _handle_list_queue(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.list_queue(_auth_request(arguments))


def _handle_get_graph_health(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.get_graph_health(_validate(GraphHealthRequest, arguments))


def _handle_run_ingest_readiness(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.run_ingest_readiness(_validate(IngestReadinessRequest, arguments))


def _handle_enqueue_links(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.enqueue_links(_validate(EnqueueLinksRequest, arguments))


def _handle_start_ingest(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.start_ingest(_validate(StartIngestRequest, arguments))


def _handle_start_reingest(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.start_reingest(_validate(StartReingestRequest, arguments))


def _handle_start_article_repair(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.start_article_repair(_validate(StartArticleRepairRequest, arguments))


def _handle_start_dream_light(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.start_dream_light(_validate(StartDreamRequest, arguments))


def _handle_start_dream_deep(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.start_dream_deep(_validate(StartDreamRequest, arguments))


def _handle_start_dream_rem(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.start_dream_rem(_validate(StartDreamRequest, arguments))


def _handle_start_dream_weave(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.start_dream_weave(_validate(StartDreamRequest, arguments))


def _handle_start_dream_bootstrap(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.start_dream_bootstrap(_validate(StartDreamBootstrapRequest, arguments))


def _handle_generate_skill(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.generate_skill(_validate(GenerateSkillRequest, arguments))


def _handle_set_skill_status(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.set_skill_status(_validate(SetSkillStatusRequest, arguments))


def _handle_retry_queue_item(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.retry_queue_item(_validate(RetryQueueItemRequest, arguments))


def _handle_clear_stale_lock(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.clear_stale_lock(_validate(ClearStaleLockRequest, arguments))


def _handle_run_onboard(server: BrainMCPServer, arguments: dict[str, Any]) -> Any:
    return server.run_onboard(_validate(RunOnboardRequest, arguments))


def build_tool_specs() -> list[ToolSpec]:
    runs_schema = _auth_schema()
    runs_schema["properties"] = dict(runs_schema["properties"])
    runs_schema["properties"]["limit"] = {"type": "integer", "minimum": 1, "maximum": 100, "default": 20}

    run_schema = _auth_schema()
    run_schema["properties"] = dict(run_schema["properties"])
    run_schema["properties"]["run_id"] = {"type": "integer", "minimum": 1}
    run_schema["required"] = ["run_id"]

    return [
        ToolSpec("get_runtime_status", "Read Brain runtime and dream status.", _auth_schema(), _handle_get_runtime_status),
        ToolSpec("list_runs", "List recent Brain runtime runs.", runs_schema, _handle_list_runs),
        ToolSpec("get_run", "Inspect one Brain runtime run with events and errors.", run_schema, _handle_get_run),
        ToolSpec("search_memory", "Search Brain memory pages by keyword relevance.", _model_schema(SearchMemoryRequest), _handle_search_memory),
        ToolSpec("list_skills", "List Brain-local skills and their usage counters.", _auth_schema(), _handle_list_skills),
        ToolSpec("read_skill", "Read a Brain-local skill document.", _model_schema(ReadSkillRequest), _handle_read_skill),
        ToolSpec("list_queue", "List Brain queue families and pending counts.", _auth_schema(), _handle_list_queue),
        ToolSpec("get_graph_health", "Read canonical graph and advisory shadow-vector health.", _model_schema(GraphHealthRequest), _handle_get_graph_health),
        ToolSpec("run_ingest_readiness", "Run the unattended-ingest readiness gate and return its report.", _model_schema(IngestReadinessRequest), _handle_run_ingest_readiness),
        ToolSpec("enqueue_links", "Queue links for append into Brain's raw drops queue.", _model_schema(EnqueueLinksRequest), _handle_enqueue_links),
        ToolSpec("start_ingest", "Queue an ingest run for a specific source family.", _model_schema(StartIngestRequest), _handle_start_ingest),
        ToolSpec("start_reingest", "Queue a reingest replay run for a specific lane/stage window.", _model_schema(StartReingestRequest), _handle_start_reingest),
        ToolSpec("start_article_repair", "Queue the article cache repair flow (dry-run or apply).", _model_schema(StartArticleRepairRequest), _handle_start_article_repair),
        ToolSpec("start_dream_light", "Unsupported over MCP; use direct CLI operator commands for Light Dream.", _model_schema(StartDreamRequest), _handle_start_dream_light),
        ToolSpec("start_dream_deep", "Unsupported over MCP; use direct CLI operator commands for Deep Dream.", _model_schema(StartDreamRequest), _handle_start_dream_deep),
        ToolSpec("start_dream_rem", "Unsupported over MCP; use direct CLI operator commands for REM Dream.", _model_schema(StartDreamRequest), _handle_start_dream_rem),
        ToolSpec("start_dream_weave", "Unsupported over MCP; use direct CLI operator commands for Weave Dream.", _model_schema(StartDreamRequest), _handle_start_dream_weave),
        ToolSpec(
            "start_dream_bootstrap",
            "Unsupported over MCP; use direct CLI operator commands for bootstrap Dream replay.",
            _model_schema(StartDreamBootstrapRequest),
            _handle_start_dream_bootstrap,
        ),
        ToolSpec("generate_skill", "Queue Brain skill generation.", _model_schema(GenerateSkillRequest), _handle_generate_skill),
        ToolSpec("set_skill_status", "Queue a skill status update.", _model_schema(SetSkillStatusRequest), _handle_set_skill_status),
        ToolSpec("retry_queue_item", "Queue a retry for one failed or blocked run.", _model_schema(RetryQueueItemRequest), _handle_retry_queue_item),
        ToolSpec("clear_stale_lock", "Queue a stale lock clear request.", _model_schema(ClearStaleLockRequest), _handle_clear_stale_lock),
        ToolSpec("run_onboard", "Queue the compatibility onboarding path for one input file.", _model_schema(RunOnboardRequest), _handle_run_onboard),
    ]


TOOL_SPECS = {spec.name: spec for spec in build_tool_specs()}


def dispatch_tool_call(server: BrainMCPServer, name: str, arguments: dict[str, Any] | None) -> dict[str, Any]:
    spec = TOOL_SPECS.get(name)
    if spec is None:
        raise KeyError(name)

    payload = spec.handler(server, arguments or {})
    structured = _serialize(payload)
    text = json.dumps(structured, indent=2, sort_keys=True)
    return {
        "content": [{"type": "text", "text": text}],
        "structuredContent": structured,
        "isError": False,
    }


def _success(message_id: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": JSONRPC_VERSION, "id": message_id, "result": result}


def _error(message_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": JSONRPC_VERSION, "id": message_id, "error": {"code": code, "message": message}}


def _read_message(stream: Any) -> dict[str, Any] | None:
    headers: dict[str, str] = {}
    while True:
        line = stream.readline()
        if not line:
            return None
        if line in (b"\r\n", b"\n"):
            break
        header = line.decode("utf-8").strip()
        if ":" not in header:
            continue
        key, value = header.split(":", 1)
        headers[key.strip().lower()] = value.strip()

    length = int(headers.get("content-length", "0"))
    if length <= 0:
        return None
    body = stream.read(length)
    if not body:
        return None
    return json.loads(body.decode("utf-8"))


def _write_message(stream: Any, payload: dict[str, Any]) -> None:
    body = json.dumps(payload).encode("utf-8")
    stream.write(f"Content-Length: {len(body)}\r\n\r\n".encode("utf-8"))
    stream.write(body)
    stream.flush()


def serve(server: BrainMCPServer | None = None) -> int:
    brain = server or BrainMCPServer()
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    while True:
        message = _read_message(stdin)
        if message is None:
            return 0

        method = message.get("method")
        message_id = message.get("id")
        params = message.get("params") or {}

        if method == "notifications/initialized":
            continue
        if method == "ping":
            if message_id is not None:
                _write_message(stdout, _success(message_id, {}))
            continue
        if method == "initialize":
            client_version = params.get("protocolVersion")
            protocol_version = client_version if client_version in SUPPORTED_PROTOCOL_VERSIONS else DEFAULT_PROTOCOL_VERSION
            result = {
                "protocolVersion": protocol_version,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "brain", "version": "0.1.0"},
            }
            _write_message(stdout, _success(message_id, result))
            continue
        if method == "tools/list":
            tools = [
                {
                    "name": spec.name,
                    "description": spec.description,
                    "inputSchema": spec.input_schema,
                }
                for spec in TOOL_SPECS.values()
            ]
            _write_message(stdout, _success(message_id, {"tools": tools}))
            continue
        if method == "tools/call":
            try:
                result = dispatch_tool_call(brain, params["name"], params.get("arguments") or {})
                _write_message(stdout, _success(message_id, result))
            except KeyError as exc:
                _write_message(stdout, _error(message_id, -32601, f"unknown tool: {exc.args[0]}"))
            except ValidationError as exc:
                result = {
                    "content": [{"type": "text", "text": str(exc)}],
                    "isError": True,
                }
                _write_message(stdout, _success(message_id, result))
            except Exception as exc:  # pragma: no cover - defensive server boundary
                result = {
                    "content": [{"type": "text", "text": f"{type(exc).__name__}: {exc}"}],
                    "isError": True,
                }
                _write_message(stdout, _success(message_id, result))
            continue

        if message_id is not None:
            _write_message(stdout, _error(message_id, -32601, f"method not found: {method}"))


if __name__ == "__main__":
    raise SystemExit(serve())
