"""
GRAEAE Hive Mind — MCP SSE bridge.

Exposes Hive (HYDRA :5005) + MNEMOS REST (PYTHIA :5002) as a single MCP server
on HYDRA :5006/sse. Speaks proper SSE MCP transport for opencode/Claude-Desktop.

Endpoint matrix:
  /sse  : SSE GET — server→client event stream
  /messages : POST — client→server JSON-RPC

Tools:
  hive.agent_list           -> GET /v1/agents on HIVE
  hive.agent_register       -> POST /v1/agents/register
  hive.job_create           -> POST /v1/jobs
  hive.job_next             -> POST /v1/jobs/next?agent_urn=
  hive.job_list             -> GET /v1/jobs
  hive.job_update           -> PATCH /v1/jobs/{id}
  hive.message_publish      -> POST /v1/messages
  hive.message_list         -> GET /v1/messages
  mnemos.memory_search      -> POST /v1/memories/search on PYTHIA
  mnemos.memory_create      -> POST /v1/memories
  mnemos.memory_get         -> GET /v1/memories/{id}
"""

from __future__ import annotations

import secrets
import json
import os
from typing import Any

import httpx
import uvicorn
from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
from mcp.server.sse import SseServerTransport
from mcp.types import Tool, TextContent, Resource, Prompt
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import PlainTextResponse
from starlette.routing import Mount, Route

from mnemos.hive_mind.config import mcp_hive_url_env, mcp_mnemos_token_env, mcp_mnemos_url_env, mcp_port_env

HIVE_URL = mcp_hive_url_env()
# C1: bus transport auth bearer token (env-sourced). Injected only on bus calls
# (_hive), kept separate from the MNEMOS token used by _mnemos.
HIVE_BUS_TOKEN = os.environ.get("HIVE_BUS_TOKEN") or os.environ.get("MNEMOS_HIVE_BUS_TOKEN", "")
MNEMOS_URL = mcp_mnemos_url_env()
MNEMOS_TOKEN = mcp_mnemos_token_env()
GRAEAE_ENABLED = os.environ.get("MCP_GRAEAE_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}

_MCP_DECORATOR_API = hasattr(Server, "list_tools")
server = Server("graeae-hive-mind") if _MCP_DECORATOR_API else None
client = httpx.AsyncClient(timeout=15.0)


def _register(name: str):
    """Register a v1 handler or defer it for the v2 constructor callbacks."""
    def decorator(fn):
        if _MCP_DECORATOR_API:
            getattr(server, name)()(fn)
        return fn
    return decorator


async def _hive(method: str, path: str, **kw) -> dict:
    if HIVE_BUS_TOKEN:
        headers = dict(kw.pop("headers", {}) or {})
        headers.setdefault("Authorization", f"Bearer {HIVE_BUS_TOKEN}")
        kw["headers"] = headers
    r = await client.request(method, f"{HIVE_URL}{path}", **kw)
    r.raise_for_status()
    if r.status_code == 204:
        return {"_status": 204, "_note": "no eligible job"}
    return r.json()


async def _mnemos(method: str, path: str, **kw) -> dict:
    headers = kw.pop("headers", {})
    headers["Authorization"] = f"Bearer {MNEMOS_TOKEN}"
    r = await client.request(method, f"{MNEMOS_URL}{path}", headers=headers, **kw)
    r.raise_for_status()
    return r.json()


# ---------- tool definitions ----------


@_register("list_tools")
async def handle_list_tools() -> list[Tool]:
    tools = [
        Tool(
            name="hive.agent_list",
            description="List registered agents in the GRAEAE Hive Mind.",
            inputSchema={"type": "object", "properties": {"kind": {"type": "string"}, "status": {"type": "string"}}},
        ),
        Tool(
            name="hive.agent_register",
            description="Register this session as an agent in the Hive. Returns urn + session_id.",
            inputSchema={
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "description": "claude/goose/opencode/codex/zeroclaw/openclaw/hermes/ic-engine/mnemos/human",
                    },
                    "host": {"type": "string"},
                    "capabilities": {"type": "array", "items": {"type": "string"}},
                    "version": {"type": "string"},
                    "metadata": {"type": "object"},
                },
                "required": ["kind", "host"],
            },
        ),
        Tool(
            name="hive.job_create",
            description="Submit work into the Hive Mind triage queue. Eligible agents self-claim.",
            inputSchema={
                "type": "object",
                "properties": {
                    "submitter_urn": {"type": "string"},
                    "kind": {"type": "string"},
                    "description": {"type": "string"},
                    "priority": {"type": "integer", "default": 0},
                    "required_capabilities": {"type": "array", "items": {"type": "string"}},
                    "eligible_kinds": {"type": "array", "items": {"type": "string"}},
                    "deadline": {"type": "number"},
                    "parent_job_id": {"type": "string"},
                },
                "required": ["submitter_urn", "kind"],
            },
        ),
        Tool(
            name="hive.job_next",
            description="Atomic dequeue: claim highest-priority eligible job for this agent. Returns job or {204: no work}.",
            inputSchema={"type": "object", "properties": {"agent_urn": {"type": "string"}}, "required": ["agent_urn"]},
        ),
        Tool(
            name="hive.job_list",
            description="List jobs in the Hive. Filter by status/agent/since.",
            inputSchema={
                "type": "object",
                "properties": {
                    "status": {"type": "string"},
                    "agent_urn": {"type": "string"},
                    "since": {"type": "number"},
                    "limit": {"type": "integer", "default": 100},
                },
            },
        ),
        Tool(
            name="hive.job_update",
            description=(
                "Update job status/result. Requeueing atomically clears claim and terminal state. "
                "Done/failed results require provider_used and model_used."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "status": {
                        "type": "string",
                        "enum": ["queued", "claimed", "running", "done", "failed", "cancelled"],
                    },
                    "result": {
                        "type": "object",
                        "properties": {
                            "provider_used": {"type": "string"},
                            "model_used": {"type": "string"},
                        },
                    },
                    "claimed_by": {"type": "string"},
                },
                "required": ["id", "status"],
            },
        ),
        Tool(
            name="hive.message_publish",
            description="Publish a Hive message. to_urn=null for broadcast.",
            inputSchema={
                "type": "object",
                "properties": {
                    "from_urn": {"type": "string"},
                    "to_urn": {"type": "string"},
                    "topic": {"type": "string"},
                    "payload": {"type": "object"},
                    "in_reply_to": {"type": "string"},
                },
                "required": ["from_urn", "topic", "payload"],
            },
        ),
        Tool(
            name="hive.message_list",
            description="List Hive messages. Filter by recipient/topic.",
            inputSchema={
                "type": "object",
                "properties": {
                    "to_urn": {"type": "string"},
                    "topic": {"type": "string"},
                    "since": {"type": "number"},
                    "limit": {"type": "integer", "default": 100},
                },
            },
        ),
        Tool(
            name="mnemos.memory_search",
            description="Search MNEMOS memory (PYTHIA). Semantic+keyword.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "category": {"type": "string"},
                    "limit": {"type": "integer", "default": 10},
                    "min_score": {"type": "number"},
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="mnemos.memory_create",
            description="Save a memory to MNEMOS. Category: infrastructure/solutions/patterns/decisions/projects/standards.",
            inputSchema={
                "type": "object",
                "properties": {
                    "content": {"type": "string"},
                    "category": {"type": "string"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["content", "category"],
            },
        ),
        Tool(
            name="mnemos.memory_get",
            description="Get a MNEMOS memory by id (mem_XXX).",
            inputSchema={"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]},
        ),
    ]
    if GRAEAE_ENABLED:
        tools.extend([
        Tool(
            name="graeae.consult",
            description="Submit a multi-LLM consensus consultation to GRAEAE (PYTHIA). Modes: auto/single/all/debate/majority.",
            inputSchema={
                "type": "object",
                "properties": {
                    "prompt": {"type": "string"},
                    "task_type": {
                        "type": "string",
                        "description": "reasoning/architecture_design/code_generation/web_search",
                    },
                    "mode": {
                        "type": "string",
                        "enum": ["auto", "local", "external", "all", "single", "debate", "majority"],
                        "default": "auto",
                    },
                    "models": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "explicit model list (overrides mode)",
                    },
                    "limit_chars": {"type": "integer"},
                    "format": {"type": "string", "default": "full"},
                },
                "required": ["prompt"],
            },
        ),
        Tool(
            name="graeae.muses",
            description="List available GRAEAE muses (LLM providers + models).",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="graeae.modes",
            description="List GRAEAE consultation modes + their descriptions.",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="graeae.get",
            description="Get a previous GRAEAE consultation by id.",
            inputSchema={
                "type": "object",
                "properties": {"consultation_id": {"type": "string"}},
                "required": ["consultation_id"],
            },
        ),
        ])
    return tools


@_register("call_tool")
async def handle_call_tool(name: str, args: dict[str, Any]) -> list[TextContent]:
    try:
        if name.startswith("graeae.") and not GRAEAE_ENABLED:
            return [
                TextContent(
                    type="text",
                    text=json.dumps({"error": "GRAEAE tools are disabled; set MCP_GRAEAE_ENABLED=1 only when the upstream MNEMOS service includes mnemos-graeae"}),
                )
            ]
        if name == "hive.agent_list":
            r = await _hive("GET", "/v1/agents", params={k: v for k, v in args.items() if v})
        elif name == "hive.agent_register":
            r = await _hive("POST", "/v1/agents/register", json=args)
        elif name == "hive.job_create":
            r = await _hive("POST", "/v1/jobs", json=args)
        elif name == "hive.job_next":
            r = await _hive("POST", f"/v1/jobs/next", params={"agent_urn": args["agent_urn"]})
        elif name == "hive.job_list":
            r = await _hive("GET", "/v1/jobs", params={k: v for k, v in args.items() if v})
        elif name == "hive.job_update":
            jid = args.pop("id")
            r = await _hive("PATCH", f"/v1/jobs/{jid}", json=args)
        elif name == "hive.message_publish":
            r = await _hive("POST", "/v1/messages", json=args)
        elif name == "hive.message_list":
            r = await _hive("GET", "/v1/messages", params={k: v for k, v in args.items() if v})
        elif name == "mnemos.memory_search":
            r = await _mnemos("POST", "/v1/memories/search", json=args)
        elif name == "mnemos.memory_create":
            payload = dict(args)
            tags = payload.pop("tags", None)
            if tags is not None:
                metadata = dict(payload.get("metadata") or {})
                metadata["tags"] = tags
                payload["metadata"] = metadata
            r = await _mnemos("POST", "/v1/memories", json=payload)
        elif name == "mnemos.memory_get":
            r = await _mnemos("GET", f"/v1/memories/{args['id']}")
        elif name == "graeae.consult":
            r = await _mnemos("POST", "/v1/consultations", json=args)
        elif name == "graeae.muses":
            r = await _mnemos("GET", "/v1/consultations/muses")
        elif name == "graeae.modes":
            r = await _mnemos("GET", "/v1/consultations/modes")
        elif name == "graeae.get":
            r = await _mnemos("GET", f"/v1/consultations/{args['consultation_id']}")
        else:
            return [TextContent(type="text", text=json.dumps({"error": f"unknown tool: {name}"}))]
        return [TextContent(type="text", text=json.dumps(r, default=str))]
    except httpx.HTTPStatusError as e:
        return [
            TextContent(
                type="text",
                text=json.dumps({"error": str(e), "status": e.response.status_code, "body": e.response.text[:500]}),
            )
        ]
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": type(e).__name__, "msg": str(e)}))]


# Silence -32601 method-not-found warnings on opencode boot.
@_register("list_resources")
async def _list_resources() -> list[Resource]:
    return []


@_register("list_prompts")
async def _list_prompts() -> list[Prompt]:
    return []


if not _MCP_DECORATOR_API:
    from mcp.types import CallToolResult, ListPromptsResult, ListResourcesResult, ListToolsResult

    async def _v2_list_tools(_context, _params):
        return ListToolsResult(tools=await handle_list_tools())

    async def _v2_call_tool(_context, params):
        return CallToolResult(content=await handle_call_tool(params.name, dict(params.arguments or {})))

    async def _v2_list_resources(_context, _params):
        return ListResourcesResult(resources=await _list_resources())

    async def _v2_list_prompts(_context, _params):
        return ListPromptsResult(prompts=await _list_prompts())

    server = Server(
        "graeae-hive-mind",
        on_list_tools=_v2_list_tools,
        on_call_tool=_v2_call_tool,
        on_list_resources=_v2_list_resources,
        on_list_prompts=_v2_list_prompts,
    )


# ---------- SSE transport ----------

sse = SseServerTransport("/messages/")


async def handle_sse(request):
    async with sse.connect_sse(request.scope, request.receive, request._send) as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="graeae-hive-mind",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


# Inbound ingress credential, independent of the two UPSTREAM tokens this
# process holds (HIVE_BUS_TOKEN and MNEMOS_TOKEN). Without it the bridge is an
# open relay: anything that can reach the port gets both upstreams' authority.
MCP_BRIDGE_TOKEN = os.environ.get("MCP_BRIDGE_TOKEN", "").strip()

_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}


def mcp_bridge_host_env() -> str:
    """Bind address. Defaults to loopback: this process relays two privileged
    upstream credentials, so it must not be reachable off-box by accident."""
    return os.environ.get("MCP_BRIDGE_HOST", "127.0.0.1").strip() or "127.0.0.1"


class _RequireBridgeToken(BaseHTTPMiddleware):
    """Authenticate INBOUND callers.

    Enforced whenever MCP_BRIDGE_TOKEN is set. Binding off-loopback without one
    is refused outright at startup (see __main__), so an exposed bridge is
    always authenticated and a loopback-only dev run stays frictionless.
    """

    async def dispatch(self, request, call_next):
        if MCP_BRIDGE_TOKEN:
            header = request.headers.get("authorization", "")
            presented = header[7:] if header.lower().startswith("bearer ") else ""
            if not secrets.compare_digest(presented, MCP_BRIDGE_TOKEN):
                return PlainTextResponse("unauthorized", status_code=401)
        return await call_next(request)


app = Starlette(
    debug=False,
    routes=[
        Route("/sse", endpoint=handle_sse),
        Mount("/messages/", app=sse.handle_post_message),
    ],
    middleware=[Middleware(_RequireBridgeToken)],
)


if __name__ == "__main__":
    _host = mcp_bridge_host_env()
    if _host not in _LOOPBACK_HOSTS and not MCP_BRIDGE_TOKEN:
        raise SystemExit(
            f"refusing to bind {_host} without MCP_BRIDGE_TOKEN: this bridge relays "
            "HIVE_BUS_TOKEN and MNEMOS_TOKEN upstream, so an unauthenticated "
            "off-loopback listener hands both to anyone who can reach the port. "
            "Set MCP_BRIDGE_TOKEN, or leave MCP_BRIDGE_HOST at 127.0.0.1."
        )
    uvicorn.run(app, host=_host, port=mcp_port_env())
