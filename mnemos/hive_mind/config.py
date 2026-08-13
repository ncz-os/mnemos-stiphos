"""Small environment helpers shared by STIPHOS executables."""

from __future__ import annotations

import os
import socket


def system_hive_url_env() -> str:
    return os.environ.get("HIVE_URL", "http://192.168.207.67:5005")


def mcp_hive_url_env() -> str:
    return os.environ.get("HIVE_URL", "http://127.0.0.1:5005")


def agent_host_env() -> str:
    return os.environ.get("AGENT_HOST", socket.gethostname().split(".")[0])


def heartbeat_interval_env() -> float:
    return float(os.environ.get("HEARTBEAT_INTERVAL", "15"))


def claim_jobs_enabled_env() -> bool:
    return os.environ.get("CLAIM_JOBS", "0") == "1"


def mcp_mnemos_url_env() -> str:
    return os.environ.get("MNEMOS_URL", "http://192.168.207.67:5002")


def mcp_mnemos_token_env() -> str:
    return os.environ.get("MNEMOS_TOKEN", "")


def mcp_port_env() -> int:
    return int(os.environ.get("PORT", "5006"))


def agent_bus_db_env() -> str:
    return os.environ.get("AGENT_BUS_DB", "/srv/agent-bus/agents.db")
