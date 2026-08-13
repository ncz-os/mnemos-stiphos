# syntax=docker/dockerfile:1
# ─────────────────────────────────────────────────────────────────────────────
# mnemos-stiphos — the STIPHOS hive service image.
#
# Publishes: ghcr.io/ncz-os/mnemos-stiphos
# Arch:      linux/amd64 + linux/arm64
#
# STIPHOS (Στίφος, "a marshaled host") is the GRAEAE Hive Mind: fleet-wide agent
# coordination, a claim/triage job queue, and cost-tier dispatch. It is a
# SEPARATE service (its own ASGI app + port), not a router mounted into the core
# API — one process per container — which is why it ships as its own image
# rather than being folded into the everything image.
#
# STIPHOS carries no ML runtime or model assets. Build it from the slim Python
# image and install only the hive package + its [mcp] server surface.
# ─────────────────────────────────────────────────────────────────────────────
ARG PYTHON_IMAGE=python:3.11-slim
FROM ${PYTHON_IMAGE}

ARG MNEMOS_STIPHOS_REF=unknown

LABEL org.opencontainers.image.title="mnemos-stiphos" \
      org.opencontainers.image.description="STIPHOS — the GRAEAE Hive Mind service for MNEMOS: agent coordination, claim/triage job queue, cost-tier dispatch. Beta." \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://gitlab.com/ncz-os/mnemos-stiphos" \
      org.mnemos.variant="stiphos" \
      org.mnemos.stiphos="${MNEMOS_STIPHOS_REF}"

COPY . /src/stiphos

# Install the hive package with its MCP-SSE bridge server surface (uvicorn +
# starlette + mcp).
RUN python -m pip install --no-cache-dir '/src/stiphos[mcp]' && \
    python -m pip check && \
    rm -rf /src/stiphos

ENV PYTHONUNBUFFERED=1 \
    AGENT_BUS_DB=/data/agents.db

VOLUME ["/data"]

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health').read()" || exit 1

CMD ["python", "-m", "uvicorn", "mnemos.hive_mind.service:app", "--host", "0.0.0.0", "--port", "8080"]
