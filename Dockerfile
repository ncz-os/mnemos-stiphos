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
# Built FROM mnemos-core: STIPHOS depends on mnemos-core (already installed in
# the base, which also provides aiosqlite for the default SQLite repository).
# This layer only adds the hive package + its [mcp] server surface.
# ─────────────────────────────────────────────────────────────────────────────
ARG MNEMOS_CORE_IMAGE=ghcr.io/ncz-os/mnemos-core:latest
FROM ${MNEMOS_CORE_IMAGE}

ARG MNEMOS_STIPHOS_REF=unknown

LABEL org.opencontainers.image.title="mnemos-stiphos" \
      org.opencontainers.image.description="STIPHOS — the GRAEAE Hive Mind service for MNEMOS: agent coordination, claim/triage job queue, cost-tier dispatch. Beta. Built FROM mnemos-core." \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://gitlab.com/ncz-os/mnemos-stiphos" \
      org.mnemos.variant="stiphos" \
      org.mnemos.stiphos="${MNEMOS_STIPHOS_REF}"

COPY . /src/stiphos

# Install the hive package with its MCP-SSE bridge server surface (uvicorn +
# starlette + mcp). mnemos-core is already satisfied by the base image; a
# constraint pins it so the resolver never pulls a divergent core wheel.
RUN MNEMOS_CORE_VERSION="$(python -c 'import importlib.metadata as m; print(m.version("mnemos-core"))')" && \
    printf 'mnemos-core==%s\n' "$MNEMOS_CORE_VERSION" > /tmp/core-constraint.txt && \
    python -m pip install --no-cache-dir --constraint /tmp/core-constraint.txt \
        '/src/stiphos[mcp]' && \
    python -m pip check && \
    rm -rf /src/stiphos /tmp/core-constraint.txt

ENV PYTHONUNBUFFERED=1

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health').read()" || exit 1

CMD ["python", "-m", "uvicorn", "mnemos.hive_mind.service:app", "--host", "0.0.0.0", "--port", "8080"]
