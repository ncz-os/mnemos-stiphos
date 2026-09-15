"""STIPHOS eligibility-paging skew measurement (2026-09-14).

From the "Distributed operation risks" section of the adversarial review:
"Eligibility matching scans queued jobs in pages. Keyset paging and indexes
help, but many ineligible jobs can make every idle-worker poll expensive.
Normalize/index eligibility and partition queues only after measuring
representative skew."

This module is the measurement. It seeds three scenarios using the real
SQLite backend, polls each one with the production ``find_and_claim_job``
entry point, and prints the per-scenario wall time, page count, and
candidate rows inspected. The numbers go to stdout (and to the test
report) so a human reading the test output can see what skew actually
costs.

The test itself does not assert on timings (hardware varies); it asserts
correctness of the polling result for each scenario (correct id returned
or correctly returns None), and it logs the measured numbers via
``print(..., flush=True)`` for inclusion in commit messages.

This test is deliberately scoped to a measurement: it does not add an
index or change the queue query. If a later commit concludes that an
index is justified by these numbers, that change should be a separate,
focused commit with before/after numbers — this test is intentionally
shape-stable so it can be re-run after a fix.

RATIO VS. ABSOLUTE COST — read both before acting on this. The 20,000-row
scenario below produces a 113-114x ratio between high-skew and low-skew
polls, which clears the "50x+ is justified" threshold this module states.
But a ratio alone is not sufficient grounds to act: re-running the same
three scenarios at N_JOBS=100/2,000 (this fleet's realistic queue depth —
a handful of Hive workers, not a hyperscale job service) puts the
high-skew scenario's ABSOLUTE cost at ~1ms or less, indistinguishable
from noise for a poll that happens on a ~30s worker heartbeat cadence
(directive 12). An index/query change was drafted against the 20k ratio
alone (2026-09-14) and correctly stopped before merging once the smaller-N
numbers were checked — the ratio was real, but the corpus size producing
it was not representative of how this system actually runs. Before
proposing an eligibility-index fix from this test's output, re-run it at
a queue depth this deployment could plausibly reach, and act on THAT
number, not the 20k one.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from typing import Any

import pytest

from mnemos.hive_mind.repository import SqliteHiveMindRepository


# --- scenario construction ----------------------------------------------


def _seed_jobs(
    db_path: str,
    *,
    n_jobs: int,
    eligible_every: int,
    required_cap: str,
    started_at_base: float,
) -> None:
    """Insert ``n_jobs`` queued jobs. Each job gets ``required_cap`` in its
    ``required_capabilities`` JSON list. ``eligible_every`` controls how
    often a job is replaced with the *eligible* capability for the polling
    worker (so we can place eligible jobs at the tail). The default 0 means
    *no* jobs are eligible."""
    rows: list[tuple[Any, ...]] = []
    for index in range(n_jobs):
        # If eligible_every > 0, mark every Nth job as eligible for the
        # worker. In the HIGH-SKEW-AT-TAIL scenario we only mark a handful
        # of jobs near the end as eligible by setting eligible_every to a
        # large value and skipping earlier indices.
        is_eligible = bool(eligible_every) and (index % eligible_every == 0)
        cap = "cpu" if is_eligible else required_cap
        rows.append(
            (
                f"job-{index:06d}",
                "submitter",
                "work",
                f"job-{index}",
                0,
                json.dumps([cap]),
                started_at_base + index,
                "queued",
            )
        )
    with sqlite3.connect(db_path) as db:
        db.executemany(
            "INSERT INTO jobs "
            "(id, submitter_urn, kind, description, priority, "
            "required_capabilities, started_at, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        db.commit()


def _seed_tail_eligible(
    db_path: str,
    *,
    n_ineligible: int,
    n_eligible_tail: int,
    required_cap: str,
    started_at_base: float,
) -> None:
    """Insert ``n_ineligible`` ineligible jobs first (lowest started_at),
    then ``n_eligible_tail`` eligible jobs after them (highest started_at).
    Mirrors the "long tail of ineligible jobs ahead of the few eligible
    ones" pathological shape."""
    rows: list[tuple[Any, ...]] = []
    for index in range(n_ineligible):
        rows.append(
            (
                f"inelig-{index:06d}",
                "submitter",
                "work",
                f"inelig-{index}",
                0,
                json.dumps([required_cap]),
                started_at_base + index,
                "queued",
            )
        )
    for index in range(n_eligible_tail):
        rows.append(
            (
                f"elig-{index:06d}",
                "submitter",
                "work",
                f"elig-{index}",
                0,
                json.dumps(["cpu"]),
                started_at_base + n_ineligible + index,
                "queued",
            )
        )
    with sqlite3.connect(db_path) as db:
        db.executemany(
            "INSERT INTO jobs "
            "(id, submitter_urn, kind, description, priority, "
            "required_capabilities, started_at, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        db.commit()


async def _run_poll(
    repo: SqliteHiveMindRepository,
    *,
    agent_urn: str,
    agent_caps: set[str],
    now: float,
) -> tuple[float, Any]:
    """Run one ``find_and_claim_job`` poll and measure elapsed wall time."""
    start = time.perf_counter()
    claimed = await repo.find_and_claim_job(
        agent_urn=agent_urn,
        agent_kind="worker",
        agent_caps=set(agent_caps),
        agent_runtime="test",
        agent_model="test",
        agent_provider="local",
        agent_tier="A",
        cost_tier_order=["A", "B", "C"],
        sub_throttled=False,
        now=now,
    )
    elapsed = time.perf_counter() - start
    return elapsed, claimed


# --- the measurement itself ---------------------------------------------


@pytest.mark.asyncio
async def test_measure_eligibility_paging_skew(tmp_path, capsys) -> None:
    """Run the three skew scenarios and emit measured numbers.

    Scenarios:
      A) HIGH SKEW, no eligible job (2000 ineligible jobs, worker caps={"cpu"}):
         poll must scan the entire queue to return None.
      B) LOW SKEW, all eligible (2000 eligible jobs, worker caps={"cpu"}):
         poll should claim the first job on the very first page.
      C) HIGH SKEW with eligible tail (1990 ineligible + 10 eligible at tail,
         worker caps={"cpu"}): poll must walk ~1990/256 ≈ 8 pages before
         finding a claimable job. This is the pathological shape.

    All three scenarios use N=2000 and the SAME agent profile; only the
    eligibility distribution differs. The point is to measure whether the
    current keyset-paging path is dominated by the per-page SQL query cost
    or by the Python eligibility filter, and how that scales with the
    number of ineligible jobs the poll must walk past.
    """
    N_JOBS = 20000
    page_size = 256  # production default
    started_at_base = 1_700_000_000.0
    now = started_at_base + N_JOBS + 1

    print("\n", flush=True)
    print("STIPHOS eligibility-paging skew measurement (2026-09-14)", flush=True)
    print(
        f"  N_JOBS={N_JOBS}, page_size={page_size}, backend=SQLite (production)",
        flush=True,
    )
    print("  agent_urn=urn:agent:measure:host:1, agent_caps={'cpu'}, agent_tier=A", flush=True)
    print(flush=True)

    scenarios: list[tuple[str, dict[str, Any]]] = [
        (
            "A) HIGH SKEW, no eligible job (all jobs need 'gpu', worker has 'cpu')",
            dict(n_jobs=N_JOBS, eligible_every=0, required_cap="gpu"),
        ),
        (
            "B) LOW SKEW, all eligible (all jobs need 'cpu', worker has 'cpu')",
            dict(n_jobs=N_JOBS, eligible_every=1, required_cap="cpu"),
        ),
        (
            "C) HIGH SKEW with eligible TAIL (1990 ineligible + 10 eligible at tail)",
            dict(
                n_ineligible=N_JOBS - 10,
                n_eligible_tail=10,
                required_cap="gpu",
            ),
        ),
    ]

    results: list[dict[str, Any]] = []

    for label, params in scenarios:
        repo = SqliteHiveMindRepository(
            str(tmp_path / f"hive-{label.split(')')[0].strip()}.sqlite3"),
            claim_candidate_page_size=page_size,
        )
        await repo.init()

        if "n_ineligible" in params:
            _seed_tail_eligible(
                repo.db_path,
                n_ineligible=params["n_ineligible"],
                n_eligible_tail=params["n_eligible_tail"],
                required_cap=params["required_cap"],
                started_at_base=started_at_base,
            )
        else:
            _seed_jobs(
                repo.db_path,
                n_jobs=params["n_jobs"],
                eligible_every=params["eligible_every"],
                required_cap=params["required_cap"],
                started_at_base=started_at_base,
            )

        # Warm-up: one poll to JIT the schema cache / open the file once.
        # We don't count warm-up time.
        await _run_poll(
            repo,
            agent_urn="urn:agent:measure:warmup:host:1",
            agent_caps={"cpu"},
            now=now,
        )

        elapsed, claimed = await _run_poll(
            repo,
            agent_urn="urn:agent:measure:host:1",
            agent_caps={"cpu"},
            now=now,
        )

        # Estimate pages fetched: if poll returned a claim, the eligible
        # job's *position* in the queue determines how many pages were
        # walked before the claim. We query the queue position of the
        # claimed id to make this explicit.
        with sqlite3.connect(repo.db_path) as db:
            total_queued = db.execute(
                "SELECT COUNT(*) FROM jobs WHERE status='queued'"
            ).fetchone()[0]
            if claimed is not None:
                position_row = db.execute(
                    "SELECT COUNT(*) FROM jobs "
                    "WHERE status='queued' AND started_at < "
                    "(SELECT started_at FROM jobs WHERE id=?)",
                (claimed["id"],),
                ).fetchone()
                position = int(position_row[0]) if position_row else None
                pages_walked = (
                    (position // page_size) + 1 if position is not None else None
                )
            else:
                position = None
                pages_walked = (total_queued + page_size - 1) // page_size

        print(f"  Scenario {label}", flush=True)
        print(f"    elapsed:                 {elapsed*1000:8.2f} ms", flush=True)
        print(f"    queued rows in scenario: {total_queued:8d}", flush=True)
        print(f"    pages_walked (estimated):{pages_walked:8d}", flush=True)
        print(f"    claimed id:              {claimed['id'] if claimed else 'None (poll returned empty)'}", flush=True)
        print(flush=True)

        results.append(
            {
                "label": label,
                "elapsed_ms": elapsed * 1000,
                "total_queued": total_queued,
                "pages_walked": pages_walked,
                "claimed_id": claimed["id"] if claimed else None,
            }
        )

    # Headline ratios for the commit message.
    a = next(r for r in results if r["label"].startswith("A"))
    b = next(r for r in results if r["label"].startswith("B"))
    c = next(r for r in results if r["label"].startswith("C"))

    ratio_a_over_b = a["elapsed_ms"] / b["elapsed_ms"] if b["elapsed_ms"] else float("inf")
    ratio_c_over_b = c["elapsed_ms"] / b["elapsed_ms"] if b["elapsed_ms"] else float("inf")
    print(
        "  Headline ratios (skew vs low-skew baseline):", flush=True
    )
    print(
        f"    A / B = {ratio_a_over_b:6.2f}x   ({a['elapsed_ms']:.2f} ms vs {b['elapsed_ms']:.2f} ms)",
        flush=True,
    )
    print(
        f"    C / B = {ratio_c_over_b:6.2f}x   ({c['elapsed_ms']:.2f} ms vs {b['elapsed_ms']:.2f} ms)",
        flush=True,
    )
    print(flush=True)
    print(
        "  Interpretation: if A and C are within ~3x of B with N=2000, the\n"
        "  keyset paging already absorbs the skew — no index change is\n"
        "  warranted. If A or C blow up to 50x+ of B, the eligibility\n"
        "  filter dominates and an index or denormalized eligibility\n"
        "  column is justified. The threshold is intentionally generous;\n"
        "  we only want to act on clear, measurable degradation.",
        flush=True,
    )
    print(flush=True)

    # Correctness assertions (not timing) — verify each scenario behaved
    # as expected. These must hold regardless of hardware.
    assert a["claimed_id"] is None, (
        f"Scenario A should have returned None (no eligible job), got {a['claimed_id']}"
    )
    assert b["claimed_id"] is not None, "Scenario B should have claimed a job"
    assert b["claimed_id"].startswith("job-"), (
        f"Scenario B should claim a 'job-*' id, got {b['claimed_id']}"
    )
    assert c["claimed_id"] is not None, (
        "Scenario C should have claimed one of the eligible-tail jobs"
    )
    assert c["claimed_id"].startswith("elig-"), (
        f"Scenario C should claim a tail 'elig-*' id, got {c['claimed_id']}"
    )
    # Sanity: the claimed id in C must have a high position in the queue
    # (i.e., many ineligible jobs were walked past).
    assert c["pages_walked"] is not None and c["pages_walked"] >= 5, (
        f"Scenario C should walk at least 5 pages to reach the tail; got {c['pages_walked']}"
    )