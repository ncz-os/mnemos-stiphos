"""F12 regression: STIPHOS hive_cache must not silently write truncated /
invalid JSON to the cache (historical bug at the old 32_000-char slice), and
must evict any malformed legacy entry already in the cache instead of letting
a json.JSONDecodeError bubble up to the caller.

The fix lives in `mnemos.hive_mind.service.cache_store` and `cache_lookup`:

* `cache_store` no longer slices the serialized JSON at a fixed character
  boundary. If the serialized payload exceeds `CACHE_RESULT_JSON_MAX_BYTES`
  (currently 1 MiB), the entry is **not** written at all — a debug-level log
  line is emitted and the function returns. A cache miss is always safe; a
  corrupted cache hit is not.
* `cache_lookup` guards against malformed legacy rows: if `json.loads` fails
  on the stored `result_json`, the row is deleted and the function returns
  None (cache miss) instead of raising.
"""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

import aiosqlite
import pytest

from mnemos.hive_mind import service


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def fresh_db_path(tmp_path: Path) -> str:
    """Point the service at a throwaway SQLite file and initialise its schema.

    The service normally runs its `hive_cache` (and friends) DDL inside the
    FastAPI lifespan handler. We don't want to drag in `TestClient` here —
    this test exercises `cache_store` / `cache_lookup` directly — so we run
    `service.SCHEMA` ourselves via `sqlite3`. The lifespan handler also runs
    some additive ALTER migrations, but none of them touch `hive_cache`, so
    this is sufficient for these tests."""
    db_path = str(tmp_path / "f12-cache.sqlite3")
    service.DB_PATH = db_path
    with sqlite3.connect(db_path) as db:
        db.executescript(service.SCHEMA)
        db.commit()
    return db_path


async def _row_count(db_path: str, cache_key: str) -> int:
    async with aiosqlite.connect(db_path) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM hive_cache WHERE cache_key=?", (cache_key,)
        ) as cur:
            row = await cur.fetchone()
            return int(row[0]) if row else 0


# ---------------------------------------------------------------------------
# Write-path: oversized entries must NOT be silently truncated.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cache_store_skips_oversized_payload(fresh_db_path: str) -> None:
    """`cache_store` must refuse to write a payload that would exceed the
    size bound. The pre-fix behaviour was to slice at 32_000 characters and
    write invalid JSON to the cache."""
    cache_key = "f12-oversized-key"
    oversized_result = {
        "exit_code": 0,
        # Build a value whose serialized JSON comfortably exceeds the bound.
        "blob": "x" * (service.CACHE_RESULT_JSON_MAX_BYTES + 4096),
    }
    # Sanity-check the test fixture itself: the payload really is over the
    # bound, otherwise this test would silently degenerate into the
    # normal-write path.
    serialized_len = len(json.dumps(oversized_result, default=str).encode("utf-8"))
    assert serialized_len > service.CACHE_RESULT_JSON_MAX_BYTES

    async with aiosqlite.connect(fresh_db_path) as db:
        await service.cache_store(
            db,
            cache_key,
            "source-job-id",
            oversized_result,
            None,
            "model-x",
            "provider-y",
            0.0,
        )
        await db.commit()

    # Nothing must have been written — a cache miss is safe; a poisoned
    # cache row is not.
    assert await _row_count(fresh_db_path, cache_key) == 0, (
        "cache_store wrote a row despite the payload exceeding "
        "CACHE_RESULT_JSON_MAX_BYTES — pre-fix bug regressed?"
    )


@pytest.mark.asyncio
async def test_cache_store_writes_a_valid_small_payload(fresh_db_path: str) -> None:
    """Sanity guard: the new size check must not regress normal-sized writes."""
    cache_key = "f12-small-key"
    result = {"exit_code": 0, "answer": "ok"}
    async with aiosqlite.connect(fresh_db_path) as db:
        await service.cache_store(
            db,
            cache_key,
            "source-job-id",
            result,
            None,
            "model-x",
            "provider-y",
            0.0,
        )
        await db.commit()

    assert await _row_count(fresh_db_path, cache_key) == 1
    # And the row must round-trip as valid JSON, of course.
    async with aiosqlite.connect(fresh_db_path) as db:
        cached = await service.cache_lookup(db, cache_key)
    assert cached is not None
    assert cached["result"] == result


# ---------------------------------------------------------------------------
# Read-path: malformed legacy rows must be evicted, not raised on.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cache_lookup_evicts_malformed_legacy_entry(fresh_db_path: str) -> None:
    """A prior build of STIPHOS could leave a 32_000-char-truncated JSON
    slice in `hive_cache.result_json`. `cache_lookup` must NOT raise on
    decode — it must treat the row as a miss and evict it so the next
    `cache_store` for the same key gets a clean slate."""
    cache_key = "f12-poisoned-legacy-key"
    # 32_000 characters of valid-looking-but-incomplete JSON, exactly the
    # shape the historical bug produced: a slice cut mid-structure. We add
    # extra cruft so even json.JSONDecodeError's strict parser rejects it.
    truncated_garbage = '{"answer": "' + ("x" * 31_990)  # no closing quotes/braces

    # Plant the malformed row directly in the cache, bypassing cache_store
    # (which would now refuse to write such a payload).
    async with aiosqlite.connect(fresh_db_path) as db:
        await db.execute(
            "INSERT INTO hive_cache (cache_key, result_json, source_job_id, "
            "result_mnemos_id, hit_count, cost_saved_usd, model, provider, "
            "cached_at, last_hit_at) "
            "VALUES (?, ?, ?, NULL, 0, 0, 'legacy', 'legacy', ?, NULL)",
            (cache_key, truncated_garbage, "legacy-source", time.time()),
        )
        await db.commit()
    assert await _row_count(fresh_db_path, cache_key) == 1

    # The lookup must NOT raise json.JSONDecodeError; it must return None.
    async with aiosqlite.connect(fresh_db_path) as db:
        cached = await service.cache_lookup(db, cache_key)
    assert cached is None, (
        "cache_lookup returned a value for a malformed cache row — the "
        "pre-fix bug (json.loads raising unhandled) regressed?"
    )

    # And the poisoned row must have been evicted, so a future write of the
    # same key does not collide with garbage.
    assert await _row_count(fresh_db_path, cache_key) == 0, (
        "cache_lookup did not evict the malformed cache row — a future "
        "cache_store for this key would have collided with garbage."
    )


@pytest.mark.asyncio
async def test_cache_lookup_returns_none_for_missing_key(fresh_db_path: str) -> None:
    """Sanity guard: the new try/except must not regress the normal cache-miss
    path."""
    async with aiosqlite.connect(fresh_db_path) as db:
        cached = await service.cache_lookup(db, "definitely-not-cached")
    assert cached is None


# ---------------------------------------------------------------------------
# End-to-end: legacy truncated JSON written directly via SQL is read-back-safe
# and a fresh write of the same key succeeds.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_legacy_garbage_does_not_block_a_fresh_write(fresh_db_path: str) -> None:
    """Even when a poisoned legacy row is already on disk, a fresh
    `cache_store` for the same key must succeed once `cache_lookup` has
    evicted the bad row — i.e. the system self-heals on first miss."""
    cache_key = "f12-self-heal-key"
    truncated = "{\"answer\": \"" + ("y" * 31_990)  # intentionally broken
    async with aiosqlite.connect(fresh_db_path) as db:
        await db.execute(
            "INSERT INTO hive_cache (cache_key, result_json, source_job_id, "
            "result_mnemos_id, hit_count, cost_saved_usd, model, provider, "
            "cached_at, last_hit_at) "
            "VALUES (?, ?, ?, NULL, 0, 0, 'legacy', 'legacy', ?, NULL)",
            (cache_key, truncated, "legacy-source", time.time()),
        )
        await db.commit()

    # First lookup evicts the bad row.
    async with aiosqlite.connect(fresh_db_path) as db:
        assert await service.cache_lookup(db, cache_key) is None
    assert await _row_count(fresh_db_path, cache_key) == 0

    # A fresh write for the same key must succeed without an
    # IntegrityError / UNIQUE constraint collision (which is what would
    # happen if the eviction did not run).
    fresh_result = {"exit_code": 0, "answer": "fresh"}
    async with aiosqlite.connect(fresh_db_path) as db:
        await service.cache_store(
            db,
            cache_key,
            "fresh-source-job",
            fresh_result,
            None,
            "model-x",
            "provider-y",
            0.0,
        )
        await db.commit()
    assert await _row_count(fresh_db_path, cache_key) == 1

    async with aiosqlite.connect(fresh_db_path) as db:
        cached = await service.cache_lookup(db, cache_key)
    assert cached is not None
    assert cached["result"] == fresh_result