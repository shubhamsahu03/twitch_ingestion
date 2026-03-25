"""
engine/rank_indexer.py
======================
Phase 4 — Global Rank Indexer

For each of the 48 months (2022–2025), paginate the SullyGnome channel
leaderboard API and build a sparse index:

    month_index[month_key][channel_id] = rownum

Then perform O(1) lookups for every (channel_id × month) pair across 2,429
channels.  Channels not found within MAX_RANK_THRESHOLD receive rank=NULL
and status="not_found_within_threshold".

Architecture
────────────
┌────────────────────────────────────────────────────────────────┐
│  RankConfig        — all tunable knobs in one place            │
│  RankIndexer       — orchestrator (public surface)             │
│    fetch_page()    — single async page fetch with retry        │
│    get_ranks_for_month()  — adaptive phased pagination         │
│    run_all_months()       — sequential month loop              │
│    build_panel_dataframe() — tidy panel → Parquet/CSV          │
│  MonthMetrics      — per-month stats (asyncio-safe)            │
│  ProgressStore     — JSON-backed crash-recovery (mid-month)    │
│  RedisCache        — optional Redis layer (graceful no-op)     │
└────────────────────────────────────────────────────────────────┘

Bugs fixed vs v1
────────────────
1. consecutive_no_new double-counted  → single counter, one increment per page
2. save_offset() never called         → called after every concurrent batch
3. import random inside fetch_page()  → moved to module top-level
4. build_panel_dataframe status wrong for parquet-reload failures
                                      → _PARQUET_RELOAD_FAILED sentinel
5. _load_month_from_parquet iterrows() → vectorized dict(zip(...))
6. metrics.api_calls / cache_hits not asyncio-safe → MonthMetrics uses Lock
7. Missing engine/__init__.py         → added separately
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import aiohttp

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False

try:
    import redis.asyncio as aioredis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

logger = logging.getLogger("twitch_engine.rank_indexer")

# Sentinel: distinguishes "month processed, 0 found" from
# "month was skipped but Parquet reload failed" in build_panel_dataframe.
_PARQUET_RELOAD_FAILED = object()


# ──────────────────────────────────────────────────────────────────────────────
#  CONFIG
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class RankConfig:
    """Single source of truth for all Phase 4 tuning parameters."""

    # ── API ──────────────────────────────────────────────────────────────────
    base_url: str        = "https://sullygnome.com"
    page_size: int       = 100       # rows per API call (fixed by API)
    request_timeout: int = 20        # seconds

    # ── Performance ──────────────────────────────────────────────────────────
    max_rank_threshold: int = 10_000  # stop scanning beyond this rank
    concurrency: int        = 15      # async semaphore slots
    early_stop_pages: int   = 5       # consecutive pages with 0 new targets → stop
    retry_limit: int        = 3       # max retries per request
    backoff_base: float     = 1.0     # exponential backoff seed (seconds)

    # ── API budget ────────────────────────────────────────────────────────────
    max_api_calls_per_month: int = 200
    max_api_calls_global: int   = 15_000

    # ── Pagination phases: list of (offset_start, offset_end) — end exclusive ─
    phases: list[tuple[int, int]] = field(default_factory=lambda: [
        (0,      500),
        (500,    2_000),
        (2_000,  5_000),
        (5_000,  10_000),
    ])

    # ── Storage ───────────────────────────────────────────────────────────────
    output_dir: Path    = field(default_factory=lambda: Path("data/ranks"))
    parquet_dir: Path   = field(default_factory=lambda: Path("data/ranks/parquet"))
    progress_file: Path = field(default_factory=lambda: Path("checkpoints/rank_progress.json"))

    # ── Redis (optional) ─────────────────────────────────────────────────────
    redis_url: str      = "redis://localhost:6379"
    redis_api_ttl: int  = 86_400    # 24 h
    redis_rank_ttl: int = 604_800   # 7 days
    use_redis: bool     = False

    # ── Jitter between requests ───────────────────────────────────────────────
    jitter_min: float = 0.05
    jitter_max: float = 0.20

    def endpoint(self, month_key: str, offset: int) -> str:
        return (
            f"{self.base_url}/api/tables/channeltables/getchannels"
            f"/{month_key}/0/1/3/desc/{offset}/{self.page_size}"  # ← move offset here
        )


# ──────────────────────────────────────────────────────────────────────────────
#  METRICS  (asyncio-safe)
# ──────────────────────────────────────────────────────────────────────────────

class MonthMetrics:
    """
    Per-month counters.  Mutation goes through async methods so concurrent
    fetch_page tasks cannot race on shared integers.
    """

    def __init__(self, month_key: str, total_targets: int) -> None:
        self.month_key            = month_key
        self.total_targets        = total_targets
        self.channels_found       = 0
        self.api_calls            = 0
        self.cache_hits           = 0
        self.early_stop_triggered = False
        self.stop_reason          = ""
        self.elapsed_s            = 0.0
        self._lock                = asyncio.Lock()

    async def inc_api_calls(self, n: int = 1) -> None:
        async with self._lock:
            self.api_calls += n

    async def inc_cache_hits(self, n: int = 1) -> None:
        async with self._lock:
            self.cache_hits += n

    @property
    def coverage(self) -> float:
        return self.channels_found / self.total_targets if self.total_targets else 0.0

    def log(self) -> None:
        logger.info(
            "[%s] done — found=%d/%d (%.1f%%) api_calls=%d cache_hits=%d "
            "early_stop=%s reason='%s' elapsed=%.1fs",
            self.month_key,
            self.channels_found, self.total_targets,
            self.coverage * 100,
            self.api_calls, self.cache_hits,
            self.early_stop_triggered, self.stop_reason,
            self.elapsed_s,
        )


# ──────────────────────────────────────────────────────────────────────────────
#  PROGRESS STORE  (mid-month crash recovery)
# ──────────────────────────────────────────────────────────────────────────────

class ProgressStore:
    """
    JSON file tracking:
      "completed": ["2022january", ...]          fully processed months
      "offsets":   {"2022january": 4200, ...}    last offset saved mid-month

    Atomic write (tmp → rename) prevents a corrupt file on crash.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._data: dict[str, Any] = {"completed": [], "offsets": {}}
        self._lock = asyncio.Lock()
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                with open(self._path, encoding="utf-8") as f:
                    self._data = json.load(f)
                logger.info(
                    "ProgressStore loaded — %d months completed, %d in-progress",
                    len(self._data.get("completed", [])),
                    len(self._data.get("offsets", {})),
                )
            except Exception as exc:
                logger.warning("ProgressStore corrupt (%s) — starting fresh", exc)
                self._data = {"completed": [], "offsets": {}}

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2)
        tmp.replace(self._path)  # atomic rename

    def is_month_done(self, month_key: str) -> bool:
        return month_key in self._data.get("completed", [])

    def resume_offset(self, month_key: str) -> int:
        """Last saved offset for this month; 0 if never started."""
        return self._data.get("offsets", {}).get(month_key, 0)

    async def save_offset(self, month_key: str, offset: int) -> None:
        """Call after each batch for mid-month crash recovery."""
        async with self._lock:
            self._data.setdefault("offsets", {})[month_key] = offset
            self._save()

    async def mark_month_done(self, month_key: str) -> None:
        async with self._lock:
            if month_key not in self._data["completed"]:
                self._data["completed"].append(month_key)
            self._data.get("offsets", {}).pop(month_key, None)
            self._save()


# ──────────────────────────────────────────────────────────────────────────────
#  REDIS CACHE  (optional — graceful no-op when unavailable)
# ──────────────────────────────────────────────────────────────────────────────

class RedisCache:
    """
    Redis layer with three key patterns:

      api:{month}:{offset}   STRING   raw page JSON           TTL: 24 h
      rankmap:{month}        HASH     channel_id → rank       TTL: 7 days
      rank:{month}:{cid}     STRING   individual rank (legacy fallback)

    Key upgrade over v1
    ───────────────────
    • get_rankmap()      — HGETALL rankmap:{month} → 1 round-trip pre-loads
                           all previously cached ranks for a month, replacing
                           the old loop of 2,429 individual GET calls.
    • set_rankmap_batch() — pipelines HSET + EXPIRE in a single round-trip,
                           replacing per-entry awaited SET calls.
    • Individual get_rank / set_rank kept as fallback only.
    """

    def __init__(self, config: RankConfig) -> None:
        self._config    = config
        self._client: Any = None
        self._available = False

    async def connect(self) -> None:
        if not HAS_REDIS or not self._config.use_redis:
            return
        try:
            self._client = aioredis.from_url(
                self._config.redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=2,
                # Connection pool sizing: match concurrency + headroom for
                # rankmap reads/writes that happen outside the semaphore
                max_connections=self._config.concurrency + 8,
            )
            await self._client.ping()
            self._available = True
            logger.info(
                "Redis connected — %s  (pool max_connections=%d)",
                self._config.redis_url,
                self._config.concurrency + 8,
            )
        except Exception as exc:
            logger.warning("Redis unavailable (%s) — running without cache", exc)
            self._client = None

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()

    # ── Page cache ────────────────────────────────────────────────────────────

    async def get_page(self, month_key: str, offset: int) -> Optional[list[dict]]:
        if not self._available:
            return None
        try:
            raw = await self._client.get(f"api:{month_key}:{offset}")
            return json.loads(raw) if raw else None
        except Exception:
            return None

    async def set_page(self, month_key: str, offset: int, rows: list[dict]) -> None:
        if not self._available:
            return
        try:
            await self._client.set(
                f"api:{month_key}:{offset}",
                json.dumps(rows),
                ex=self._config.redis_api_ttl,
            )
        except Exception:
            pass

    # ── Batch rankmap  (HGETALL / pipelined HSET) ─────────────────────────────

    async def get_rankmap(self, month_key: str) -> dict[int, int]:
        """
        Load ALL cached ranks for a month in ONE Redis round-trip.

        Returns {channel_id: rank} for every channel that had a rank cached.
        Callers filter to their target set.

        HGETALL rankmap:{month_key}  →  {"123": "500", "456": "1200", ...}
        """
        if not self._available:
            return {}
        try:
            raw: dict[str, str] = await self._client.hgetall(f"rankmap:{month_key}")
            return {int(cid): int(rank) for cid, rank in raw.items()}
        except Exception:
            return {}

    async def set_rankmap_batch(
        self,
        month_key: str,
        ranks: dict[int, int],
        merge: bool = True,
    ) -> None:
        """
        Write a dict of {channel_id: rank} to rankmap:{month_key} using a
        Redis pipeline — one round-trip regardless of dict size.

        merge=True  → HSET merges into existing hash (safe for incremental
                       writes after each batch of pages).
        merge=False → DEL + HSET (used for full month write at completion).

        Pipeline sequence:
            MULTI
            [DEL rankmap:{month_key}]   ← only when merge=False
            HSET rankmap:{month_key} cid1 rank1 cid2 rank2 ...
            EXPIRE rankmap:{month_key} {ttl}
            EXEC
        """
        if not self._available or not ranks:
            return
        try:
            key = f"rankmap:{month_key}"
            # HSET accepts flat: field1 val1 field2 val2 ...
            flat: dict[str, str] = {str(cid): str(rank) for cid, rank in ranks.items()}

            async with self._client.pipeline(transaction=True) as pipe:
                if not merge:
                    pipe.delete(key)
                pipe.hset(key, mapping=flat)
                pipe.expire(key, self._config.redis_rank_ttl)
                await pipe.execute()
        except Exception as exc:
            logger.debug("set_rankmap_batch failed for %s: %s", month_key, exc)

    # ── Individual rank (legacy fallback — used when rankmap not yet populated) ─

    async def get_rank(self, month_key: str, channel_id: int) -> Optional[int]:
        if not self._available:
            return None
        try:
            val = await self._client.hget(f"rankmap:{month_key}", str(channel_id))
            return int(val) if val is not None else None
        except Exception:
            return None

    @property
    def available(self) -> bool:
        return self._available


# ──────────────────────────────────────────────────────────────────────────────
#  HTTP HEADERS
# ──────────────────────────────────────────────────────────────────────────────

def _api_headers() -> dict[str, str]:
    return {
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control":   "no-cache",
        "Pragma":          "no-cache",
        "Referer":         "https://sullygnome.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    }


# ──────────────────────────────────────────────────────────────────────────────
#  RANK INDEXER
# ──────────────────────────────────────────────────────────────────────────────

class RankIndexer:
    """
    Orchestrates paginated rank indexing for all months.

    Usage:
        async with RankIndexer(channels, month_keys, config) as indexer:
            await indexer.run_all_months()
            df = indexer.build_panel_dataframe()
            indexer.save(df)
            indexer.print_summary()
    """

    def __init__(
        self,
        target_channels: list[int],
        month_keys: list[str],
        config: Optional[RankConfig] = None,
    ) -> None:
        self.config          = config or RankConfig()
        self.target_channels = target_channels
        self._target_set     = set(target_channels)
        self.month_keys      = month_keys

        # month_key → {channel_id → rank}  OR  _PARQUET_RELOAD_FAILED sentinel
        self._index: dict[str, Any] = {}

        self._global_api_calls = 0
        self._global_call_lock = asyncio.Lock()
        self._metrics: list[MonthMetrics] = []

        self._progress = ProgressStore(self.config.progress_file)
        self._cache    = RedisCache(self.config)
        self._sem      = asyncio.Semaphore(self.config.concurrency)
        self._session: Optional[aiohttp.ClientSession] = None

    # ── Session lifecycle ─────────────────────────────────────────────────────

    async def __aenter__(self) -> "RankIndexer":
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                limit=self.config.concurrency,
                limit_per_host=self.config.concurrency,
                ttl_dns_cache=300,
            ),
            timeout=aiohttp.ClientTimeout(total=self.config.request_timeout),
            headers=_api_headers(),
        )
        await self._cache.connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._session:
            await self._session.close()
        await self._cache.close()

    # ── Global budget guard ───────────────────────────────────────────────────

    async def _check_global_budget(self) -> bool:
        """Increment global counter; return False if budget exceeded."""
        async with self._global_call_lock:
            self._global_api_calls += 1
            if self._global_api_calls > self.config.max_api_calls_global:
                logger.error(
                    "Global API budget exceeded (%d > %d) — aborting.",
                    self._global_api_calls,
                    self.config.max_api_calls_global,
                )
                return False
            return True

    # ── Core page fetcher ─────────────────────────────────────────────────────

    async def fetch_page(
        self,
        month_key: str,
        offset: int,
        metrics: MonthMetrics,
    ) -> list[dict]:
        """
        Fetch one page for the given month and offset.

        1. Redis cache check → return immediately on hit.
        2. Acquire semaphore → call API with exponential-backoff retry.
        3. Validate rownum monotonicity (log, don't abort).
        4. Deduplicate within page.
        5. Cache to Redis.
        6. Return [{id, rownum}, ...].
        """
        # 1. Redis cache
        cached = await self._cache.get_page(month_key, offset)
        if cached is not None:
            await metrics.inc_cache_hits()
            return cached

        # 2. API call (semaphore acquired here — shared across concurrent tasks)
        # Budget is checked once per logical page fetch, not per retry attempt.
        # Moving it here prevents a 3-retry page from consuming 4 budget slots.
        if not await self._check_global_budget():
            return []

        rows: list[dict] = []
        async with self._sem:
            for attempt in range(self.config.retry_limit + 1):
                await asyncio.sleep(
                    random.uniform(self.config.jitter_min, self.config.jitter_max)
                )

                url = self.config.endpoint(month_key, offset)
                try:
                    assert self._session is not None
                    async with self._session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            rows = self._extract_rows(data, month_key, offset)
                            await metrics.inc_api_calls()
                            break

                        elif resp.status == 429:
                            wait = 30 + self.config.backoff_base * (2 ** attempt)
                            logger.warning(
                                "[%s] offset=%d 429 — backing off %.0fs",
                                month_key, offset, wait,
                            )
                            await asyncio.sleep(wait)

                        elif resp.status == 403:
                            logger.error(
                                "[%s] offset=%d 403 Forbidden — skipping page",
                                month_key, offset,
                            )
                            return []

                        elif resp.status >= 500:
                            wait = self.config.backoff_base * (2 ** attempt)
                            logger.warning(
                                "[%s] offset=%d HTTP %d — retry %d in %.1fs",
                                month_key, offset, resp.status, attempt + 1, wait,
                            )
                            await asyncio.sleep(wait)

                        else:
                            logger.warning(
                                "[%s] offset=%d unexpected HTTP %d",
                                month_key, offset, resp.status,
                            )
                            return []

                except asyncio.TimeoutError:
                    wait = self.config.backoff_base * (1.5 ** attempt)
                    logger.warning(
                        "[%s] offset=%d timeout — retry %d in %.1fs",
                        month_key, offset, attempt + 1, wait,
                    )
                    await asyncio.sleep(wait)

                except aiohttp.ClientError as exc:
                    logger.warning("[%s] offset=%d ClientError: %s", month_key, offset, exc)
                    await asyncio.sleep(2)

        if not rows:
            logger.warning(
                "[%s] offset=%d — no rows after %d attempts",
                month_key, offset, self.config.retry_limit + 1,
            )
            return []

        # 3. Validate rownum monotonicity
        rownums = [r["rownum"] for r in rows]
        for i in range(1, len(rownums)):
            if rownums[i] <= rownums[i - 1]:
                logger.warning(
                    "[%s] offset=%d non-monotonic rownum at pos %d: %d → %d",
                    month_key, offset, i, rownums[i - 1], rownums[i],
                )
                break  # log first occurrence per page only

        # 4. Deduplicate within page
        seen:    set[int]   = set()
        deduped: list[dict] = []
        for r in rows:
            if r["id"] not in seen:
                seen.add(r["id"])
                deduped.append(r)
        if len(deduped) < len(rows):
            logger.warning(
                "[%s] offset=%d deduplicated %d → %d rows",
                month_key, offset, len(rows), len(deduped),
            )

        # 5. Cache
        await self._cache.set_page(month_key, offset, deduped)
        return deduped

    def _extract_rows(self, data: Any, month_key: str, offset: int) -> list[dict]:
        """Parse raw API response → list of {id: int, rownum: int}."""
        if not data:
            return []

        raw_rows: list = []
        if isinstance(data, dict):
            raw_rows = data.get("data") or data.get("aaData") or []
        elif isinstance(data, list):
            raw_rows = data

        if not isinstance(raw_rows, list):
            logger.warning("[%s] offset=%d malformed response — skipping", month_key, offset)
            return []

        result: list[dict] = []
        for row in raw_rows:
            if not isinstance(row, dict):
                continue
            try:
                cid    = int(row.get("id") or row.get("channelId") or 0)
                rownum = int(row.get("rownum") or row.get("rank") or 0)
                if cid > 0 and rownum > 0:
                    result.append({"id": cid, "rownum": rownum})
            except (TypeError, ValueError):
                continue
        return result

    # ── Phased pagination ─────────────────────────────────────────────────────

    async def get_ranks_for_month(self, month_key: str) -> dict[int, int]:
        """
        Build {channel_id → rank} for one month.

        Phases:
          0: offsets   0–500    (guaranteed top-500 coverage)
          1: offsets 500–2000
          2: offsets 2000–5000
          3: offsets 5000–10000

        Early stop when:
          a) all targets found
          b) N consecutive pages yield zero new targets
          c) per-month or global API budget exceeded

        Crash recovery: resumes from saved offset if a previous run was
        interrupted mid-month.
        """
        t0        = time.monotonic()
        remaining = set(self._target_set)
        index:    dict[int, int] = {}
        metrics   = MonthMetrics(month_key=month_key, total_targets=len(self._target_set))

        # Pre-load from Redis rankmap — ONE HGETALL call replaces 2,429 GETs.
        # On a warm re-run (all channels already cached) this skips all pagination.
        if self._cache.available:
            cached_map = await self._cache.get_rankmap(month_key)
            pre_found = [cid for cid in list(remaining) if cid in cached_map]
            for cid in pre_found:
                index[cid] = cached_map[cid]
                remaining.discard(cid)
            if pre_found:
                await metrics.inc_cache_hits(len(pre_found))
                logger.info(
                    "[%s] pre-loaded %d/%d ranks via HGETALL (1 Redis call)",
                    month_key, len(pre_found), len(self._target_set),
                )

        # Resume from saved offset (crash recovery)
        resume_from = self._progress.resume_offset(month_key)
        if resume_from:
            logger.info("[%s] Resuming from offset %d", month_key, resume_from)

        # ── Single counter, incremented exactly once per page  (Bug 1 fix) ───
        consecutive_no_new = 0

        for phase_idx, (phase_start, phase_end) in enumerate(self.config.phases):
            consecutive_no_new = 0 
            if not remaining:
                metrics.stop_reason          = "all_found"
                metrics.early_stop_triggered = True
                break

            if metrics.api_calls >= self.config.max_api_calls_per_month:
                metrics.stop_reason          = "per_month_budget"
                metrics.early_stop_triggered = True
                logger.warning("[%s] per-month budget reached", month_key)
                break

            effective_start = max(phase_start, resume_from)
            effective_end   = min(phase_end, self.config.max_rank_threshold)
            if effective_start >= effective_end:
                continue

            phase_offsets = list(range(effective_start, effective_end, self.config.page_size))
            logger.info(
                "[%s] Phase %d  offsets %d–%d  pages=%d  remaining=%d",
                month_key, phase_idx,
                effective_start, effective_end,
                len(phase_offsets), len(remaining),
            )

            for batch_start in range(0, len(phase_offsets), self.config.concurrency):
                batch = phase_offsets[batch_start: batch_start + self.config.concurrency]

                if metrics.api_calls >= self.config.max_api_calls_per_month:
                    metrics.stop_reason          = "per_month_budget"
                    metrics.early_stop_triggered = True
                    break

                pages: list[list[dict]] = await asyncio.gather(*[
                    asyncio.create_task(self.fetch_page(month_key, off, metrics))
                    for off in batch
                ])

                # ── Snapshot remaining BEFORE processing (needed for batch_new diff) ─
                remaining_before_batch = set(remaining)

                # ── Process pages — one counter increment per page (Bug 1 fix) ─
                for page_rows in pages:
                    if not page_rows:
                        consecutive_no_new += 1
                        continue

                    found_in_page = 0
                    for entry in page_rows:
                        cid    = entry["id"]
                        rownum = entry["rownum"]
                        if cid in remaining:
                            index[cid] = rownum
                            remaining.discard(cid)
                            found_in_page += 1

                    if found_in_page == 0:
                        consecutive_no_new += 1
                    else:
                        consecutive_no_new = 0  # reset on any discovery

                    if not remaining:
                        break

                # ── Pipelined rankmap write: only channels found in THIS batch ─
                # Use the before/after diff so we never re-write previously
                # cached entries (Bug 1 fix — batch_new was growing every batch).
                newly_found_ids = remaining_before_batch - remaining
                if newly_found_ids:
                    batch_new = {cid: index[cid] for cid in newly_found_ids}
                    await self._cache.set_rankmap_batch(month_key, batch_new, merge=True)

                # ── Save progress after every batch (Bug 2 fix) ───────────────
                last_offset = batch[-1] + self.config.page_size
                await self._progress.save_offset(month_key, last_offset)

                if not remaining:
                    metrics.stop_reason          = "all_found"
                    metrics.early_stop_triggered = True
                    break

                if consecutive_no_new >= self.config.early_stop_pages:
                    metrics.stop_reason = (
                        f"no_new_targets_in_{consecutive_no_new}_pages"
                    )
                    metrics.early_stop_triggered = True
                    logger.info(
                        "[%s] Early stop — %d consecutive pages with no new targets",
                        month_key, consecutive_no_new,
                    )
                    break

            if metrics.early_stop_triggered:
                break

        await self._progress.mark_month_done(month_key)

        # Write complete final rankmap (merge=False = fresh authoritative copy).
        # Next incremental run loads this in 1 HGETALL and skips all pagination.
        if index:
            await self._cache.set_rankmap_batch(month_key, index, merge=False)
            logger.info(
                "[%s] Wrote final rankmap to Redis (%d entries)", month_key, len(index)
            )

        metrics.channels_found = len(index)
        metrics.elapsed_s      = time.monotonic() - t0
        metrics.log()
        self._metrics.append(metrics)
        return index

    # ── Month orchestration ────────────────────────────────────────────────────

    async def run_all_months(self, force: bool = False) -> None:
        """
        Iterate over all month_keys sequentially.
        Each month is internally parallelised via concurrency semaphore.

        Args:
            force: reprocess months already marked complete in ProgressStore.
        """
        total = len(self.month_keys)
        logger.info(
            "Phase 4 — RankIndexer starting | %d months × %d target channels",
            total, len(self.target_channels),
        )
        t0 = time.time()

        for i, month_key in enumerate(self.month_keys, 1):
            logger.info("═══ [%d/%d] %s ═══", i, total, month_key)

            if not force and self._progress.is_month_done(month_key):
                logger.info("[%s] Already complete — loading from Parquet", month_key)
                reloaded = self._load_month_from_parquet(month_key)

                # Bug 3 fix: if Parquet is missing or unreadable (e.g. pyarrow
                # not installed, partition deleted), fall back to Redis rankmap
                # before giving up with the error sentinel.
                if reloaded is None and self._cache.available:
                    logger.info(
                        "[%s] Parquet unavailable — trying Redis rankmap fallback",
                        month_key,
                    )
                    redis_map = await self._cache.get_rankmap(month_key)
                    reloaded = redis_map if redis_map else None

                self._index[month_key] = (
                    reloaded if reloaded is not None else _PARQUET_RELOAD_FAILED
                )
                if reloaded is None:
                    logger.warning(
                        "[%s] Could not reload from Parquet or Redis — "
                        "channels will show status='error'. Re-run with --force "
                        "to reprocess this month.",
                        month_key,
                    )
                continue

            try:
                self._index[month_key] = await self.get_ranks_for_month(month_key)
            except Exception as exc:
                logger.exception("[%s] Fatal error — %s", month_key, exc)
                self._index[month_key] = {}

        elapsed_min = (time.time() - t0) / 60
        total_found = sum(
            len(v) for v in self._index.values() if isinstance(v, dict)
        )
        logger.info(
            "Phase 4 done — %d months | %d rank entries | "
            "global API calls=%d | elapsed=%.1f min",
            total, total_found, self._global_api_calls, elapsed_min,
        )

    def _load_month_from_parquet(self, month_key: str) -> Optional[dict[int, int]]:
        """
        Reconstruct {channel_id: rank} from a saved Parquet partition.
        Returns None on any failure so the caller can use the sentinel.

        Bug 5 fix: uses vectorized dict(zip(...)) instead of iterrows().
        """
        if not HAS_PARQUET:
            return None
        pdir = self.config.parquet_dir / f"month_key={month_key}"
        if not pdir.exists():
            logger.warning("[%s] Parquet partition not found at %s", month_key, pdir)
            return None
        try:
            table = pq.read_table(str(pdir))
            df    = table.to_pandas()
            valid = df[df["rank"].notna() & (df["rank"] > 0)]
            return dict(zip(valid["channel_id"].astype(int), valid["rank"].astype(int)))
        except Exception as exc:
            logger.warning("[%s] Parquet reload failed: %s", month_key, exc)
            return None

    # ── DataFrame + storage ───────────────────────────────────────────────────

    def build_panel_dataframe(self) -> "pd.DataFrame":
        """
        Convert _index → tidy panel DataFrame.

        Schema
        ──────
        channel_id : int64
        year       : int64
        month      : str          "january" … "december"
        rank       : Int64        nullable (pd.NA when not found)
        status     : str          "found" | "not_found_within_threshold" | "error"

        Sorted by (channel_id, year, month_ordinal).
        Validated: no duplicates, rank > 0 when present.

        Bug 4 fix: uses _PARQUET_RELOAD_FAILED sentinel to emit correct status
        for months that were skipped (incremental) but couldn't be reloaded.
        """
        if not HAS_PANDAS:
            raise ImportError("pandas is required — pip install pandas")

        MONTH_ORDER = {m: i for i, m in enumerate([
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december",
        ])}

        rows: list[dict] = []

        for month_key in self.month_keys:
            year_int  = int(month_key[:4])
            month_str = month_key[4:]
            entry     = self._index.get(month_key)

            for cid in self.target_channels:
                if entry is _PARQUET_RELOAD_FAILED or entry is None:
                    rank, status = None, "error"
                elif isinstance(entry, dict):
                    rank = entry.get(cid)
                    status = "found" if rank is not None else "not_found_within_threshold"
                else:
                    rank, status = None, "error"

                rows.append({
                    "channel_id": cid,
                    "year":       year_int,
                    "month":      month_str,
                    "rank":       rank,
                    "status":     status,
                })

        df = pd.DataFrame(rows)
        df["rank"] = pd.array(df["rank"], dtype=pd.Int64Dtype())

        df["_mo"] = df["month"].map(MONTH_ORDER)
        df.sort_values(["channel_id", "year", "_mo"], inplace=True)
        df.drop(columns=["_mo"], inplace=True)
        df.reset_index(drop=True, inplace=True)

        # Validation
        dupes = df.duplicated(subset=["channel_id", "year", "month"]).sum()
        if dupes:
            logger.error("VALIDATION: %d duplicate (channel_id, year, month) rows", dupes)

        bad_rank = (df["rank"].notna() & (df["rank"] <= 0)).sum()
        if bad_rank:
            logger.error("VALIDATION: %d rows with rank ≤ 0", bad_rank)

        n_found     = (df["status"] == "found").sum()
        n_not_found = (df["status"] == "not_found_within_threshold").sum()
        n_error     = (df["status"] == "error").sum()
        n_total     = len(df)
        logger.info(
            "Panel built — %d rows | found=%.1f%% | not_found=%.1f%% | error=%.1f%%",
            n_total,
            n_found     / n_total * 100,
            n_not_found / n_total * 100,
            n_error     / n_total * 100,
        )

        return df

    def save(self, df: "pd.DataFrame") -> None:
        """Write tidy panel to CSV + partitioned Parquet."""
        cfg = self.config
        cfg.output_dir.mkdir(parents=True, exist_ok=True)

        csv_path = cfg.output_dir / "twitch_global_ranks.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8")
        logger.info("Saved CSV → %s  (%d rows)", csv_path, len(df))

        if HAS_PARQUET:
            cfg.parquet_dir.mkdir(parents=True, exist_ok=True)
            df_pq = df.copy()
            df_pq["month_key"] = df_pq["year"].astype(str) + df_pq["month"]
            table = pa.Table.from_pandas(df_pq, preserve_index=False)
            pq.write_to_dataset(
                table,
                root_path=str(cfg.parquet_dir),
                partition_cols=["month_key"],
                existing_data_behavior="overwrite_or_ignore",
            )
            logger.info("Saved Parquet → %s  (partitioned by month_key)", cfg.parquet_dir)
        else:
            logger.warning("pyarrow not installed — Parquet skipped")

    # ── Summary ────────────────────────────────────────────────────────────────

    def print_summary(self) -> None:
        if not self._metrics:
            return
        total_api   = sum(m.api_calls  for m in self._metrics)
        total_cache = sum(m.cache_hits for m in self._metrics)
        avg_cov     = sum(m.coverage   for m in self._metrics) / len(self._metrics)
        early_stops = sum(1 for m in self._metrics if m.early_stop_triggered)
        total_s     = sum(m.elapsed_s  for m in self._metrics)

        logger.info("═" * 64)
        logger.info("  Phase 4 Summary")
        logger.info("  Months processed : %d", len(self._metrics))
        logger.info("  Total API calls  : %d  (global: %d)", total_api, self._global_api_calls)
        logger.info("  Cache hits       : %d", total_cache)
        logger.info("  Avg coverage     : %.1f%%", avg_cov * 100)
        logger.info("  Early stops      : %d / %d months", early_stops, len(self._metrics))
        logger.info("  Total elapsed    : %.1f min", total_s / 60)
        logger.info("═" * 64)
