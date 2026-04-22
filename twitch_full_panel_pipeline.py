
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import aiohttp
import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PARQUET = True
except Exception:
    HAS_PARQUET = False

MONTHS = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]
MONTH_TO_NUM = {m: i + 1 for i, m in enumerate(MONTHS)}

logger = logging.getLogger("twitch_full_panel_pipeline")


def setup_logging(level: str) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)
    Path("logs").mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler("logs/twitch_full_panel_pipeline.log", encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


def month_key(year: int, month_name: str) -> str:
    return f"{int(year)}{str(month_name).strip().lower()}"


def normalize_slug(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip().lower()


def slugify_display_name(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    s = str(value).strip().lower()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    return s


def safe_int(value: Any) -> Optional[int]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def safe_float(value: Any) -> Optional[float]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    try:
        return float(value)
    except Exception:
        return None


def month_order(month_name: str) -> int:
    return MONTH_TO_NUM[str(month_name).strip().lower()]


def make_month_keys(years: list[int], months: list[str]) -> list[str]:
    keys: list[str] = []
    for y in sorted(set(int(v) for v in years)):
        for m in sorted(set(str(v).strip().lower() for v in months), key=lambda x: MONTH_TO_NUM[x]):
            keys.append(f"{y}{m}")
    return keys


@dataclass
class PipelineConfig:
    input_csv: Path
    output_dir: Path = Path("output")
    years: list[int] = field(default_factory=lambda: [2022, 2023, 2024, 2025])
    months: list[str] = field(default_factory=lambda: MONTHS.copy())
    leaderboard_base_url: str = "https://sullygnome.com"
    channel_api_base_url: str = "https://sullygnome.com/api/channels"
    page_size: int = 100
    max_rank_threshold: int = 20_000
    concurrency_shallow: int = 8
    concurrency_deep: int = 5
    request_timeout_s: int = 30
    retry_limit: int = 4
    backoff_base_s: float = 2.0
    jitter_min_s: float = 0.10
    jitter_max_s: float = 0.40
    early_stop_pages: int = 10
    max_api_calls_per_month: int = 250
    max_api_calls_global: int = 15_000
    use_cache: bool = True
    cache_db_path: Path = Path("cache/api_cache.sqlite3")
    progress_path: Path = Path("checkpoints/rank_progress.json")
    write_parquet: bool = True
    phases: list[tuple[int, int]] = field(default_factory=lambda: [
        (0, 500), (500, 2_000), (2_000, 5_000), (5_000, 10_000), (10_000, 20_000)
    ])

    def __post_init__(self) -> None:
        self.output_dir = Path(self.output_dir)
        self.cache_db_path = Path(self.cache_db_path)
        self.progress_path = Path(self.progress_path)


class SQLiteCache:
    def __init__(self, db_path: Path, enabled: bool = True) -> None:
        self.db_path = Path(db_path)
        self.enabled = enabled
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = asyncio.Lock()

    def connect_sync(self) -> None:
        if not self.enabled:
            return
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS api_cache (cache_key TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at REAL NOT NULL, ttl INTEGER NOT NULL)"
        )
        self._conn.commit()

    async def get(self, key: str) -> Optional[dict[str, Any]]:
        if not self.enabled or self._conn is None:
            return None
        async with self._lock:
            cur = self._conn.execute(
                "SELECT payload, created_at, ttl FROM api_cache WHERE cache_key = ?",
                (key,),
            )
            row = cur.fetchone()
            if not row:
                return None
            payload, created_at, ttl = row
            if (time.time() - float(created_at)) > float(ttl):
                self._conn.execute("DELETE FROM api_cache WHERE cache_key = ?", (key,))
                self._conn.commit()
                return None
            try:
                return json.loads(payload)
            except Exception:
                return None

    async def set(self, key: str, value: dict[str, Any], ttl_seconds: int) -> None:
        if not self.enabled or self._conn is None:
            return
        async with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO api_cache(cache_key, payload, created_at, ttl) VALUES (?, ?, ?, ?)",
                (key, json.dumps(value, ensure_ascii=False), time.time(), int(ttl_seconds)),
            )
            self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


class ProgressStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._data: dict[str, Any] = {"completed": [], "offsets": {}}
        self._lock = asyncio.Lock()
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                self._data = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception as exc:
                logger.warning("ProgressStore corrupt: %s", exc)
                self._data = {"completed": [], "offsets": {}}

    def is_done(self, month: str) -> bool:
        return month in set(self._data.get("completed", []))

    def resume_offset(self, month: str) -> int:
        return int(self._data.get("offsets", {}).get(month, 0) or 0)

    async def save_offset(self, month: str, offset: int) -> None:
        async with self._lock:
            self._data.setdefault("offsets", {})[month] = int(offset)
            self._write()

    async def mark_done(self, month: str) -> None:
        async with self._lock:
            completed = self._data.setdefault("completed", [])
            if month not in completed:
                completed.append(month)
            self._data.get("offsets", {}).pop(month, None)
            self._write()

    def _write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, indent=2), encoding="utf-8")
        tmp.replace(self.path)


@dataclass
class MasterData:
    df: pd.DataFrame
    channel_map: dict[int, str]
    display_name_map: dict[int, str]
    csv_rank_map: dict[tuple[int, str], int]
    csv_metrics_map: dict[tuple[int, str], dict[str, Any]]
    month_keys: list[str]


def load_master_csv(path: Path) -> MasterData:
    df = pd.read_csv(path)

    required = {
        "year", "month", "channel_id", "channel_slug", "display_name", "rank_position",
        "followers", "followers_gained", "average_viewers", "peak_viewers",
        "hours_streamed", "hours_watched", "status", "mature", "language", "created",
        "peak_viewer_rank", "avg_viewer_rank", "follower_rank", "follower_gain_rank",
        "games_by_avg_viewers_json", "games_by_stream_time_json", "all_games_json",
        "top_game", "top_game_hours", "top_game_pct", "game_count", "is_variety_streamer"
    }
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Input CSV missing columns: {sorted(missing)}")

    df["channel_id"] = df["channel_id"].apply(safe_int)
    df = df[df["channel_id"].notna()].copy()
    df["channel_id"] = df["channel_id"].astype(int)
    df["channel_slug"] = df["channel_slug"].map(normalize_slug)
    df["display_name"] = df["display_name"].fillna("").astype(str)
    df["month_key"] = df.apply(lambda r: month_key(int(r["year"]), str(r["month"])), axis=1)

    channels = (
        df[["channel_id", "channel_slug", "display_name"]]
        .drop_duplicates(subset=["channel_id"], keep="first")
        .sort_values("channel_id")
    )
    channel_map = dict(zip(channels["channel_id"], channels["channel_slug"]))
    display_name_map = dict(zip(channels["channel_id"], channels["display_name"]))
    month_keys = sorted(df["month_key"].unique().tolist(), key=lambda mk: (int(mk[:4]), MONTH_TO_NUM[mk[4:]]))

    csv_rank_map: dict[tuple[int, str], int] = {}
    csv_metrics_map: dict[tuple[int, str], dict[str, Any]] = {}
    for row in df.itertuples(index=False):
        mk = month_key(int(row.year), str(row.month))
        cid = int(row.channel_id)
        rank = safe_int(getattr(row, "rank_position", None))
        if rank is not None and rank > 0:
            csv_rank_map[(cid, mk)] = rank
        csv_metrics_map[(cid, mk)] = {
            "average_viewers": safe_float(getattr(row, "average_viewers", None)),
            "hours_watched": safe_float(getattr(row, "hours_watched", None)),
            "hours_streamed": safe_float(getattr(row, "hours_streamed", None)),
            "followers": safe_int(getattr(row, "followers", None)),
            "followers_gained": safe_int(getattr(row, "followers_gained", None)),
            "peak_viewers": safe_int(getattr(row, "peak_viewers", None)),
            "top_game": getattr(row, "top_game", None),
            "top_game_hours": safe_float(getattr(row, "top_game_hours", None)),
            "top_game_pct": safe_float(getattr(row, "top_game_pct", None)),
            "game_count": safe_int(getattr(row, "game_count", None)),
            "is_variety_streamer": safe_int(getattr(row, "is_variety_streamer", None)),
            "language": getattr(row, "language", None),
            "created": getattr(row, "created", None),
            "mature": getattr(row, "mature", None),
            "peak_viewer_rank": safe_int(getattr(row, "peak_viewer_rank", None)),
            "avg_viewer_rank": safe_int(getattr(row, "avg_viewer_rank", None)),
            "follower_rank": safe_int(getattr(row, "follower_rank", None)),
            "follower_gain_rank": safe_int(getattr(row, "follower_gain_rank", None)),
            "source": "csv",
        }

    logger.info("Loaded master CSV rows=%d channels=%d months=%d", len(df), len(channel_map), len(month_keys))
    return MasterData(df, channel_map, display_name_map, csv_rank_map, csv_metrics_map, month_keys)


def extract_leaderboard_metrics_from_row(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize one leaderboard row into the panel schema."""
    viewminutes = safe_float(row.get("viewminutes"))
    streamedminutes = safe_float(row.get("streamedminutes"))

    metrics = {
        "average_viewers": safe_float(row.get("avgviewers")),
        "hours_watched": (viewminutes / 60.0) if viewminutes is not None else None,
        "hours_streamed": (streamedminutes / 60.0) if streamedminutes is not None else None,
        "followers": safe_int(row.get("followers")),
        "followers_gained": safe_int(row.get("followersgained")),
        "peak_viewers": safe_int(row.get("maxviewers")),
        "language": row.get("language"),
        "display_name": row.get("displayname") or row.get("display_name"),
        "channel_slug": row.get("url") or row.get("channel_slug"),
        "source": "leaderboard",
    }
    if not any(v is not None for k, v in metrics.items() if k != "source"):
        return {}
    return metrics


def extract_rows_from_leaderboard_response(data: Any) -> list[dict[str, Any]]:
    if not data:
        return []
    rows: list[Any] = []
    if isinstance(data, dict):
        for k in ("data", "aaData", "rows", "results", "items"):
            if isinstance(data.get(k), list):
                rows = data[k]
                break
    elif isinstance(data, list):
        rows = data
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        cid = safe_int(row.get("id") or row.get("channelId") or row.get("channel_id"))
        rank = safe_int(row.get("rownum") or row.get("rank") or row.get("position") or row.get("rank_position"))
        if cid is None or rank is None or cid <= 0 or rank <= 0:
            continue
        out.append({
            "channel_id": cid,
            "rank": rank,
            **extract_leaderboard_metrics_from_row(row),
        })
    return out



def extract_metrics_from_channel_response(data: Any) -> dict[str, Any]:
    if not data:
        return {}
    if isinstance(data, list):
        if not data:
            return {}
        if len(data) == 1 and isinstance(data[0], dict):
            data = data[0]
        else:
            return {}
    if not isinstance(data, dict):
        return {}

    payload = data
    for key in ("data", "results", "result", "channel", "items"):
        if isinstance(data.get(key), dict):
            payload = data[key]
            break

    def pick(*keys: str) -> Any:
        for k in keys:
            if k in payload:
                return payload[k]
        return None

    metrics = {
        "average_viewers": safe_float(pick("average_viewers", "avg_viewers", "avgViewers")),
        "hours_watched": safe_float(pick("hours_watched", "hoursWatched", "watch_time")),
        "hours_streamed": safe_float(pick("hours_streamed", "hoursStreamed", "stream_time", "streamTime")),
        "followers": safe_int(pick("followers", "follower_count", "followerCount")),
        "followers_gained": safe_int(pick("followers_gained", "followersGained", "new_followers")),
        "peak_viewers": safe_int(pick("peak_viewers", "peakViewers")),
        "top_game": pick("top_game", "topGame", "game"),
        "top_game_hours": safe_float(pick("top_game_hours", "topGameHours")),
        "top_game_pct": safe_float(pick("top_game_pct", "topGamePct")),
        "game_count": safe_int(pick("game_count", "gameCount")),
        "is_variety_streamer": safe_int(pick("is_variety_streamer", "isVarietyStreamer")),
        "language": pick("language", "lang"),
        "created": pick("created", "created_at", "createdAt"),
        "mature": pick("mature"),
        "peak_viewer_rank": safe_int(pick("peak_viewer_rank", "peakViewerRank")),
        "avg_viewer_rank": safe_int(pick("avg_viewer_rank", "avgViewerRank")),
        "follower_rank": safe_int(pick("follower_rank", "followerRank")),
        "follower_gain_rank": safe_int(pick("follower_gain_rank", "followerGainRank")),
        "source": "api",
    }
    if not any(v is not None for k, v in metrics.items() if k != "source"):
        return {}
    return metrics


@dataclass
class MonthStats:
    month_key: str
    total_targets: int
    channels_found: int = 0
    api_calls: int = 0
    cache_hits: int = 0
    early_stop_triggered: bool = False
    stop_reason: str = ""
    elapsed_s: float = 0.0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    @property
    def coverage(self) -> float:
        return self.channels_found / self.total_targets if self.total_targets else 0.0

    async def inc_api_calls(self, n: int = 1) -> None:
        async with self._lock:
            self.api_calls += n

    async def inc_cache_hits(self, n: int = 1) -> None:
        async with self._lock:
            self.cache_hits += n


class RankIndexer:
    def __init__(self, target_channels: list[int], month_keys: list[str], config: PipelineConfig, cache: SQLiteCache, progress: ProgressStore) -> None:
        self.target_channels = target_channels
        self.target_set = set(target_channels)
        self.month_keys = month_keys
        self.config = config
        self.cache = cache
        self.progress = progress
        self._session: Optional[aiohttp.ClientSession] = None
        self._sem = asyncio.Semaphore(config.concurrency_shallow)
        self._global_lock = asyncio.Lock()
        self._global_api_calls = 0
        self._stats: list[MonthStats] = []

    async def __aenter__(self) -> "RankIndexer":
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self.config.concurrency_shallow, limit_per_host=self.config.concurrency_shallow, ttl_dns_cache=300),
            timeout=aiohttp.ClientTimeout(total=self.config.request_timeout_s),
            headers=self._headers(),
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            await self._session.close()

    @staticmethod
    def _headers() -> dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": "https://sullygnome.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }

    async def _consume_budget(self) -> bool:
        async with self._global_lock:
            self._global_api_calls += 1
            return self._global_api_calls <= self.config.max_api_calls_global

    def _endpoint(self, month: str, offset: int) -> str:
        return f"{self.config.leaderboard_base_url}/api/tables/channeltables/getchannels/{month}/0/1/3/desc/{offset}/{self.config.page_size}"

    async def fetch_page(self, month: str, offset: int, stats: MonthStats) -> list[dict[str, Any]]:
        key = f"leaderboard:{month}:{offset}"
        if self.config.use_cache:
            cached = await self.cache.get(key)
            if cached is not None:
                await stats.inc_cache_hits()
                return cached.get("rows", [])

        if not await self._consume_budget():
            return []

        assert self._session is not None
        url = self._endpoint(month, offset)
        if 8_000 <= offset <= 14_000:
            retries = self.config.retry_limit
        elif 14_000 < offset <= 20_000:
            retries = 2
        else:
            retries = 1

        async with self._sem:
            rows: list[dict[str, Any]] = []
            for attempt in range(retries + 1):
                try:
                    await asyncio.sleep(self.config.jitter_min_s + (self.config.jitter_max_s - self.config.jitter_min_s) * 0.5)
                    async with self._session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            rows = extract_rows_from_leaderboard_response(data)
                            await stats.inc_api_calls()
                            break
                        if resp.status == 429:
                            await asyncio.sleep(self.config.backoff_base_s * (2 ** attempt))
                            continue
                        if resp.status == 403:
                            rows = []
                            break
                        if resp.status >= 500:
                            await asyncio.sleep(self.config.backoff_base_s * (2 ** attempt))
                            continue
                        rows = []
                        break
                except asyncio.TimeoutError:
                    await asyncio.sleep(self.config.backoff_base_s * (1.5 ** attempt))
                except aiohttp.ClientError:
                    await asyncio.sleep(2.0)

            if self.config.use_cache:
                await self.cache.set(key, {"rows": rows}, ttl_seconds=24 * 3600)
            return rows

    async def get_ranks_for_month(
        self,
        month: str,
        seed_rank_map: dict[int, int],
        seed_metrics_map: dict[int, dict[str, Any]],
    ) -> tuple[dict[int, int], dict[int, dict[str, Any]]]:
        start = time.monotonic()
        remaining = set(self.target_set)
        rank_map: dict[int, int] = {}
        metrics_map: dict[int, dict[str, Any]] = {}
        stats = MonthStats(month, len(self.target_set))

        for cid, rank in seed_rank_map.items():
            if cid in remaining and rank is not None and rank > 0:
                rank_map[cid] = int(rank)
                remaining.discard(cid)

        for cid, metrics in seed_metrics_map.items():
            metrics_map[cid] = dict(metrics)

        if rank_map:
            await stats.inc_cache_hits(len(rank_map))

        if not remaining:
            stats.channels_found = len(rank_map)
            stats.elapsed_s = time.monotonic() - start
            self._stats.append(stats)
            return rank_map, metrics_map

        consecutive_no_new = 0
        current_concurrency = self.config.concurrency_shallow
        resume_from = self.progress.resume_offset(month)
        recent_batch_news: list[int] = []

        for phase_idx, (phase_start, phase_end) in enumerate(self.config.phases):
            if not remaining:
                break

            effective_start = max(phase_start, resume_from)
            effective_end = min(phase_end, self.config.max_rank_threshold)
            if effective_start >= effective_end:
                continue

            offsets = list(range(effective_start, effective_end, self.config.page_size))
            if phase_start >= 10_000:
                offsets = list(reversed(offsets))

            logger.info("[%s] phase=%d offsets=%d remaining=%d", month, phase_idx, len(offsets), len(remaining))
            phase_batches = 0
            phase_pages = 0
            phase_rows = 0
            phase_hits = 0
            batch_start = 0

            while batch_start < len(offsets):
                batch = offsets[batch_start: batch_start + self.config.concurrency_shallow]
                if not batch:
                    break

                phase_batches += 1

                if stats.api_calls >= self.config.max_api_calls_per_month:
                    stats.early_stop_triggered = True
                    stats.stop_reason = "per_month_budget"
                    break

                min_offset = min(batch)
                target_concurrency = self.config.concurrency_shallow if min_offset <= 8_000 else self.config.concurrency_deep
                if target_concurrency != current_concurrency:
                    self._sem = asyncio.Semaphore(target_concurrency)
                    current_concurrency = target_concurrency

                before = set(remaining)
                pages = await asyncio.gather(*[self.fetch_page(month, off, stats) for off in batch])

                batch_page_count = 0
                batch_nonempty = 0
                batch_rows = 0
                batch_new = 0
                batch_all_empty = True

                for i, page_rows in enumerate(pages):
                    off = batch[i]
                    batch_page_count += 1
                    if not page_rows:
                        if off <= 10_000:
                            consecutive_no_new += 1
                        continue

                    batch_all_empty = False
                    batch_nonempty += 1
                    batch_rows += len(page_rows)
                    found = 0

                    for row in page_rows:
                        cid = int(row["channel_id"])
                        rank = int(row["rank"])

                        metrics = {
                            "average_viewers": row.get("average_viewers"),
                            "hours_watched": row.get("hours_watched"),
                            "hours_streamed": row.get("hours_streamed"),
                            "followers": row.get("followers"),
                            "followers_gained": row.get("followers_gained"),
                            "peak_viewers": row.get("peak_viewers"),
                            "language": row.get("language"),
                            "display_name": row.get("display_name"),
                            "channel_slug": row.get("channel_slug"),
                            "source": "leaderboard",
                        }
                        if any(v is not None for k, v in metrics.items() if k != "source"):
                            metrics_map[cid] = metrics

                        if cid in remaining:
                            rank_map[cid] = rank
                            remaining.discard(cid)
                            found += 1

                        if rank is not None and metrics.get("average_viewers") is None:
                            logger.debug("[BUG] Rank found but metrics missing for %s (%s)", cid, month)

                    batch_new += found
                    consecutive_no_new = 0 if found else consecutive_no_new + 1

                phase_pages += batch_page_count
                phase_rows += batch_rows
                phase_hits += batch_new
                recent_batch_news.append(batch_new)
                if len(recent_batch_news) > 5:
                    recent_batch_news.pop(0)

                density = (batch_rows / batch_page_count) if batch_page_count else 0.0
                logger.info(
                    "[%s] phase=%d batch=%d pages=%d nonempty=%d rows=%d hit_density=%.1f new=%d remaining=%d",
                    month,
                    phase_idx,
                    phase_batches,
                    batch_page_count,
                    batch_nonempty,
                    batch_rows,
                    density,
                    batch_new,
                    len(remaining),
                )

                newly_found = before - remaining
                if newly_found and self.config.use_cache:
                    await self.cache.set(
                        f"rankmap:{month}",
                        {"rows": {str(cid): rank_map[cid] for cid in newly_found}},
                        ttl_seconds=7 * 24 * 3600,
                    )

                await self.progress.save_offset(month, batch[-1] + self.config.page_size)

                if phase_idx >= 4 and batch_all_empty and phase_batches >= 3:
                    jump_pages = max(20, self.config.concurrency_shallow * 3)
                    batch_start += jump_pages
                    consecutive_no_new = 0
                    logger.info("[%s] phase=%d dead-zone jump ahead by %d pages", month, phase_idx, jump_pages)
                    continue

                if phase_batches > 3:
                    threshold = self.config.early_stop_pages + (10 if min_offset > 10_000 else 0)
                    if consecutive_no_new >= threshold:
                        stats.early_stop_triggered = True
                        stats.stop_reason = f"no_new_targets_in_{consecutive_no_new}_pages"
                        logger.info(
                            "[%s] early stop — %d consecutive pages with no new targets (threshold=%d, offset=%d)",
                            month, consecutive_no_new, threshold, min_offset,
                        )
                        break

                if phase_idx >= 5 and len(recent_batch_news) >= 3 and sum(recent_batch_news[-3:]) == 0:
                    stats.early_stop_triggered = True
                    stats.stop_reason = "phase5_zero_yield"
                    logger.info("[%s] phase=%d zero-yield stop", month, phase_idx)
                    break

                if phase_idx >= 4 and len(recent_batch_news) >= 5 and sum(recent_batch_news[-5:]) < 3:
                    stats.early_stop_triggered = True
                    stats.stop_reason = "sparse_tail_stop"
                    logger.info("[%s] phase=%d sparse-tail stop", month, phase_idx)
                    break

                if not remaining:
                    break

                batch_start += len(batch)

            logger.info(
                "[%s] phase=%d summary pages=%d rows=%d hits=%d remaining=%d",
                month, phase_idx, phase_pages, phase_rows, phase_hits, len(remaining)
            )

            if stats.early_stop_triggered:
                break

        if remaining:
            recovery_start = 12_000
            recovery_end = min(30_000, self.config.max_rank_threshold)
            recovery_offsets = list(range(recovery_start, recovery_end, self.config.page_size))
            recovery_no_new = 0

            logger.info(
                "[%s] recovery pass — %d missing channels, offsets %d..%d",
                month, len(remaining), recovery_start, recovery_end
            )

            for batch_start in range(0, len(recovery_offsets), self.config.concurrency_shallow):
                if not remaining:
                    break
                if recovery_no_new >= 5:
                    logger.info("[%s] recovery early stop after repeated zero-yield batches", month)
                    break

                batch = recovery_offsets[batch_start: batch_start + self.config.concurrency_shallow]
                before = set(remaining)
                pages = await asyncio.gather(*[self.fetch_page(month, off, stats) for off in batch])

                batch_rows = 0
                batch_new = 0
                for page_rows in pages:
                    batch_rows += len(page_rows)
                    for row in page_rows:
                        cid = int(row["channel_id"])
                        rank = int(row["rank"])
                        metrics = {
                            "average_viewers": row.get("average_viewers"),
                            "hours_watched": row.get("hours_watched"),
                            "hours_streamed": row.get("hours_streamed"),
                            "followers": row.get("followers"),
                            "followers_gained": row.get("followers_gained"),
                            "peak_viewers": row.get("peak_viewers"),
                            "language": row.get("language"),
                            "display_name": row.get("display_name"),
                            "channel_slug": row.get("channel_slug"),
                            "source": "leaderboard",
                        }
                        if any(v is not None for k, v in metrics.items() if k != "source"):
                            metrics_map[cid] = metrics
                        if cid in remaining:
                            rank_map[cid] = rank
                            remaining.discard(cid)
                            batch_new += 1

                recovery_no_new = 0 if batch_new else recovery_no_new + 1
                density = (batch_rows / len(batch)) if batch else 0.0
                logger.info(
                    "[%s] recovery batch=%d pages=%d rows=%d hit_density=%.1f new=%d remaining=%d",
                    month,
                    (batch_start // max(1, self.config.concurrency_shallow)) + 1,
                    len(batch),
                    batch_rows,
                    density,
                    batch_new,
                    len(remaining),
                )

                newly_found = before - remaining
                if newly_found and self.config.use_cache:
                    await self.cache.set(
                        f"rankmap:{month}",
                        {"rows": {str(cid): rank_map[cid] for cid in newly_found}},
                        ttl_seconds=7 * 24 * 3600,
                    )

                await self.progress.save_offset(month, batch[-1] + self.config.page_size)

            logger.info(
                "[%s] recovery pass complete — %d still missing",
                month, len(remaining)
            )

        if self.config.use_cache:
            await self.cache.set(
                f"monthresult:{month}",
                {
                    "ranks": {str(cid): int(rank) for cid, rank in rank_map.items()},
                    "metrics": {str(cid): metrics for cid, metrics in metrics_map.items()},
                },
                ttl_seconds=7 * 24 * 3600,
            )

        await self.progress.mark_done(month)
        stats.channels_found = len(rank_map)
        stats.elapsed_s = time.monotonic() - start
        self._stats.append(stats)

        logger.info(
            "[%s] done found=%d/%d api_calls=%d cache_hits=%d coverage=%.1f%% reason=%s",
            month, stats.channels_found, stats.total_targets, stats.api_calls,
            stats.cache_hits, stats.coverage * 100, stats.stop_reason or "completed"
        )
        return rank_map, metrics_map

    async def run_all_months(self, seed_rank_map: dict[tuple[int, str], int], seed_metrics_map: dict[tuple[int, str], dict[str, Any]], force: bool = False) -> tuple[dict[str, dict[int, int]], dict[str, dict[int, dict[str, Any]]]]:
        result_ranks: dict[str, dict[int, int]] = {}
        result_metrics: dict[str, dict[int, dict[str, Any]]] = {}
        for i, month in enumerate(self.month_keys, 1):
            logger.info("═══ [%d/%d] %s ═══", i, len(self.month_keys), month)
            if not force and self.progress.is_done(month):
                cached_month = await self.cache.get(f"monthresult:{month}") if self.config.use_cache else None
                if cached_month and isinstance(cached_month.get("ranks"), dict):
                    result_ranks[month] = {int(k): int(v) for k, v in cached_month["ranks"].items()}
                    metrics_blob = cached_month.get("metrics", {})
                    result_metrics[month] = {
                        int(k): dict(v) if isinstance(v, dict) else {}
                        for k, v in metrics_blob.items()
                    }
                    logger.info(
                        "[%s] loaded %d ranks and %d metrics rows from cache",
                        month,
                        len(result_ranks[month]),
                        len(result_metrics[month]),
                    )
                    continue

                cached = await self.cache.get(f"rankmap:{month}") if self.config.use_cache else None
                if cached and isinstance(cached.get("rows"), dict):
                    result_ranks[month] = {int(k): int(v) for k, v in cached["rows"].items()}
                    result_metrics[month] = {}
                    logger.info("[%s] loaded %d ranks from cache", month, len(result_ranks[month]))
                    continue
            month_seed_rank = {cid: rank for (cid, mk), rank in seed_rank_map.items() if mk == month}
            month_seed_metrics = {cid: m for (cid, mk), m in seed_metrics_map.items() if mk == month}
            result_ranks[month], result_metrics[month] = await self.get_ranks_for_month(month, month_seed_rank, month_seed_metrics)
        return result_ranks, result_metrics



@dataclass
class FetchOutcome:
    state: str  # ok | empty | error
    data: Optional[dict[str, Any]] = None
    error: Optional[str] = None


class ChannelDataFetcher:
    def __init__(self, config: PipelineConfig, cache: SQLiteCache, channel_map: dict[int, str], display_name_map: dict[int, str]) -> None:
        self.config = config
        self.cache = cache
        self.channel_map = channel_map
        self.display_name_map = display_name_map
        self._session: Optional[aiohttp.ClientSession] = None
        self._sem = asyncio.Semaphore(config.concurrency_shallow)
        self._global_lock = asyncio.Lock()
        self._global_api_calls = 0
        self._stats = {"api_calls": 0, "cache_hits": 0, "empty": 0, "error": 0}

    async def __aenter__(self) -> "ChannelDataFetcher":
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self.config.concurrency_shallow, limit_per_host=self.config.concurrency_shallow, ttl_dns_cache=300),
            timeout=aiohttp.ClientTimeout(total=self.config.request_timeout_s),
            headers=self._headers(),
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            await self._session.close()

    @staticmethod
    def _headers() -> dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": "https://sullygnome.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }

    async def _consume_budget(self) -> bool:
        async with self._global_lock:
            self._global_api_calls += 1
            return self._global_api_calls <= self.config.max_api_calls_global

    def _url(self, slug: str, month: str) -> str:
        return f"{self.config.channel_api_base_url}/{quote(slug, safe='')}/{month}"

    async def _fetch_url(self, url: str) -> FetchOutcome:
        assert self._session is not None
        for attempt in range(self.config.retry_limit + 1):
            try:
                await asyncio.sleep(self.config.jitter_min_s + (self.config.jitter_max_s - self.config.jitter_min_s) * 0.5)
                async with self._session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        metrics = extract_metrics_from_channel_response(data)
                        if metrics:
                            return FetchOutcome("ok", metrics)
                        return FetchOutcome("empty", None)
                    if resp.status == 404:
                        return FetchOutcome("empty", None)
                    if resp.status == 429:
                        await asyncio.sleep(self.config.backoff_base_s * (2 ** attempt))
                        continue
                    if resp.status >= 500:
                        await asyncio.sleep(self.config.backoff_base_s * (2 ** attempt))
                        continue
                    return FetchOutcome("error", error=f"http_{resp.status}")
            except asyncio.TimeoutError:
                await asyncio.sleep(self.config.backoff_base_s * (1.5 ** attempt))
            except aiohttp.ClientError as exc:
                await asyncio.sleep(2.0)
                last = f"client_error:{exc.__class__.__name__}"
            except Exception as exc:
                return FetchOutcome("error", error=f"unexpected:{exc.__class__.__name__}")
        return FetchOutcome("error", error="retry_exhausted")

    async def fetch_one(self, channel_id: int, channel_slug: str, display_name: str, month: str) -> FetchOutcome:
        normalized = normalize_slug(channel_slug) or slugify_display_name(display_name)
        key = f"channel:{normalized}:{month}"

        if self.config.use_cache:
            cached = await self.cache.get(key)
            if cached is not None:
                self._stats["cache_hits"] += 1
                state = cached.get("state", "empty")
                if state == "ok":
                    return FetchOutcome("ok", cached.get("data") or {})
                if state == "empty":
                    return FetchOutcome("empty", None)
                return FetchOutcome("error", error=cached.get("error"))

        if not await self._consume_budget():
            self._stats["error"] += 1
            return FetchOutcome("error", error="global_api_budget_exceeded")

        assert self._session is not None
        candidates = []
        if normalized:
            candidates.append(normalized)
        fallback = slugify_display_name(display_name)
        if fallback and fallback not in candidates:
            candidates.append(fallback)

        async with self._sem:
            for slug in candidates:
                outcome = await self._fetch_url(self._url(slug, month))
                if outcome.state == "ok":
                    if self.config.use_cache:
                        await self.cache.set(key, {"state": "ok", "data": outcome.data}, ttl_seconds=24 * 3600)
                    self._stats["api_calls"] += 1
                    return outcome
                if outcome.state == "empty":
                    if self.config.use_cache:
                        await self.cache.set(key, {"state": "empty"}, ttl_seconds=24 * 3600)
                    self._stats["empty"] += 1
                    return outcome
            self._stats["error"] += 1
            return FetchOutcome("error", error="all_slug_candidates_failed")

    async def fetch_missing_metrics(self, month_keys: list[str], csv_metrics_map: dict[tuple[int, str], dict[str, Any]]) -> dict[tuple[int, str], FetchOutcome]:
        results: dict[tuple[int, str], FetchOutcome] = {}
        channel_ids = sorted(self.channel_map.keys())

        for idx, month in enumerate(month_keys, 1):
            missing = [cid for cid in channel_ids if (cid, month) not in csv_metrics_map]
            if not missing:
                logger.info("[%s] all metrics already present in CSV", month)
                continue

            logger.info("[%s] fetching metrics for %d missing channel-month pairs", month, len(missing))
            for start in range(0, len(missing), self.config.concurrency_shallow):
                batch = missing[start:start + self.config.concurrency_shallow]
                outcomes = await asyncio.gather(*[
                    self.fetch_one(cid, self.channel_map[cid], self.display_name_map.get(cid, ""), month)
                    for cid in batch
                ])
                for cid, outcome in zip(batch, outcomes):
                    results[(cid, month)] = outcome

            logger.info(
                "[%s] done metrics batch — api_calls=%d cache_hits=%d empty=%d error=%d",
                month, self._stats["api_calls"], self._stats["cache_hits"], self._stats["empty"], self._stats["error"]
            )
        return results


class PanelBuilder:
    def __init__(
        self,
        master: MasterData,
        rank_maps: dict[str, dict[int, int]],
        metrics_maps: dict[str, dict[int, dict[str, Any]]],
    ) -> None:
        self.master = master
        self.rank_maps = rank_maps
        self.metrics_maps = metrics_maps

    def build(self) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []

        for cid in sorted(self.master.channel_map.keys()):
            for mk in self.master.month_keys:
                year = int(mk[:4])
                month_name = mk[4:]

                csv_metrics = self.master.csv_metrics_map.get((cid, mk))
                csv_rank = self.master.csv_rank_map.get((cid, mk))
                rank = self.rank_maps.get(mk, {}).get(cid, csv_rank)

                leaderboard_metrics = self.metrics_maps.get(mk, {}).get(cid)
                leaderboard_has_values = bool(leaderboard_metrics) and any(
                    v is not None for k, v in leaderboard_metrics.items() if k != "source"
                )

                metrics: dict[str, Any] = dict(csv_metrics) if csv_metrics else {}
                if leaderboard_has_values:
                    for key, value in leaderboard_metrics.items():
                        if key == "source":
                            continue
                        if value is not None:
                            metrics[key] = value
                    metrics["source"] = "leaderboard"
                elif metrics:
                    metrics.setdefault("source", "csv")

                if metrics:
                    if rank is not None and rank <= 500:
                        status = "active_top500"
                    elif rank is not None:
                        status = "active_not_top500"
                    else:
                        status = "active_unranked"
                else:
                    status = "inactive"

                rows.append({
                    "channel_id": cid,
                    "channel_name": self.master.channel_map[cid],
                    "display_name": self.master.display_name_map.get(cid, ""),
                    "year": year,
                    "month": month_name,
                    "month_key": mk,
                    "month_order": month_order(month_name),
                    "rank": rank,
                    "average_viewers": metrics.get("average_viewers") if metrics else None,
                    "hours_watched": metrics.get("hours_watched") if metrics else None,
                    "hours_streamed": metrics.get("hours_streamed") if metrics else None,
                    "followers": metrics.get("followers") if metrics else None,
                    "followers_gained": metrics.get("followers_gained") if metrics else None,
                    "peak_viewers": metrics.get("peak_viewers") if metrics else None,
                    "top_game": metrics.get("top_game") if metrics else None,
                    "top_game_hours": metrics.get("top_game_hours") if metrics else None,
                    "top_game_pct": metrics.get("top_game_pct") if metrics else None,
                    "game_count": metrics.get("game_count") if metrics else None,
                    "is_variety_streamer": metrics.get("is_variety_streamer") if metrics else None,
                    "language": metrics.get("language") if metrics else None,
                    "created": metrics.get("created") if metrics else None,
                    "mature": metrics.get("mature") if metrics else None,
                    "peak_viewer_rank": metrics.get("peak_viewer_rank") if metrics else None,
                    "avg_viewer_rank": metrics.get("avg_viewer_rank") if metrics else None,
                    "follower_rank": metrics.get("follower_rank") if metrics else None,
                    "follower_gain_rank": metrics.get("follower_gain_rank") if metrics else None,
                    "status": status,
                })

        df = pd.DataFrame(rows)

        df["rank"] = pd.array(df["rank"], dtype="Int64")
        for col in [
            "channel_id", "year", "month_order",
            "followers", "followers_gained", "peak_viewers",
            "game_count", "peak_viewer_rank", "avg_viewer_rank",
            "follower_rank", "follower_gain_rank",
        ]:
            if col in df.columns:
                df[col] = pd.array(df[col], dtype="Int64")

        for col in ["average_viewers", "hours_watched", "hours_streamed", "top_game_hours", "top_game_pct"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df.sort_values(["channel_id", "year", "month_order"], inplace=True)
        df.reset_index(drop=True, inplace=True)

        expected = len(self.master.channel_map) * len(self.master.month_keys)
        if len(df) != expected:
            raise ValueError(f"Row count mismatch: expected {expected}, got {len(df)}")

        if df.duplicated(subset=["channel_id", "year", "month"]).any():
            raise ValueError("Duplicate (channel_id, year, month) rows detected")

        bad_rank = df["rank"].notna() & (df["rank"] <= 0)
        if bad_rank.any():
            raise ValueError(f"Invalid ranks detected: {int(bad_rank.sum())}")

        ranked_missing_metrics = df[
            df["rank"].notna() &
            df[["average_viewers", "hours_watched", "hours_streamed", "followers", "followers_gained", "peak_viewers"]].isna().all(axis=1)
        ]
        if len(ranked_missing_metrics) > 0:
            logger.warning(
                "%d ranked rows are missing all leaderboard metrics (%.2f%%)",
                len(ranked_missing_metrics),
                100.0 * len(ranked_missing_metrics) / max(1, int(df["rank"].notna().sum())),
            )

        rank_found_pct = float(df["rank"].notna().mean() * 100.0)
        panel_coverage_pct = float((df["status"] != "inactive").mean() * 100.0)
        inactive_pct = float((df["status"] == "inactive").mean() * 100.0)
        metrics_available_pct = float(df["average_viewers"].notna().mean() * 100.0)

        logger.info("Panel built rows=%d statuses=%s", len(df), df["status"].value_counts(dropna=False).to_dict())
        logger.info(
            "Coverage report — rank_found=%.1f%% panel_coverage=%.1f%% inactive=%.1f%% metrics_available=%.1f%%",
            rank_found_pct,
            panel_coverage_pct,
            inactive_pct,
            metrics_available_pct,
        )

        return df

    def save(self, df: pd.DataFrame, output_dir: Path, write_parquet: bool = True) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / "twitch_full_panel.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8")
        logger.info("Saved CSV -> %s", csv_path)

        if write_parquet and HAS_PARQUET:
            parquet_dir = output_dir / "parquet"
            parquet_dir.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_to_dataset(
                table,
                root_path=str(parquet_dir),
                partition_cols=["month_key"],
                existing_data_behavior="overwrite_or_ignore",
            )
            logger.info("Saved Parquet -> %s", parquet_dir)
        elif write_parquet:
            logger.warning("pyarrow not installed; parquet skipped")



def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build a complete Twitch panel dataset")
    p.add_argument("--input", type=Path, default=Path("data/twitch_monthly_fact_table_final.csv"))
    p.add_argument("--output-dir", type=Path, default=Path("output"))
    p.add_argument("--years", nargs="+", type=int, default=[2022, 2023, 2024, 2025])
    p.add_argument("--months", nargs="+", default=MONTHS, choices=MONTHS)
    p.add_argument("--max-rank", type=int, default=20_000)
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    p.add_argument("--force", action="store_true")
    p.add_argument("--no-parquet", action="store_true")
    p.add_argument("--no-cache", action="store_true")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--max-api-calls", type=int, default=15_000)
    p.add_argument("--progress", type=Path, default=Path("checkpoints/rank_progress.json"))
    p.add_argument("--cache-db", type=Path, default=Path("cache/api_cache.sqlite3"))
    return p.parse_args()


async def run_pipeline(args: argparse.Namespace) -> pd.DataFrame:
    config = PipelineConfig(
        input_csv=args.input,
        output_dir=args.output_dir,
        years=args.years,
        months=args.months,
        max_rank_threshold=args.max_rank,
        concurrency_shallow=args.concurrency,
        concurrency_deep=max(5, min(5, args.concurrency)),
        max_api_calls_global=args.max_api_calls,
        use_cache=not args.no_cache,
        cache_db_path=args.cache_db,
        progress_path=args.progress,
        write_parquet=not args.no_parquet,
    )

    master = load_master_csv(config.input_csv)
    month_keys = make_month_keys(config.years, config.months)
    month_keys = [mk for mk in month_keys if mk in set(master.month_keys)]
    if not month_keys:
        raise ValueError("No months remain after filtering input years/months against the CSV")

    cache = SQLiteCache(config.cache_db_path, enabled=config.use_cache)
    cache.connect_sync()
    progress = ProgressStore(config.progress_path)

    async with RankIndexer(sorted(master.channel_map.keys()), month_keys, config, cache, progress) as rank_indexer:
        rank_maps, metrics_maps = await rank_indexer.run_all_months(
            master.csv_rank_map,
            master.csv_metrics_map,
            force=(args.force or args.mode == "full"),
        )

    builder = PanelBuilder(master, rank_maps, metrics_maps)
    df = builder.build()
    builder.save(df, config.output_dir, write_parquet=config.write_parquet)
    await cache.close()
    return df



def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)
    t0 = time.time()
    try:
        df = asyncio.run(run_pipeline(args))
        logger.info("Done rows=%d elapsed=%.1f min output=%s", len(df), (time.time() - t0) / 60, args.output_dir / "twitch_full_panel.csv")
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
