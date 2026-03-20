"""
Twitch Analytics Ingestion Engine
Fully Deterministic · API-First (JSON Only) · Adaptive · Residential-Safe

Phase 1 — IngestionEngine  : Leaderboard + Pie Top-5 (÷4 fix) → twitch_monthly_fact_table.csv
Phase 2 — EnrichmentEngine : Games Table API (all games, paginated) → twitch_monthly_fact_table_enriched.csv
"""

from __future__ import annotations

import asyncio
import contextlib
import csv
import json
import logging
import os
import random
import time
from enum import Enum, auto
from pathlib import Path
from typing import Any, Optional
from dotenv import load_dotenv

import aiohttp

load_dotenv()

# ─────────────────────────────────────────────
#  CONSTANTS  (locked per spec)
# ─────────────────────────────────────────────
MAX_CONCURRENT_REQUESTS = 12
BASE_RATE               = 12
MIN_RATE                = 7
MAX_RATE                = 14
REQUEST_TIMEOUT         = 20
RETRY_LIMIT             = 3
JITTER_MIN              = 0.1
JITTER_MAX              = 0.25
BATCH_SIZE              = 300
BATCH_SLEEP_MIN         = 3
BATCH_SLEEP_MAX         = 5

BASE_URL = "https://sullygnome.com"

# Leaderboard — 5 pages x 100 channels = 500 per month
LEADERBOARD_URL = (
    BASE_URL
    + "/api/tables/channeltables/getchannels/{yearmonth}/0/1/3/desc/{start}/100"
)

# Pie chart APIs (top-5 per channel per month)
PIE_STREAM_TIME_URL = (
    BASE_URL
    + "/api/charts/piecharts/getconfig/channelgamestreamedtime"
    + "/0/{channel_id}/{slug}/%20/%20/{year}/{month}/%20/0/"
)
PIE_AVG_VIEWERS_URL = (
    BASE_URL
    + "/api/charts/piecharts/getconfig/channelgameavgviewers"
    + "/0/{channel_id}/{slug}/%20/%20/{year}/{month}/%20/0/"
)

# Games table API — returns ALL games for a channel-month, paginated at 100
# Confirmed:  GET /api/tables/channeltables/games/{yearmonth}/{channelId}/%20/1/2/desc/{start}/100
# Parameters: yearmonth=2024february  channelId=13076616  start=0,100,200...
GAMES_API_URL = (
    BASE_URL
    + "/api/tables/channeltables/games/{yearmonth}/{channel_id}/%20/1/2/desc/{start}/100"
)

MONTHS = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]

# ─────────────────────────────────────────────
#  SCHEMAS
# ─────────────────────────────────────────────
# File 1 — leaderboard metadata + top-5 pie data (÷4 fixed)
PHASE1_SCHEMA = [
    "year", "month", "channel_id", "channel_slug", "display_name",
    "rank_position", "followers", "followers_gained", "average_viewers",
    "peak_viewers", "hours_streamed", "hours_watched", "streams",
    "status", "mature", "language", "created",
    "peak_viewer_rank", "avg_viewer_rank", "follower_rank", "follower_gain_rank",
    "top5_games_by_avg_viewers_json", "top5_games_by_stream_time_json",
]

# File 2 — Phase 1 columns + full games breakdown from games table API
PHASE2_SCHEMA = PHASE1_SCHEMA + ["all_games_json"]

logger = logging.getLogger("twitch_engine")


# ─────────────────────────────────────────────
#  HEADERS
# ─────────────────────────────────────────────
def _headers() -> dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": BASE_URL + "/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    }


# ─────────────────────────────────────────────
#  TWITCH HELIX CLIENT  (optional — 'created' date)
# ─────────────────────────────────────────────
class TwitchClient:
    """Fetches channel creation dates from Twitch Helix API with in-memory caching."""

    def __init__(self, client_id: str, client_secret: str) -> None:
        self.client_id     = client_id
        self.client_secret = client_secret
        self.token: Optional[str] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._cache: dict[str, str] = {}
        self._pacing_lock = asyncio.Lock()

    async def __aenter__(self) -> "TwitchClient":
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        await self._authenticate()
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._session:
            await self._session.close()

    async def _authenticate(self) -> None:
        url = (
            "https://id.twitch.tv/oauth2/token"
            f"?client_id={self.client_id}"
            f"&client_secret={self.client_secret}"
            "&grant_type=client_credentials"
        )
        assert self._session is not None
        async with self._session.post(url) as resp:
            if resp.status == 200:
                self.token = (await resp.json()).get("access_token")
                logger.info("Twitch Helix API authenticated.")
            else:
                logger.error("Twitch auth failed — status %d", resp.status)

    async def get_created_at(self, channel_id: int, slug: str) -> str:
        cache_key = str(channel_id) if channel_id > 0 else slug
        if cache_key in self._cache:
            return self._cache[cache_key]
        if not self.token or not self._session:
            return ""
        async with self._pacing_lock:
            await asyncio.sleep(0.1)
        try:
            headers = {"Client-ID": self.client_id, "Authorization": f"Bearer {self.token}"}
            async with self._session.get(
                f"https://api.twitch.tv/helix/users?login={slug}", headers=headers
            ) as resp:
                if resp.status == 429:
                    await asyncio.sleep(5)
                    return await self.get_created_at(channel_id, slug)
                if resp.status == 401:
                    await self._authenticate()
                    return await self.get_created_at(channel_id, slug)
                if resp.status == 200:
                    users = (await resp.json()).get("data", [])
                    if users:
                        val = users[0].get("created_at", "")
                        self._cache[cache_key] = val
                        return val
        except Exception as exc:
            logger.debug("Twitch API error for %s: %s", slug, exc)
        return ""


# ─────────────────────────────────────────────
#  ADAPTIVE RATE LIMITER  (token bucket)
# ─────────────────────────────────────────────
class RateLimiter:
    def __init__(self) -> None:
        self._rate: float            = BASE_RATE
        self._tokens: float          = BASE_RATE
        self._last_refill: float     = time.monotonic()
        self._lock                   = asyncio.Lock()
        self._error_timestamps: list[float] = []
        self._total_requests: int    = 0
        self._success_streak: int    = 0
        self._timeout_window: list[float]   = []

    async def acquire(self) -> None:
        async with self._lock:
            self._refill()
            while self._tokens < 1:
                await asyncio.sleep((1 - self._tokens) / self._rate)
                self._refill()
            self._tokens -= 1
        await asyncio.sleep(random.uniform(JITTER_MIN, JITTER_MAX))

    def _refill(self) -> None:
        now = time.monotonic()
        self._tokens = min(self._rate, self._tokens + (now - self._last_refill) * self._rate)
        self._last_refill = now

    def record_429(self) -> None:
        logger.warning("Rate limiter: 429 → 30 s backoff")
        self._rate = max(MIN_RATE, self._rate * 0.8)
        self._success_streak = 0

    def record_5xx(self) -> None:
        now = time.monotonic()
        self._error_timestamps.append(now)
        self._error_timestamps = [t for t in self._error_timestamps if now - t <= 60]
        self._total_requests += 1
        if len(self._error_timestamps) / max(self._total_requests, 1) > 0.05:
            self._rate = max(MIN_RATE, self._rate * 0.8)
            logger.warning("Rate limiter: 5xx >5%% → %.1f rps", self._rate)
        self._success_streak = 0

    def record_timeout(self) -> None:
        now = time.monotonic()
        self._timeout_window.append(now)
        self._timeout_window = [t for t in self._timeout_window if now - t <= 60]
        if len(self._timeout_window) >= 3:
            self._rate = max(MIN_RATE, self._rate * 0.8)
            logger.warning("Rate limiter: timeout spike → %.1f rps", self._rate)
        self._success_streak = 0

    def record_success(self) -> None:
        self._success_streak += 1
        self._total_requests += 1
        if self._success_streak % 20 == 0:
            prev = self._rate
            self._rate = min(MAX_RATE, self._rate + 1)
            if self._rate != prev:
                logger.info("Rate limiter: streak %d → %.1f rps", self._success_streak, self._rate)


# ─────────────────────────────────────────────
#  CIRCUIT BREAKER
# ─────────────────────────────────────────────
class CBState(Enum):
    CLOSED    = auto()
    OPEN      = auto()
    HALF_OPEN = auto()


class CircuitBreaker:
    def __init__(self) -> None:
        self._state                     = CBState.CLOSED
        self._consecutive_failures: int = 0
        self._403_timestamps: list[float] = []
        self._half_open_ok: int         = 0
        self._half_open_fail: int       = 0
        self._open_until: float         = 0.0
        self._lock                      = asyncio.Lock()

    async def check(self) -> None:
        async with self._lock:
            if self._state == CBState.OPEN:
                wait = self._open_until - time.monotonic()
                if wait > 0:
                    logger.warning("Circuit breaker OPEN — waiting %.0f s", wait)
                    await asyncio.sleep(wait)
                self._state = CBState.HALF_OPEN
                self._half_open_ok = self._half_open_fail = 0
                logger.info("Circuit breaker → HALF_OPEN")

    async def record_403(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self._403_timestamps.append(now)
            self._403_timestamps = [t for t in self._403_timestamps if now - t <= 60]
            if len(self._403_timestamps) >= 3:
                await self._open(300)

    async def record_failure(self) -> None:
        async with self._lock:
            self._consecutive_failures += 1
            if self._state == CBState.HALF_OPEN:
                self._half_open_fail += 1
                if self._half_open_fail >= 1:
                    await self._open(600)
            elif self._consecutive_failures >= 10:
                await self._open(300)

    async def record_success(self) -> None:
        async with self._lock:
            self._consecutive_failures = 0
            if self._state == CBState.HALF_OPEN:
                self._half_open_ok += 1
                if self._half_open_ok >= 10:
                    self._state = CBState.CLOSED
                    logger.info("Circuit breaker → CLOSED")

    async def _open(self, duration: float) -> None:
        self._state      = CBState.OPEN
        self._open_until = time.monotonic() + duration
        logger.error("Circuit breaker → OPEN for %.0f s", duration)


# ─────────────────────────────────────────────
#  HTTP CLIENT
# ─────────────────────────────────────────────
class HttpClient:
    def __init__(self, rate_limiter: RateLimiter, circuit_breaker: CircuitBreaker) -> None:
        self._rl = rate_limiter
        self._cb = circuit_breaker
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "HttpClient":
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                limit=MAX_CONCURRENT_REQUESTS,
                limit_per_host=MAX_CONCURRENT_REQUESTS,
                ttl_dns_cache=300,
            ),
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            headers=_headers(),
        )
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._session:
            await self._session.close()

    async def get_json(self, url: str) -> Optional[dict]:
        return await self._request(url)

    async def _request(self, url: str, attempt: int = 0) -> Any:
        await self._cb.check()
        await self._rl.acquire()
        try:
            assert self._session is not None
            async with self._session.get(url) as resp:
                if resp.status == 200:
                    self._rl.record_success()
                    await self._cb.record_success()
                    # content_type=None avoids strict MIME check on some endpoints
                    return await resp.json(content_type=None)

                if resp.status == 429:
                    self._rl.record_429()
                    await asyncio.sleep(30)
                    if attempt < RETRY_LIMIT:
                        return await self._request(url, attempt + 1)
                    return None

                if resp.status == 403:
                    await self._cb.record_403()
                    logger.error("403 Forbidden: %s", url)
                    return None

                if resp.status >= 500:
                    self._rl.record_5xx()
                    await self._cb.record_failure()
                    if attempt < RETRY_LIMIT:
                        await asyncio.sleep(2 ** attempt)
                        return await self._request(url, attempt + 1)
                    return None

                logger.warning("HTTP %d: %s", resp.status, url)
                return None

        except asyncio.TimeoutError:
            self._rl.record_timeout()
            await self._cb.record_failure()
            if attempt < RETRY_LIMIT:
                await asyncio.sleep(1.5 ** attempt)
                return await self._request(url, attempt + 1)
            logger.error("Timeout after %d retries: %s", RETRY_LIMIT, url)
            return None

        except aiohttp.ClientError as exc:
            await self._cb.record_failure()
            if attempt < RETRY_LIMIT:
                await asyncio.sleep(2)
                return await self._request(url, attempt + 1)
            logger.error("ClientError %s — %s", exc, url)
            return None


# ─────────────────────────────────────────────
#  CHECKPOINT  (resume-safe, append-only)
# ─────────────────────────────────────────────
class Checkpoint:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._done: set[str] = set()
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            with open(self._path, encoding="utf-8") as f:
                for line in f:
                    k = line.strip()
                    if k:
                        self._done.add(k)
            logger.info("Checkpoint loaded: %d records (%s)", len(self._done), self._path.name)

    def key(self, channel_id: int, year: int, month: str) -> str:
        return f"{channel_id}_{year}_{month}"

    def is_done(self, channel_id: int, year: int, month: str) -> bool:
        return self.key(channel_id, year, month) in self._done

    def mark_done(self, channel_id: int, year: int, month: str) -> None:
        k = self.key(channel_id, year, month)
        self._done.add(k)
        with open(self._path, "a", encoding="utf-8") as f:
            f.write(k + "\n")


# ─────────────────────────────────────────────
#  CSV WRITER  (asyncio-safe, schema-aware)
# ─────────────────────────────────────────────
class CsvWriter:
    def __init__(self, path: Path, schema: list[str]) -> None:
        self._path   = path
        self._schema = schema
        self._lock   = asyncio.Lock()
        if not path.exists():
            with open(path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=schema, extrasaction="ignore").writeheader()

    async def write(self, record: dict) -> None:
        async with self._lock:
            with open(self._path, "a", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=self._schema, extrasaction="ignore").writerow(record)


# ─────────────────────────────────────────────
#  EXTRACTION HELPERS
# ─────────────────────────────────────────────
def _extract_pie_stream_time(data: Optional[dict]) -> str:
    """
    Parse channelgamestreamedtime pie API response.
    Values are hours streamed per game — stored as-is, no correction needed.
    Output: [["GameName", hours], ..., ["Other", hours]]  top-5 + remainder
    """
    if not data:
        return "[]"
    try:
        labels: list[str]   = data["data"]["labels"]
        values: list[float] = data["data"]["datasets"][0]["data"]
        pairs = sorted([(g, v / 4) for g, v in zip(labels, values)],key=lambda x: x[1],reverse=True,)
        top5  = pairs[:5]
        rest  = sum(v for _, v in pairs[5:])
        result: list[list] = [[g, round(v, 2)] for g, v in top5]
        if rest:
            result.append(["Other", round(rest, 2)])
        return json.dumps(result)
    except (KeyError, IndexError, TypeError):
        return "[]"


def _extract_pie_avg_viewers(data: Optional[dict]) -> str:
    """
    Parse channelgameavgviewers pie API response.

    BUG FIX (÷4): The SullyGnome pie API returns average viewer values that are
    4x the figure displayed on the website. Confirmed by cross-referencing the
    API output against multiple rendered channel-month pages. The ÷4 correction
    is applied ONLY here — the stream-time pie and the games table API are unaffected.

    Output: [["GameName", avg_viewers], ..., ["Other", avg_viewers]]  top-5 + remainder
    """
    if not data:
        return "[]"
    try:
        labels: list[str]   = data["data"]["labels"]
        values: list[float] = data["data"]["datasets"][0]["data"]
        pairs = sorted(
            [(g, v / 4) for g, v in zip(labels, values)],
            key=lambda x: x[1],
            reverse=True,
        )
        top5  = pairs[:5]
        rest  = sum(v for _, v in pairs[5:])
        result: list[list] = [[g, round(v, 2)] for g, v in top5]
        if rest:
            result.append(["Other", round(rest, 2)])
        return json.dumps(result)
    except (KeyError, IndexError, TypeError):
        return "[]"


def _parse_games_api_rows(rows: list[dict]) -> str:
    """
    Convert games table API rows into compact JSON.

    API field mapping (confirmed from live response):
        gamesplayed → "DisplayName|SlugName|ImageURL" → split '|', take index 0
        streamtime  → minutes → ÷60 → hours_streamed
        viewtime    → minutes → ÷60 → hours_watched
        avgviewers  → direct integer (NO ÷4 — this is the table API, not the pie API)
        maxviewers  → peak_viewers

    Output per row: ["GameName", hours_streamed, avg_viewers, peak_viewers, hours_watched]
    Sorted by hours_streamed descending (API default: sort col 2 desc).
    """
    games: list[list] = []
    for row in rows:
        game_raw: str = (row.get("gamesplayed") or "")
        game_name     = game_raw.split("|")[0].strip() if game_raw else "Unknown"
        games.append([
            game_name,
            round((row.get("streamtime") or 0) / 60, 2),   # minutes → hours
            int(row.get("avgviewers") or 0),                 # already avg concurrent — no correction
            int(row.get("maxviewers") or 0),                 # peak viewers
            round((row.get("viewtime")  or 0) / 60, 2),     # minutes → hours watched
        ])
    return json.dumps(games) if games else "[]"


# ─────────────────────────────────────────────
#  CORE FETCHERS
# ─────────────────────────────────────────────
async def fetch_leaderboard_page(
    client: HttpClient, yearmonth: str, start: int
) -> list[dict]:
    data = await client.get_json(LEADERBOARD_URL.format(yearmonth=yearmonth, start=start))
    if not data:
        return []
    rows = data.get("data", data) if isinstance(data, dict) else data
    if isinstance(rows, dict):
        rows = rows.get("aaData", [])
    return rows if isinstance(rows, list) else []


async def fetch_leaderboard_month(
    client: HttpClient, year: int, month_name: str
) -> list[dict]:
    """Fetch all 5 pages concurrently → up to 500 channels."""
    yearmonth = f"{year}{month_name}"
    pages = await asyncio.gather(*[
        fetch_leaderboard_page(client, yearmonth, s) for s in range(0, 500, 100)
    ])
    channels: list[dict] = []
    for page in pages:
        channels.extend(page)
    return channels


async def fetch_pie(
    client: HttpClient,
    channel_id: int,
    slug: str,
    year: int,
    month_num: int,
    pie_type: str,
) -> str:
    """Fetch one pie chart and return parsed top-5 JSON."""
    if pie_type == "stream_time":
        url  = PIE_STREAM_TIME_URL.format(
            channel_id=channel_id, slug=slug, year=year, month=month_num
        )
        data = await client.get_json(url)
        return _extract_pie_stream_time(data)
    else:
        url  = PIE_AVG_VIEWERS_URL.format(
            channel_id=channel_id, slug=slug, year=year, month=month_num
        )
        data = await client.get_json(url)
        return _extract_pie_avg_viewers(data)  # ÷4 correction applied inside


async def fetch_games_api(
    client: HttpClient, channel_id: int, yearmonth: str
) -> str:
    """
    Fetch ALL games played by a channel in a given month from the games table API.

    Paginates automatically: if recordsTotal > 100, fetches further pages at
    start=100, 200, ... until all rows are collected.

    Most channels play < 20 games/month so one request suffices; pagination
    handles variety streamers who may approach or exceed 100.
    """
    all_rows: list[dict] = []
    start = 0

    while True:
        url  = GAMES_API_URL.format(yearmonth=yearmonth, channel_id=channel_id, start=start)
        data = await client.get_json(url)
        if not data:
            break

        page_rows     = data.get("data") or []
        records_total = int(data.get("recordsTotal") or 0)
        all_rows.extend(page_rows)

        # Stop when collected all, got an empty page, or there are no records
        if not page_rows or len(all_rows) >= records_total:
            break
        start += 100

    return _parse_games_api_rows(all_rows)


# ─────────────────────────────────────────────
#  PHASE 1 — RECORD BUILDER
# ─────────────────────────────────────────────
async def build_phase1_record(
    client: HttpClient,
    twitch_client: Optional[TwitchClient],
    channel: dict,
    year: int,
    month_name: str,
    month_num: int,
) -> Optional[dict]:
    """Build one Phase 1 row: leaderboard metadata + top-5 pie charts."""
    channel_id: int  = channel.get("id") or channel.get("channelId", 0)
    slug: str        = (channel.get("url") or channel.get("channelurl") or "").strip("/").split("/")[-1]
    display_name: str = channel.get("displayname") or channel.get("channelname", "")

    # Fire both pie fetches (and optional Twitch created date) concurrently
    tasks: list = [
        asyncio.create_task(fetch_pie(client, channel_id, slug, year, month_num, "stream_time")),
        asyncio.create_task(fetch_pie(client, channel_id, slug, year, month_num, "avg_viewers")),
    ]
    if twitch_client:
        tasks.append(asyncio.create_task(twitch_client.get_created_at(channel_id, slug)))

    results      = await asyncio.gather(*tasks)
    pie_st       = results[0]
    pie_av       = results[1]
    created_date = results[2] if twitch_client and len(results) > 2 else ""

    streamed_min = channel.get("streamedminutes") or 0
    view_min     = channel.get("viewminutes")     or 0

    return {
        "year":                           year,
        "month":                          month_name,
        "channel_id":                     channel_id,
        "channel_slug":                   slug,
        "display_name":                   display_name,
        "rank_position":                  channel.get("rownum") or channel.get("rank", ""),
        "followers":                      channel.get("followers", ""),
        "followers_gained":               channel.get("followersgained", ""),
        "average_viewers":                channel.get("avgviewers")  or channel.get("averageviewers", ""),
        "peak_viewers":                   channel.get("maxviewers")  or channel.get("peakviewers", ""),
        "hours_streamed":                 round(streamed_min / 60, 2) if streamed_min else "",
        "hours_watched":                  round(view_min     / 60, 2) if view_min     else "",
        "streams":                        "",  # not available from leaderboard or pie APIs
        "status":                         channel.get("status",   ""),
        "mature":                         channel.get("mature",   ""),
        "language":                       channel.get("language", ""),
        "created":                        created_date,
        # Rank columns are empty here; populated by the inline transformer after Phase 2
        "peak_viewer_rank":               "",
        "avg_viewer_rank":                "",
        "follower_rank":                  "",
        "follower_gain_rank":             "",
        "top5_games_by_avg_viewers_json": pie_av,
        "top5_games_by_stream_time_json": pie_st,
    }


# ─────────────────────────────────────────────
#  PHASE 1 — INGESTION ENGINE
# ─────────────────────────────────────────────
class IngestionEngine:
    """
    Phase 1: Leaderboard API + Pie Top-5 (÷4 fixed) → PHASE1_SCHEMA CSV.
    Output: twitch_monthly_fact_table.csv
    """

    def __init__(
        self,
        output_path: Path,
        checkpoint_path: Path,
        mode: str = "full",
        years: Optional[list[int]] = None,
        months: Optional[list[str]] = None,
    ) -> None:
        self._output_path   = output_path
        self._checkpoint    = Checkpoint(checkpoint_path)
        self._writer        = CsvWriter(output_path, PHASE1_SCHEMA)
        self._mode          = mode
        self._years         = years  or list(range(2022, 2026))
        self._months        = months or MONTHS
        self._rl            = RateLimiter()
        self._cb            = CircuitBreaker()
        self._sem           = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self._stats         = {"processed": 0, "skipped": 0, "errors": 0, "start": time.time()}
        self._twitch_id     = os.getenv("TWITCH_CLIENT_ID")
        self._twitch_secret = os.getenv("TWITCH_CLIENT_SECRET")

    async def run(self) -> None:
        twitch_ctx = (
            TwitchClient(self._twitch_id, self._twitch_secret)
            if self._twitch_id and self._twitch_secret
            else None
        )
        if not twitch_ctx:
            logger.warning("TWITCH credentials not set — 'created' column will be empty.")

        work = [(y, m, MONTHS.index(m) + 1) for y in self._years for m in self._months]

        async with HttpClient(self._rl, self._cb) as client, \
                   (twitch_ctx if twitch_ctx else contextlib.nullcontext()) as tc:
            batch_count = 0
            for year, month_name, month_num in work:
                logger.info("═══ [Phase 1] %d %s ═══", year, month_name.capitalize())
                channels = await fetch_leaderboard_month(client, year, month_name)
                if not channels:
                    logger.warning("No channels for %d %s — skipping", year, month_name)
                    continue
                logger.info("  Leaderboard: %d channels", len(channels))

                for i in range(0, len(channels), BATCH_SIZE):
                    await self._process_batch(
                        client, tc, channels[i: i + BATCH_SIZE],
                        year, month_name, month_num
                    )
                    batch_count += 1
                    if batch_count % 2 == 0:
                        pause = random.uniform(BATCH_SLEEP_MIN, BATCH_SLEEP_MAX)
                        logger.info("Batch pause %.1f s | processed: %d", pause, self._stats["processed"])
                        await asyncio.sleep(pause)

                self._log_progress()

        logger.info(
            "Phase 1 done — processed=%d skipped=%d errors=%d runtime=%.1f min",
            self._stats["processed"], self._stats["skipped"],
            self._stats["errors"], (time.time() - self._stats["start"]) / 60,
        )

    async def _process_batch(
        self,
        client: HttpClient,
        twitch_client: Optional[TwitchClient],
        channels: list[dict],
        year: int,
        month_name: str,
        month_num: int,
    ) -> None:
        tasks = []
        for ch in channels:
            cid = ch.get("id") or ch.get("channelId", 0)
            if self._mode == "incremental" and self._checkpoint.is_done(cid, year, month_name):
                self._stats["skipped"] += 1
                continue
            tasks.append(self._process_one(client, twitch_client, ch, year, month_name, month_num))
        if tasks:
            await asyncio.gather(*tasks)

    async def _process_one(
        self,
        client: HttpClient,
        twitch_client: Optional[TwitchClient],
        channel: dict,
        year: int,
        month_name: str,
        month_num: int,
    ) -> None:
        cid = channel.get("id") or channel.get("channelId", 0)
        async with self._sem:
            try:
                record = await build_phase1_record(
                    client, twitch_client, channel, year, month_name, month_num
                )
                if record:
                    await self._writer.write(record)
                    self._checkpoint.mark_done(cid, year, month_name)
                    self._stats["processed"] += 1
                else:
                    self._stats["errors"] += 1
            except Exception as exc:
                logger.exception("Phase 1 error — channel %s: %s", cid, exc)
                self._stats["errors"] += 1

    def _log_progress(self) -> None:
        elapsed = time.time() - self._stats["start"]
        logger.info(
            "Progress — processed=%d skipped=%d errors=%d rate=%.2f rec/s elapsed=%.1f min",
            self._stats["processed"], self._stats["skipped"], self._stats["errors"],
            self._stats["processed"] / max(elapsed, 1), elapsed / 60,
        )


# ─────────────────────────────────────────────
#  PHASE 2 — ENRICHMENT ENGINE
# ─────────────────────────────────────────────
class EnrichmentEngine:
    """
    Phase 2: Reads Phase 1 CSV, calls the games table API for each channel-month,
    appends all_games_json, and writes a PHASE2_SCHEMA CSV.

    Uses a separate checkpoint file (checkpoints/enriched.txt) so it is independently
    resume-safe without interfering with Phase 1's checkpoint.

    Output: twitch_monthly_fact_table_enriched.csv  (input to inline transformer)
    """

    def __init__(
        self,
        phase1_path: Path,
        phase2_path: Path,
        checkpoint_path: Path,
    ) -> None:
        self._phase1_path = phase1_path
        self._phase2_path = phase2_path
        self._checkpoint  = Checkpoint(checkpoint_path)
        self._writer      = CsvWriter(phase2_path, PHASE2_SCHEMA)
        self._rl          = RateLimiter()
        self._cb          = CircuitBreaker()
        self._sem         = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self._stats       = {"processed": 0, "skipped": 0, "errors": 0, "start": time.time()}

    async def run(self) -> None:
        if not self._phase1_path.exists():
            raise FileNotFoundError(
                f"Phase 1 output not found: {self._phase1_path}  — run Phase 1 first."
            )

        with open(self._phase1_path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        logger.info("═══ [Phase 2] Enrichment: %d rows to process ═══", len(rows))

        async with HttpClient(self._rl, self._cb) as client:
            batch_count = 0
            for i in range(0, len(rows), BATCH_SIZE):
                await self._process_batch(client, rows[i: i + BATCH_SIZE])
                batch_count += 1
                if batch_count % 2 == 0:
                    pause = random.uniform(BATCH_SLEEP_MIN, BATCH_SLEEP_MAX)
                    logger.info("Phase 2 batch pause %.1f s | enriched: %d", pause, self._stats["processed"])
                    await asyncio.sleep(pause)

        logger.info(
            "Phase 2 done — processed=%d skipped=%d errors=%d runtime=%.1f min",
            self._stats["processed"], self._stats["skipped"],
            self._stats["errors"], (time.time() - self._stats["start"]) / 60,
        )

    async def _process_batch(self, client: HttpClient, rows: list[dict]) -> None:
        tasks = []
        for row in rows:
            cid   = int(row.get("channel_id") or 0)
            year  = int(row.get("year")       or 0)
            month = row.get("month", "")
            if self._checkpoint.is_done(cid, year, month):
                self._stats["skipped"] += 1
                continue
            tasks.append(self._enrich_row(client, row))
        if tasks:
            await asyncio.gather(*tasks)

    async def _enrich_row(self, client: HttpClient, row: dict) -> None:
        cid       = int(row.get("channel_id") or 0)
        year      = int(row.get("year")       or 0)
        month     = row.get("month", "")
        yearmonth = f"{year}{month}"

        async with self._sem:
            try:
                all_games        = await fetch_games_api(client, cid, yearmonth)
                enriched         = dict(row)
                enriched["all_games_json"] = all_games
                await self._writer.write(enriched)
                self._checkpoint.mark_done(cid, year, month)
                self._stats["processed"] += 1
            except Exception as exc:
                logger.exception("Phase 2 error — %s %s %s: %s", cid, year, month, exc)
                self._stats["errors"] += 1
