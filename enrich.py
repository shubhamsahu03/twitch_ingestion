#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

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

DEFAULT_INPUT = Path("output/twitch_full_panel_cleaned.csv")
DEFAULT_OUTPUT = Path("output/twitch_full_panel_enriched.csv")
CACHE_DB = Path("cache/enrich_cache.sqlite3")

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID", "nudb4j9kaxj6h6328zbdldqs6hn7ma")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET", "n9h5jqmyu9dpo69et257cjmd1ngp04")

RATE_LIMIT_PER_SEC = 6
USER_BATCH_SIZE = 100
GAMES_CONCURRENCY = 8
REQUEST_TIMEOUT = 25
RETRY_COUNT = 5
CHUNK_SIZE = 5000

logger = logging.getLogger("twitch_enrich")


# =============================================================================
# Logging / helpers
# =============================================================================

def setup_logging(level: str) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(sh)

    Path("logs").mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler("logs/twitch_enrich.log", encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)

    logging.getLogger("aiohttp").setLevel(logging.WARNING)


def normalize_month(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def month_number(value: Any) -> Optional[int]:
    return MONTH_TO_NUM.get(normalize_month(value))


def clean_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def safe_int(value: Any) -> Optional[int]:
    v = clean_number(value)
    return int(v) if v is not None else None


def safe_float(value: Any) -> Optional[float]:
    return clean_number(value)


def normalize_slug(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    s = str(value).strip().lower()
    s = s.replace("https://twitch.tv/", "").replace("http://twitch.tv/", "")
    s = s.replace("https://www.twitch.tv/", "").replace("http://www.twitch.tv/", "")
    s = s.strip("/")
    s = re.sub(r"\s+", "", s)
    return s


def first_nonempty(*values: Any) -> str:
    for v in values:
        if v is None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def chunks(seq: list[Any], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def ensure_columns(df: pd.DataFrame, defaults: dict[str, Any]) -> pd.DataFrame:
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


# =============================================================================
# SQLite cache
# =============================================================================

class SQLiteCache:
    def __init__(self, path: Path):
        self.path = Path(path)
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = asyncio.Lock()

    def connect(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cache (
                cache_key TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at REAL NOT NULL,
                ttl INTEGER NOT NULL
            )
            """
        )
        self._conn.commit()

    async def get(self, key: str) -> Optional[dict[str, Any]]:
        if self._conn is None:
            return None
        async with self._lock:
            cur = self._conn.execute(
                "SELECT payload, created_at, ttl FROM cache WHERE cache_key = ?",
                (key,),
            )
            row = cur.fetchone()
            if not row:
                return None
            payload, created_at, ttl = row
            if (time.time() - float(created_at)) > float(ttl):
                self._conn.execute("DELETE FROM cache WHERE cache_key = ?", (key,))
                self._conn.commit()
                return None
            try:
                return json.loads(payload)
            except Exception:
                return None

    async def set(self, key: str, value: dict[str, Any], ttl_seconds: int = 30 * 24 * 3600) -> None:
        if self._conn is None:
            return
        async with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO cache(cache_key, payload, created_at, ttl) VALUES (?, ?, ?, ?)",
                (key, json.dumps(value, ensure_ascii=False), time.time(), int(ttl_seconds)),
            )
            self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


# =============================================================================
# Twitch Helix client
# =============================================================================

class TwitchHelixClient:
    def __init__(self, client_id: str, client_secret: str, cache: SQLiteCache):
        self.client_id = client_id
        self.client_secret = client_secret
        self.cache = cache
        self.access_token: Optional[str] = None

    async def authenticate(self, session: aiohttp.ClientSession) -> None:
        if not self.client_id or not self.client_secret:
            raise RuntimeError("Missing TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET in environment")
        url = (
            "https://id.twitch.tv/oauth2/token"
            f"?client_id={self.client_id}"
            f"&client_secret={self.client_secret}"
            "&grant_type=client_credentials"
        )
        async with session.post(url, timeout=REQUEST_TIMEOUT) as resp:
            data = await resp.json(content_type=None)
            self.access_token = data.get("access_token")
            if not self.access_token:
                raise RuntimeError(f"Twitch auth failed: {data}")
            logger.info("✅ Twitch auth success")

    def _headers(self) -> dict[str, str]:
        return {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }

    async def get_users_batch(self, session: aiohttp.ClientSession, logins: list[str]) -> dict[str, str]:
        if not self.access_token:
            await self.authenticate(session)

        logins = [normalize_slug(x) for x in logins if normalize_slug(x)]
        if not logins:
            return {}

        cache_key = "twitch_users:" + "|".join(sorted(logins))
        cached = await self.cache.get(cache_key)
        if cached and isinstance(cached.get("rows"), dict):
            return cached["rows"]

        query = "&".join(f"login={l}" for l in logins)
        url = f"https://api.twitch.tv/helix/users?{query}"

        for attempt in range(RETRY_COUNT):
            try:
                async with session.get(url, headers=self._headers(), timeout=REQUEST_TIMEOUT) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        out = {u["login"].lower(): u.get("created_at") for u in data.get("data", []) if u.get("login")}
                        await self.cache.set(cache_key, {"rows": out})
                        return out
                    if resp.status == 429:
                        await asyncio.sleep(2 ** attempt)
                        continue
            except Exception as exc:
                logger.debug("get_users_batch failed: %s", exc)
                await asyncio.sleep(1 + attempt)
        return {}


# =============================================================================
# SullyGnome games client
# =============================================================================

class SullyGnomeGamesClient:
    def __init__(self, cache: SQLiteCache):
        self.cache = cache
        self.semaphore = asyncio.Semaphore(GAMES_CONCURRENCY)

    @staticmethod
    def parse_games_payload(data: Any) -> list[dict[str, Any]]:
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
            raw = first_nonempty(row.get("gamesplayed"), row.get("game"), row.get("games"), row.get("title"))
            game = raw.split("|")[0].strip() if raw else None
            out.append({
                "game": game,
                "hours": round((safe_float(row.get("streamtime")) or 0.0) / 60.0, 2),
                "peak_viewers": safe_int(row.get("maxviewers")),
                "avg_viewers": safe_float(row.get("avgviewers")),
                "view_minutes": safe_float(row.get("viewtime")),
            })
        return out

    async def fetch_game_table(
        self,
        session: aiohttp.ClientSession,
        channel_id: int,
        year: int,
        month_name: str,
    ) -> list[dict[str, Any]]:
        month_name = normalize_month(month_name)
        cache_key = f"sg_games:{channel_id}:{year}:{month_name}"
        cached = await self.cache.get(cache_key)
        if cached and isinstance(cached.get("rows"), list):
            return cached["rows"]

        url = (
            f"https://sullygnome.com/api/tables/channeltables/games/"
            f"{year}{month_name}/{channel_id}/%20/1/2/desc/0/100"
        )

        for attempt in range(RETRY_COUNT):
            try:
                async with self.semaphore:
                    await asyncio.sleep(1.0 / RATE_LIMIT_PER_SEC)
                    async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            rows = self.parse_games_payload(data)
                            await self.cache.set(cache_key, {"rows": rows})
                            return rows
                        if resp.status == 429:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        if resp.status in (403, 404):
                            await self.cache.set(cache_key, {"rows": []}, ttl_seconds=7 * 24 * 3600)
                            return []
            except Exception as exc:
                logger.debug("games fetch failed %s/%s/%s: %s", channel_id, year, month_name, exc)
                await asyncio.sleep(1 + attempt)

        await self.cache.set(cache_key, {"rows": []}, ttl_seconds=7 * 24 * 3600)
        return []


# =============================================================================
# Output writer
# =============================================================================

class StreamWriter:
    def __init__(self, output_csv: Path, write_parquet: bool):
        self.output_csv = Path(output_csv)
        self.write_parquet = write_parquet and HAS_PARQUET
        self._csv_started = False
        self._parquet_writer: Optional[pq.ParquetWriter] = None
        self._parquet_path = self.output_csv.with_suffix(".parquet")

        self.output_csv.parent.mkdir(parents=True, exist_ok=True)
        if self.output_csv.exists():
            self.output_csv.unlink()
        if self._parquet_path.exists():
            self._parquet_path.unlink()

    def write_chunk(self, df: pd.DataFrame) -> None:
        df.to_csv(self.output_csv, mode="a", index=False, header=not self._csv_started, encoding="utf-8")
        self._csv_started = True

        if self.write_parquet:
            table = pa.Table.from_pandas(df, preserve_index=False)
            if self._parquet_writer is None:
                self._parquet_writer = pq.ParquetWriter(str(self._parquet_path), table.schema, compression="snappy")
            self._parquet_writer.write_table(table)

    def close(self) -> None:
        if self._parquet_writer is not None:
            self._parquet_writer.close()
            self._parquet_writer = None


# =============================================================================
# Core enrichment
# =============================================================================

@dataclass
class EnrichmentConfig:
    input_csv: Path
    output_csv: Path
    status_filter: Optional[set[str]] = None
    overwrite_existing: bool = False
    chunksize: int = CHUNK_SIZE


def row_needs_games(row: pd.Series, overwrite_existing: bool) -> bool:
    if overwrite_existing:
        return True
    for col in ("all_games_json", "games_by_stream_time_json", "games_by_avg_viewers_json"):
        if col in row.index:
            val = row.get(col)
            if pd.notna(val) and str(val).strip() != "":
                return False
    return True


async def enrich_created_for_chunk(
    df: pd.DataFrame,
    twitch_client: TwitchHelixClient,
    session: aiohttp.ClientSession,
) -> pd.DataFrame:
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        return df

    if "channel_slug" not in df.columns:
        if "channel_name" in df.columns:
            df["channel_slug"] = df["channel_name"].map(normalize_slug)
        elif "display_name" in df.columns:
            df["channel_slug"] = df["display_name"].map(normalize_slug)
        else:
            return df

    if "created" not in df.columns:
        df["created"] = None

    mask_missing = df["created"].isna() | (df["created"].astype(str).str.strip() == "")
    if not mask_missing.any():
        return df

    unique_logins = (
        df.loc[mask_missing, "channel_slug"]
        .fillna("")
        .map(normalize_slug)
        .loc[lambda s: s != ""]
        .drop_duplicates()
        .tolist()
    )
    if not unique_logins:
        return df

    creation_map: dict[str, str] = {}
    for batch in chunks(unique_logins, USER_BATCH_SIZE):
        res = await twitch_client.get_users_batch(session, batch)
        creation_map.update({k.lower(): v for k, v in res.items() if v})

    if creation_map:
        filled = df["channel_slug"].map(lambda x: creation_map.get(normalize_slug(x)))
        df.loc[mask_missing, "created"] = df.loc[mask_missing, "created"].fillna(filled)
    return df


async def enrich_games_for_chunk(
    df: pd.DataFrame,
    sg_client: SullyGnomeGamesClient,
    session: aiohttp.ClientSession,
    overwrite_existing: bool,
) -> pd.DataFrame:
    needed_cols = [
        "all_games_json",
        "games_by_stream_time_json",
        "games_by_avg_viewers_json",
        "top_game",
        "top_game_hours",
        "top_game_pct",
        "game_count",
        "is_variety_streamer",
    ]
    for col in needed_cols:
        if col not in df.columns:
            df[col] = None

    if "month_lower" not in df.columns:
        df["month_lower"] = df["month"].map(normalize_month)

    needs_mask = df.apply(lambda r: row_needs_games(r, overwrite_existing), axis=1)
    if not needs_mask.any():
        return df

    work = df[needs_mask].copy()
    unique_jobs = (
        work[["channel_id", "year", "month_lower"]]
        .drop_duplicates()
        .to_dict(orient="records")
    )
    if not unique_jobs:
        return df

    results: dict[tuple[int, int, str], dict[str, Any]] = {}

    async def fetch_one(job: dict[str, Any]):
        cid = int(job["channel_id"])
        year = int(job["year"])
        month_lower = str(job["month_lower"])
        games_list = await sg_client.fetch_game_table(session, cid, year, month_lower)
        return job, games_list

    for batch in chunks(unique_jobs, 500):
        tasks = [fetch_one(job) for job in batch]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        for item in batch_results:
            if isinstance(item, Exception):
                logger.warning("⚠️ game job failed: %s", item)
                continue
            job, games_list = item
            if not games_list:
                continue

            all_games = []
            for g in games_list:
                game = g.get("game")
                hours = safe_float(g.get("hours")) or 0.0
                peak = safe_int(g.get("peak_viewers")) or 0
                avg = safe_float(g.get("avg_viewers")) or 0.0
                view_minutes = safe_float(g.get("view_minutes")) or 0.0
                all_games.append([game, round(hours, 2), peak, avg, view_minutes])

            by_time = sorted(all_games, key=lambda x: (x[1] if x[1] is not None else 0), reverse=True)
            by_viewers = sorted(all_games, key=lambda x: (x[3] if x[3] is not None else 0), reverse=True)
            total_hours = float(sum((g[1] or 0.0) for g in all_games))
            top_game = by_time[0][0] if by_time else None
            top_hours = by_time[0][1] if by_time else 0.0
            actual_hours = safe_float(df.loc[
                (df["channel_id"].astype(int) == int(job["channel_id"])) &
                (df["year"].astype(int) == int(job["year"])) &
                (df["month_lower"].astype(str) == str(job["month_lower"])),
                "hours_streamed"
            ].head(1).squeeze())
            actual_hours = actual_hours if actual_hours and actual_hours > 0 else total_hours
            top_pct = round((top_hours / actual_hours) * 100, 2) if actual_hours > 0 else 0.0
            is_variety = bool((top_pct < 75) and (len(all_games) > 2))

            results[(int(job["channel_id"]), int(job["year"]), str(job["month_lower"]))] = {
                "all_games_json": json.dumps(all_games, ensure_ascii=False),
                "games_by_stream_time_json": json.dumps([[g[0], g[1]] for g in by_time], ensure_ascii=False),
                "games_by_avg_viewers_json": json.dumps([[g[0], g[3]] for g in by_viewers], ensure_ascii=False),
                "top_game": top_game,
                "top_game_hours": round(top_hours, 2),
                "top_game_pct": top_pct,
                "game_count": len(all_games),
                "is_variety_streamer": is_variety,
            }

    for (cid, year, month_lower), payload in results.items():
        mask = (
            (df["channel_id"].astype(int) == int(cid)) &
            (df["year"].astype(int) == int(year)) &
            (df["month_lower"].astype(str) == str(month_lower))
        )
        if not mask.any():
            continue

        for col, val in payload.items():
            if col not in df.columns:
                df[col] = None
            if overwrite_existing:
                df.loc[mask, col] = val
            else:
                fill_mask = mask & (df[col].isna() | (df[col].astype(str).str.strip() == ""))
                if fill_mask.any():
                    df.loc[fill_mask, col] = val

    return df


async def enrich_dataset(cfg: EnrichmentConfig) -> None:
    logger.info("📥 Loading %s in chunks of %d", cfg.input_csv, cfg.chunksize)

    cache = SQLiteCache(CACHE_DB)
    cache.connect()

    twitch_client = TwitchHelixClient(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET, cache)
    sg_client = SullyGnomeGamesClient(cache)
    writer = StreamWriter(cfg.output_csv, write_parquet=True)

    total_in = 0
    total_out = 0

    try:
        reader = pd.read_csv(cfg.input_csv, chunksize=cfg.chunksize)
        for chunk_idx, chunk in enumerate(reader, start=1):
            total_in += len(chunk)
            logger.info("━━ chunk %d: rows=%d", chunk_idx, len(chunk))

            chunk = ensure_columns(chunk, {
                "created": None,
                "games_by_avg_viewers_json": None,
                "games_by_stream_time_json": None,
                "all_games_json": None,
                "top_game": None,
                "top_game_hours": None,
                "top_game_pct": None,
                "game_count": None,
                "is_variety_streamer": None,
            })

            if "channel_slug" not in chunk.columns:
                if "channel_name" in chunk.columns:
                    chunk["channel_slug"] = chunk["channel_name"].map(normalize_slug)
                elif "display_name" in chunk.columns:
                    chunk["channel_slug"] = chunk["display_name"].map(normalize_slug)
                else:
                    raise ValueError("Input CSV must contain channel_slug or channel_name or display_name")

            if "year" not in chunk.columns or "month" not in chunk.columns or "channel_id" not in chunk.columns:
                raise ValueError("Input CSV must contain year, month, and channel_id")

            chunk["channel_slug"] = chunk["channel_slug"].map(normalize_slug)
            chunk["month_lower"] = chunk["month"].map(normalize_month)

            if cfg.status_filter:
                before = len(chunk)
                chunk = chunk[chunk["status"].isin(cfg.status_filter)].copy()
                logger.info("   status filter: %d -> %d rows", before, len(chunk))
                if chunk.empty:
                    continue

            async with aiohttp.ClientSession() as session:
                if TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET:
                    chunk = await enrich_created_for_chunk(chunk, twitch_client, session)
                else:
                    logger.warning("TWITCH credentials missing; skipping created enrichment")
                chunk = await enrich_games_for_chunk(chunk, sg_client, session, cfg.overwrite_existing)

            for col in ("game_count", "top_game_hours", "top_game_pct"):
                if col in chunk.columns:
                    chunk[col] = pd.to_numeric(chunk[col], errors="coerce")
            if "is_variety_streamer" in chunk.columns:
                chunk["is_variety_streamer"] = chunk["is_variety_streamer"].astype("boolean")

            if "month_lower" in chunk.columns:
                chunk.drop(columns=["month_lower"], inplace=True)

            writer.write_chunk(chunk)
            total_out += len(chunk)

            logger.info("   wrote chunk %d (out_rows=%d)", chunk_idx, total_out)

    finally:
        writer.close()
        await cache.close()

    logger.info("✅ Enrichment complete: input_rows=%d output_rows=%d -> %s", total_in, total_out, cfg.output_csv)


# =============================================================================
# CLI
# =============================================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Streaming enrichment for twitch_full_panel.csv")
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    p.add_argument("--chunksize", type=int, default=CHUNK_SIZE)
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument(
        "--status-filter",
        default="",
        help="Optional comma-separated statuses to process only (e.g. active_top500,active_not_top500)",
    )
    p.add_argument(
        "--overwrite-existing",
        action="store_true",
        help="Overwrite existing game columns instead of skipping already enriched rows",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    if not args.input.exists():
        raise FileNotFoundError(f"Input file not found: {args.input}")

    status_filter = None
    if args.status_filter.strip():
        status_filter = {s.strip() for s in args.status_filter.split(",") if s.strip()}

    cfg = EnrichmentConfig(
        input_csv=args.input,
        output_csv=args.output,
        status_filter=status_filter,
        overwrite_existing=bool(args.overwrite_existing),
        chunksize=int(args.chunksize),
    )
    asyncio.run(enrich_dataset(cfg))


if __name__ == "__main__":
    main()
