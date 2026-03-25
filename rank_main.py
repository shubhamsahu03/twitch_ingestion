#!/usr/bin/env python3
"""
rank_main.py
============
Phase 4 — Global Rank Indexer  (standalone CLI)

Reads  twitch_monthly_fact_table_final.csv  to extract the 2,429 unique
channel IDs, then for each of the 48 months (2022–2025) paginates the
SullyGnome channel leaderboard API and records the exact global rank of
every channel — including those outside the original top-500.

Output files (under --output-dir, default data/ranks/):
  twitch_global_ranks.csv          — tidy panel, all 2429 × 48 rows
  parquet/month_key=<key>/         — Parquet partitions (if pyarrow installed)

Usage
──────
  # Minimal — full run with defaults
  python rank_main.py

  # Custom paths / scope
  python rank_main.py \\
      --input  data/twitch_monthly_fact_table_final.csv \\
      --output-dir data/ranks \\
      --years 2022 2023 \\
      --months january february march \\
      --max-rank 10000 \\
      --concurrency 15 \\
      --log-level DEBUG

  # Incremental (skip already-completed months)
  python rank_main.py --mode incremental

  # Force full reprocessing even if progress exists
  python rank_main.py --force

  # Enable Redis cache (requires redis server + redis-py[asyncio])
  python rank_main.py --redis
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

# ── Module imports ─────────────────────────────────────────────────────────────
try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas is required — pip install pandas", file=sys.stderr)
    sys.exit(1)

# rank_indexer lives at engine/rank_indexer.py — add parent to sys.path so
# `python rank_main.py` works whether placed at project root or inside engine/.
sys.path.insert(0, str(Path(__file__).parent))
from engine.rank_indexer import RankConfig, RankIndexer

MONTHS_ALL = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]


# ──────────────────────────────────────────────────────────────────────────────
#  HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def setup_logging(level: str) -> None:
    Path("logs").mkdir(exist_ok=True)
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    fh = logging.FileHandler("logs/rank_indexer.log", encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)

    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


logger = logging.getLogger("twitch_engine.rank_main")


def load_target_channels(csv_path: Path) -> list[int]:
    """
    Read the fact table CSV and return the sorted list of unique channel_ids.
    Validates that IDs are positive integers and logs a summary.
    """
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Input CSV not found: {csv_path}\n"
            "Run the main pipeline first to generate twitch_monthly_fact_table_final.csv"
        )

    df = pd.read_csv(csv_path, usecols=["channel_id"], dtype={"channel_id": str})
    raw_ids = df["channel_id"].dropna().unique().tolist()

    channel_ids: list[int] = []
    skipped = 0
    for raw in raw_ids:
        try:
            cid = int(raw)
            if cid > 0:
                channel_ids.append(cid)
            else:
                skipped += 1
        except (ValueError, TypeError):
            skipped += 1

    channel_ids.sort()
    logger.info(
        "Loaded %d unique channel IDs from %s  (skipped invalid: %d)",
        len(channel_ids), csv_path.name, skipped
    )
    return channel_ids


def build_month_keys(years: list[int], months: list[str]) -> list[str]:
    """Return month keys in strict chronological order, e.g. ['2022january', ...].

    Months are sorted by calendar position regardless of the order the user
    provided them on the CLI (argparse preserves input order, not calendar order).
    """
    CALENDAR = [
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
    ]
    ordered_months = sorted(months, key=lambda m: CALENDAR.index(m))
    keys: list[str] = []
    for year in sorted(years):
        for month in ordered_months:
            keys.append(f"{year}{month}")
    return keys


# ──────────────────────────────────────────────────────────────────────────────
#  CLI
# ──────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 4 — Twitch Global Rank Indexer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python rank_main.py
  python rank_main.py --max-rank 5000 --concurrency 10
  python rank_main.py --years 2024 2025 --months january february
  python rank_main.py --force           # reprocess even if progress exists
  python rank_main.py --redis           # enable Redis page cache
        """,
    )
    parser.add_argument(
        "--input", type=Path,
        default=Path("data/twitch_monthly_fact_table_final.csv"),
        help="Path to twitch_monthly_fact_table_final.csv",
    )
    parser.add_argument(
        "--output-dir", type=Path,
        default=Path("data/ranks"),
        help="Directory for output CSV + Parquet (default: data/ranks/)",
    )
    parser.add_argument(
        "--checkpoint-dir", type=Path,
        default=Path("checkpoints"),
        help="Directory for progress JSON (default: checkpoints/)",
    )
    parser.add_argument(
        "--years", nargs="+", type=int,
        default=list(range(2022, 2026)),
        help="Years to process (default: 2022 2023 2024 2025)",
    )
    parser.add_argument(
        "--months", nargs="+",
        default=MONTHS_ALL,
        choices=MONTHS_ALL,
        metavar="MONTH",
        help="Months to process (default: all 12)",
    )
    parser.add_argument(
        "--max-rank", type=int, default=10_000,
        help="Max rank threshold to scan per month (default: 10000)",
    )
    parser.add_argument(
        "--concurrency", type=int, default=15,
        help="Async concurrency level (default: 15)",
    )
    parser.add_argument(
        "--mode", choices=["full", "incremental"],
        default="incremental",
        help="incremental=skip completed months, full=force reprocess all",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Force reprocessing of all months (overrides --mode)",
    )
    parser.add_argument(
        "--redis", action="store_true",
        help="Enable Redis page/rank cache (requires running Redis + redis-py[asyncio])",
    )
    parser.add_argument(
        "--redis-url", default="redis://localhost:6379",
        help="Redis connection URL (default: redis://localhost:6379)",
    )
    parser.add_argument(
        "--max-api-calls", type=int, default=15_000,
        help="Global API call budget before aborting (default: 15000)",
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


# ──────────────────────────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    t0 = time.time()

    # ── Resolve force flag ────────────────────────────────────────────────────
    force = args.force or (args.mode == "full")

    # ── Load targets ──────────────────────────────────────────────────────────
    target_channels = load_target_channels(args.input)
    month_keys      = build_month_keys(args.years, args.months)

    # ── Estimate workload ─────────────────────────────────────────────────────
    pages_per_month = args.max_rank // 100   # e.g. 10000 / 100 = 100
    est_calls       = len(month_keys) * pages_per_month
    est_min_lo      = est_calls / (args.concurrency * 60 * 3)
    est_min_hi      = est_calls / (args.concurrency * 60 * 1)

    logger.info("=" * 64)
    logger.info("  Phase 4 — Global Rank Indexer")
    logger.info("  Target channels  : %d", len(target_channels))
    logger.info("  Months           : %d  (%s → %s)", len(month_keys), month_keys[0], month_keys[-1])
    logger.info("  Max rank/month   : %d  (%d pages × 100)", args.max_rank, pages_per_month)
    logger.info("  Concurrency      : %d", args.concurrency)
    logger.info("  Redis cache      : %s", args.redis)
    logger.info("  Mode             : %s%s", args.mode, "  (+force)" if force else "")
    logger.info("  Est. API calls   : ≤%d  (with early stopping, often much less)", est_calls)
    logger.info("  Est. runtime     : %.0f – %.0f min", est_min_lo, est_min_hi)
    logger.info("=" * 64)

    # ── Build config ──────────────────────────────────────────────────────────
    config = RankConfig(
        max_rank_threshold      = args.max_rank,
        concurrency             = args.concurrency,
        output_dir              = args.output_dir,
        parquet_dir             = args.output_dir / "parquet",
        progress_file           = args.checkpoint_dir / "rank_progress.json",
        use_redis               = args.redis,
        redis_url               = args.redis_url,
        max_api_calls_global    = args.max_api_calls,
        max_api_calls_per_month = pages_per_month * 2,  # generous per-month ceiling
    )

    # ── Run ───────────────────────────────────────────────────────────────────
    async with RankIndexer(target_channels, month_keys, config) as indexer:
        await indexer.run_all_months(force=force)
        df = indexer.build_panel_dataframe()
        indexer.save(df)
        indexer.print_summary()

    # ── Final stats ───────────────────────────────────────────────────────────
    found_pct = (df["status"] == "found").mean() * 100
    elapsed   = (time.time() - t0) / 60

    logger.info("")
    logger.info("═" * 64)
    logger.info("  Output           : %s", config.output_dir / "twitch_global_ranks.csv")
    logger.info("  Total rows       : %d", len(df))
    logger.info("  Found            : %.1f%%  (%d rows)",
                found_pct, (df["status"] == "found").sum())
    logger.info("  Not found        : %.1f%%  (%d rows)",
                100 - found_pct, (df["status"] != "found").sum())
    logger.info("  Wall time        : %.1f min", elapsed)
    logger.info("═" * 64)

    return df


if __name__ == "__main__":
    asyncio.run(main())
