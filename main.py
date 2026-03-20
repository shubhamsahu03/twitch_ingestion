#!/usr/bin/env python3
"""
Twitch Analytics Pipeline — Single-Run Orchestrator

Runs three phases sequentially and produces two deliverable files:

  Phase 1  →  twitch_monthly_fact_table.csv          (leaderboard + top-5 pie, ÷4 fixed)
  Phase 2  →  twitch_monthly_fact_table_enriched.csv  (+ all_games_json from games table API)
  Transform→  twitch_monthly_fact_table_final.csv     (+ cohort ranks, clean column order)

Usage:
    python main.py --mode full
    python main.py --mode incremental
    python main.py --mode full --years 2024 2025
    python main.py --mode full --years 2022 --months january february
"""

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

from engine.ingestion import (
    IngestionEngine,
    EnrichmentEngine,
    MONTHS,
)


# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
def setup_logging(log_level: str = "INFO") -> None:
    Path("logs").mkdir(exist_ok=True)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    root.addHandler(ch)

    fh = logging.FileHandler("logs/engine.log", encoding="utf-8")
    fh.setFormatter(formatter)
    root.addHandler(fh)

    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


logger = logging.getLogger("twitch_engine.main")


# ─────────────────────────────────────────────
#  INLINE TRANSFORMER  (replaces transformer.py)
# ─────────────────────────────────────────────
def run_transformer(enriched_csv: Path, final_csv: Path) -> None:
    """
    Phase 3 — Feature Engineering (Cohort Ranking).

    Reads the Phase 2 enriched CSV, computes per-month cohort ranks for
    avg_viewers, peak_viewers, followers, and followers_gained, then writes
    the final clean CSV.

    Key fixes vs. the old transformer.py:
      • pd.to_numeric() coercion before groupby-rank prevents silent NaN ranks
        when a value is an empty string (which would previously produce rank 0
        for ALL rows in that cohort).
      • na_option='bottom' sends channels with missing data to the bottom of the
        rank instead of floating to an arbitrary position.
      • ascending=False is explicit: highest metric value = Rank 1.
    """
    try:
        import pandas as pd
    except ImportError:
        logger.error("pandas not installed — run: pip install pandas")
        raise

    logger.info("═══ [Phase 3] Transformer — Cohort Ranking ═══")
    logger.info("Input : %s", enriched_csv)
    logger.info("Output: %s", final_csv)

    if not enriched_csv.exists():
        raise FileNotFoundError(
            f"Enriched CSV not found: {enriched_csv}  — run Phase 2 first."
        )

    df = pd.read_csv(enriched_csv)
    logger.info("Loaded %d rows × %d columns", *df.shape)

    # Drop 'streams' — not retrievable from any available API endpoint
    if "streams" in df.columns:
        df.drop(columns=["streams"], inplace=True)
        logger.info("Dropped 'streams' column (unavailable from API).")

    # ── Cohort rank calculation ──────────────────────────────────────────────
    # Ranks are scoped per (year, month) so each month's Top 500 cohort gets
    # independent ranks from 1 to 500.
    rank_mappings = {
        "average_viewers":  "avg_viewer_rank",
        "peak_viewers":     "peak_viewer_rank",
        "followers":        "follower_rank",
        "followers_gained": "follower_gain_rank",
    }

    for metric_col, rank_col in rank_mappings.items():
        if metric_col not in df.columns:
            logger.warning("Missing column '%s' — skipping %s", metric_col, rank_col)
            continue

        # Coerce to numeric — empty strings / non-numeric values become NaN
        df[metric_col] = pd.to_numeric(df[metric_col], errors="coerce")

        df[rank_col] = (
            df.groupby(["year", "month"])[metric_col]
            .rank(
                method="min",       # ties share the best rank  e.g. 1,2,2,4
                ascending=False,    # highest value = Rank 1
                na_option="bottom", # channels with NaN go to the bottom of cohort
            )
            .fillna(0)
            .astype(int)
        )
        logger.info("  ✓ %s  (highest %s = Rank 1)", rank_col, metric_col)

    # Handle missing creation dates
    if "created" in df.columns:
        missing = df["created"].isna().sum()
        df["created"] = df["created"].fillna("")
        if missing:
            logger.info("Filled %d missing 'created' values with ''", missing)

    # ── Final column order ───────────────────────────────────────────────────
    final_columns = [
        "year", "month", "channel_id", "channel_slug", "display_name",
        "rank_position", "followers", "followers_gained", "average_viewers", "peak_viewers",
        "hours_streamed", "hours_watched", "status", "mature", "language", "created",
        "peak_viewer_rank", "avg_viewer_rank", "follower_rank", "follower_gain_rank",
        "top5_games_by_avg_viewers_json", "top5_games_by_stream_time_json",
        "all_games_json",
    ]
    # Keep only columns that actually exist (graceful if a column is missing)
    final_columns = [c for c in final_columns if c in df.columns]

    final_csv.parent.mkdir(parents=True, exist_ok=True)
    df[final_columns].to_csv(final_csv, index=False, encoding="utf-8")

    rows, cols = df[final_columns].shape
    logger.info("Transformer done — %d rows × %d columns → %s", rows, cols, final_csv)


# ─────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Twitch Analytics Pipeline — Phase 1 → Phase 2 → Transform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Output files (all written under --data-dir):
  twitch_monthly_fact_table.csv          Phase 1: leaderboard + top-5 pie (÷4 fixed)
  twitch_monthly_fact_table_enriched.csv Phase 2: + all_games_json from games table API
  twitch_monthly_fact_table_final.csv    Phase 3: + cohort ranks (transformer)

Examples:
  python main.py --mode full
  python main.py --mode incremental
  python main.py --mode full --years 2024 2025
  python main.py --mode full --years 2022 --months january february march
  python main.py --mode full --skip-phase2   # produce only the Phase 1 file
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
        help="full = reprocess everything; incremental = skip already checkpointed rows",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Directory for output CSVs  (default: data/)",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        default=Path("checkpoints"),
        help="Directory for checkpoint files  (default: checkpoints/)",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=list(range(2022, 2026)),
        help="Years to process  (default: 2022 2023 2024 2025)",
    )
    parser.add_argument(
        "--months",
        nargs="+",
        default=MONTHS,
        choices=MONTHS,
        metavar="MONTH",
        help="Months to process  (default: all 12)",
    )
    parser.add_argument(
        "--skip-phase2",
        action="store_true",
        help="Stop after Phase 1 — skip games API enrichment and transformer",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


# ─────────────────────────────────────────────
#  MAIN PIPELINE
# ─────────────────────────────────────────────
async def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    # ── File / directory paths ───────────────────────────────────────────────
    data_dir       = args.data_dir
    cp_dir         = args.checkpoint_dir
    data_dir.mkdir(parents=True, exist_ok=True)
    cp_dir.mkdir(parents=True, exist_ok=True)

    p1_csv         = data_dir / "twitch_monthly_fact_table.csv"
    p2_csv         = data_dir / "twitch_monthly_fact_table_enriched.csv"
    final_csv      = data_dir / "twitch_monthly_fact_table_final.csv"
    p1_checkpoint  = cp_dir  / "processed.txt"
    p2_checkpoint  = cp_dir  / "enriched.txt"

    # ── Full mode: wipe previous outputs and checkpoints ────────────────────
    if args.mode == "full":
        logger.warning("Mode=FULL — deleting existing outputs and checkpoints for a clean run.")
        for f in [p1_csv, p2_csv, final_csv, p1_checkpoint, p2_checkpoint]:
            if f.exists():
                f.unlink()
                logger.info("  Deleted: %s", f)

    # ── Summary banner ───────────────────────────────────────────────────────
    total_months   = len(args.years) * len(args.months)
    total_channels = total_months * 500
    # Phase 1: 5 leaderboard + 2 pie per channel
    p1_requests    = total_months * 5 + total_channels * 2
    # Phase 2: 1 games API per channel (most channels < 100 games → 1 call)
    p2_requests    = total_channels
    total_requests = p1_requests + (0 if args.skip_phase2 else p2_requests)

    logger.info("=" * 64)
    logger.info("  Twitch Analytics Pipeline")
    logger.info("  Mode        : %s", args.mode)
    logger.info("  Years       : %s", args.years)
    logger.info("  Months      : %s", args.months)
    logger.info("  Scope       : %d months × 500 channels = %d records", total_months, total_channels)
    logger.info("  Est. requests: ~%d  (Phase1=%d  Phase2=%d)", total_requests, p1_requests, p2_requests)
    logger.info("  Est. runtime : ~%.1f – %.1f hours", total_requests / (11 * 3600), total_requests / (9 * 3600))
    logger.info("=" * 64)

    pipeline_start = time.time()

    # ════════════════════════════════════════════════════════════════════════
    #  PHASE 1 — Leaderboard + Pie Top-5  → twitch_monthly_fact_table.csv
    # ════════════════════════════════════════════════════════════════════════
    logger.info("")
    logger.info("▶ Starting Phase 1: Leaderboard + Pie Top-5 ingestion")
    p1_engine = IngestionEngine(
        output_path=p1_csv,
        checkpoint_path=p1_checkpoint,
        mode=args.mode,
        years=args.years,
        months=args.months,
    )
    await p1_engine.run()
    logger.info("✅ Phase 1 complete — %s", p1_csv)

    if args.skip_phase2:
        logger.info("--skip-phase2 set — stopping after Phase 1.")
        logger.info("Total runtime: %.1f min", (time.time() - pipeline_start) / 60)
        return

    # ════════════════════════════════════════════════════════════════════════
    #  PHASE 2 — Games Table API  → twitch_monthly_fact_table_enriched.csv
    # ════════════════════════════════════════════════════════════════════════
    logger.info("")
    logger.info("▶ Starting Phase 2: Games Table API enrichment")
    p2_engine = EnrichmentEngine(
        phase1_path=p1_csv,
        phase2_path=p2_csv,
        checkpoint_path=p2_checkpoint,
    )
    await p2_engine.run()
    logger.info("✅ Phase 2 complete — %s", p2_csv)

    # ════════════════════════════════════════════════════════════════════════
    #  PHASE 3 — Transformer  → twitch_monthly_fact_table_final.csv
    # ════════════════════════════════════════════════════════════════════════
    logger.info("")
    logger.info("▶ Starting Phase 3: Cohort Ranking (transformer)")
    run_transformer(p2_csv, final_csv)
    logger.info("✅ Phase 3 complete — %s", final_csv)

    # ── Final summary ────────────────────────────────────────────────────────
    total_runtime = (time.time() - pipeline_start) / 60
    logger.info("")
    logger.info("=" * 64)
    logger.info("  Pipeline complete in %.1f minutes", total_runtime)
    logger.info("  Deliverable 1 : %s", p1_csv)
    logger.info("  Deliverable 2 : %s", final_csv)
    logger.info("  (Intermediate): %s", p2_csv)
    logger.info("=" * 64)


if __name__ == "__main__":
    asyncio.run(main())
