#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
import logging
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Iterable

import matplotlib.pyplot as plt
import pandas as pd


LOGGER = logging.getLogger("twitch_panel_analysis")

DEFAULT_INPUT = Path("output/twitch_full_panel_enriched.csv")
DEFAULT_OUTPUT_DIR = Path("analysis_output")

KEY_COLUMNS = [
    "channel_id",
    "channel_name",
    "display_name",
    "year",
    "month",
    "month_key",
    "status",
    "rank",
    "average_viewers",
    "hours_watched",
    "hours_streamed",
    "followers",
    "followers_gained",
    "peak_viewers",
    "created",
    "top_game",
    "top_game_hours",
    "top_game_pct",
    "game_count",
    "is_variety_streamer",
    "games_by_avg_viewers_json",
    "games_by_stream_time_json",
    "all_games_json",
]


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )


def normalize_month(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def safe_int(value: Any) -> Optional[int]:
    v = safe_float(value)
    return int(v) if v is not None else None


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def parse_json_maybe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (list, dict)):
        return value
    s = str(value).strip()
    if not s:
        return None
    for loader in (json.loads, ast.literal_eval):
        try:
            return loader(s)
        except Exception:
            continue
    return None


def iter_chunks(path: Path, chunksize: int) -> Iterable[pd.DataFrame]:
    for chunk in pd.read_csv(path, chunksize=chunksize, low_memory=False):
        yield chunk


def ensure_month_key(df: pd.DataFrame) -> pd.DataFrame:
    if "month_key" not in df.columns:
        if "year" in df.columns and "month" in df.columns:
            df["month_key"] = df["year"].astype(str) + df["month"].astype(str).map(normalize_month)
        else:
            df["month_key"] = ""
    return df


def ensure_column(df: pd.DataFrame, col: str, default: Any = None) -> pd.DataFrame:
    if col not in df.columns:
        df[col] = default
    return df


@dataclass
class AnalysisState:
    total_rows: int = 0
    unique_channels: set[int] = None
    columns_seen: set[str] = None
    status_counts: Counter = None
    month_counts: Counter = None
    missing_counts: Counter = None

    rank_nonnull: int = 0
    metrics_nonnull: int = 0
    active_rows: int = 0
    top500_rows: int = 0

    monthly_rows: Counter = None
    monthly_rank_nonnull: Counter = None
    monthly_active_rows: Counter = None
    monthly_avg_viewers_sum: defaultdict = None
    monthly_avg_viewers_count: Counter = None
    monthly_hours_watched_sum: defaultdict = None
    monthly_hours_watched_count: Counter = None

    channel_stats: dict[int, dict[str, float]] = None
    game_hours: Counter = None
    game_mentions: Counter = None
    game_avg_viewers_sum: defaultdict = None
    game_avg_viewers_count: Counter = None
    created_year_counts: Counter = None

    def __post_init__(self) -> None:
        self.unique_channels = set()
        self.columns_seen = set()
        self.status_counts = Counter()
        self.month_counts = Counter()
        self.missing_counts = Counter()
        self.monthly_rows = Counter()
        self.monthly_rank_nonnull = Counter()
        self.monthly_active_rows = Counter()
        self.monthly_avg_viewers_sum = defaultdict(float)
        self.monthly_avg_viewers_count = Counter()
        self.monthly_hours_watched_sum = defaultdict(float)
        self.monthly_hours_watched_count = Counter()
        self.channel_stats = {}
        self.game_hours = Counter()
        self.game_mentions = Counter()
        self.game_avg_viewers_sum = defaultdict(float)
        self.game_avg_viewers_count = Counter()
        self.created_year_counts = Counter()


def init_channel_stats() -> dict[str, float]:
    return {
        "rows": 0,
        "rank_min": math.inf,
        "avg_viewers_sum": 0.0,
        "avg_viewers_count": 0.0,
        "hours_watched_sum": 0.0,
        "hours_watched_count": 0.0,
        "followers_sum": 0.0,
        "followers_count": 0.0,
        "peak_viewers_max": 0.0,
        "active_rows": 0,
        "top500_rows": 0,
    }


def update_channel_stats(state: AnalysisState, row: pd.Series) -> None:
    cid = safe_int(row.get("channel_id"))
    if cid is None:
        return
    st = state.channel_stats.setdefault(cid, init_channel_stats())
    st["rows"] += 1

    rank = safe_float(row.get("rank"))
    if rank is not None:
        st["rank_min"] = min(st["rank_min"], rank)

    av = safe_float(row.get("average_viewers"))
    if av is not None:
        st["avg_viewers_sum"] += av
        st["avg_viewers_count"] += 1

    hw = safe_float(row.get("hours_watched"))
    if hw is not None:
        st["hours_watched_sum"] += hw
        st["hours_watched_count"] += 1

    fol = safe_float(row.get("followers"))
    if fol is not None:
        st["followers_sum"] += fol
        st["followers_count"] += 1

    peak = safe_float(row.get("peak_viewers"))
    if peak is not None:
        st["peak_viewers_max"] = max(st["peak_viewers_max"], peak)

    status = safe_str(row.get("status"))
    if status != "inactive":
        st["active_rows"] += 1
    if status == "active_top500":
        st["top500_rows"] += 1


def update_game_stats(state: AnalysisState, row: pd.Series) -> None:
    payload = parse_json_maybe(row.get("all_games_json"))
    if not payload:
        return

    if isinstance(payload, dict):
        payload = payload.get("rows") or payload.get("data") or []

    if not isinstance(payload, list):
        return

    for item in payload:
        game = None
        hours = None
        avg_viewers = None

        if isinstance(item, dict):
            game = safe_str(item.get("game") or item.get("title") or item.get("name"))
            hours = safe_float(item.get("hours") or item.get("streamtime"))
            avg_viewers = safe_float(item.get("avg_viewers") or item.get("avgviewers"))
        elif isinstance(item, (list, tuple)) and item:
            game = safe_str(item[0]) if len(item) > 0 else ""
            hours = safe_float(item[1]) if len(item) > 1 else None
            avg_viewers = safe_float(item[3]) if len(item) > 3 else None
        else:
            continue

        if not game:
            continue

        state.game_mentions[game] += 1
        if hours is not None:
            state.game_hours[game] += float(hours)
        if avg_viewers is not None:
            state.game_avg_viewers_sum[game] += float(avg_viewers)
            state.game_avg_viewers_count[game] += 1


def parse_created_year(value: Any) -> Optional[int]:
    s = safe_str(value)
    if not s:
        return None
    try:
        ts = pd.to_datetime(s, errors="coerce")
        if pd.isna(ts):
            return None
        return int(ts.year)
    except Exception:
        return None


def analyze_csv(input_path: Path, chunksize: int) -> AnalysisState:
    state = AnalysisState()

    for idx, chunk in enumerate(iter_chunks(input_path, chunksize), start=1):
        chunk = ensure_month_key(chunk)
        state.total_rows += len(chunk)
        state.columns_seen.update(chunk.columns)

        if "channel_id" in chunk.columns:
            state.unique_channels.update(
                [x for x in chunk["channel_id"].dropna().map(safe_int).tolist() if x is not None]
            )

        if "status" in chunk.columns:
            state.status_counts.update(chunk["status"].fillna("UNKNOWN").astype(str).str.strip())

        if "month_key" in chunk.columns:
            state.month_counts.update(chunk["month_key"].fillna("UNKNOWN").astype(str))

        if "rank" in chunk.columns:
            state.rank_nonnull += int(chunk["rank"].notna().sum())

        metric_cols = [
            c for c in ("average_viewers", "hours_watched", "hours_streamed", "followers", "followers_gained", "peak_viewers")
            if c in chunk.columns
        ]
        if metric_cols:
            state.metrics_nonnull += int(chunk[metric_cols].notna().any(axis=1).sum())

        if "status" in chunk.columns:
            state.active_rows += int((chunk["status"].fillna("inactive").astype(str) != "inactive").sum())
            state.top500_rows += int((chunk["status"].fillna("").astype(str) == "active_top500").sum())

        for col in chunk.columns:
            state.missing_counts[col] += int(chunk[col].isna().sum())

        if "month_key" in chunk.columns:
            for mk, sub in chunk.groupby("month_key", dropna=False):
                mk = safe_str(mk) or "UNKNOWN"
                state.monthly_rows[mk] += len(sub)
                if "rank" in sub.columns:
                    state.monthly_rank_nonnull[mk] += int(sub["rank"].notna().sum())
                if "status" in sub.columns:
                    state.monthly_active_rows[mk] += int((sub["status"].fillna("inactive").astype(str) != "inactive").sum())
                if "average_viewers" in sub.columns:
                    vals = pd.to_numeric(sub["average_viewers"], errors="coerce")
                    state.monthly_avg_viewers_sum[mk] += float(vals.sum(skipna=True))
                    state.monthly_avg_viewers_count[mk] += int(vals.notna().sum())
                if "hours_watched" in sub.columns:
                    vals = pd.to_numeric(sub["hours_watched"], errors="coerce")
                    state.monthly_hours_watched_sum[mk] += float(vals.sum(skipna=True))
                    state.monthly_hours_watched_count[mk] += int(vals.notna().sum())

        if "created" in chunk.columns:
            years = chunk["created"].map(parse_created_year)
            state.created_year_counts.update([y for y in years.dropna().astype(int).tolist()])

        if "all_games_json" in chunk.columns:
            for _, row in chunk.iterrows():
                update_game_stats(state, row)

        for _, row in chunk.iterrows():
            update_channel_stats(state, row)

        if idx % 5 == 0:
            LOGGER.info("Processed %d chunks | rows=%d | unique_channels=%d", idx, state.total_rows, len(state.unique_channels))

    return state


def top_channels_frame(state: AnalysisState, top_n: int, metric: str = "avg_viewers") -> pd.DataFrame:
    records = []
    for cid, st in state.channel_stats.items():
        if metric == "avg_viewers":
            score = st["avg_viewers_sum"] / st["avg_viewers_count"] if st["avg_viewers_count"] else None
        elif metric == "hours_watched":
            score = st["hours_watched_sum"]
        elif metric == "followers":
            score = st["followers_sum"] / st["followers_count"] if st["followers_count"] else None
        else:
            score = None

        records.append({
            "channel_id": cid,
            "rows": st["rows"],
            "rank_min": None if st["rank_min"] == math.inf else st["rank_min"],
            "avg_viewers": (st["avg_viewers_sum"] / st["avg_viewers_count"]) if st["avg_viewers_count"] else None,
            "hours_watched": st["hours_watched_sum"],
            "followers_avg": (st["followers_sum"] / st["followers_count"]) if st["followers_count"] else None,
            "peak_viewers_max": st["peak_viewers_max"],
            "active_rows": st["active_rows"],
            "top500_rows": st["top500_rows"],
            "score": score,
        })

    df = pd.DataFrame(records)
    if df.empty:
        return df
    return df.sort_values("score", ascending=False).head(top_n).reset_index(drop=True)


def games_frame(state: AnalysisState, top_n: int, mode: str = "hours") -> pd.DataFrame:
    records = []
    keys = set(state.game_mentions.keys()) | set(state.game_hours.keys()) | set(state.game_avg_viewers_sum.keys())
    for game in keys:
        records.append({
            "game": game,
            "mentions": int(state.game_mentions.get(game, 0)),
            "total_hours": float(state.game_hours.get(game, 0.0)),
            "avg_viewers_mean": (
                state.game_avg_viewers_sum[game] / state.game_avg_viewers_count[game]
                if state.game_avg_viewers_count.get(game, 0) else None
            ),
        })

    df = pd.DataFrame(records)
    if df.empty:
        return df

    if mode == "hours":
        return df.sort_values("total_hours", ascending=False).head(top_n).reset_index(drop=True)
    return df.sort_values("mentions", ascending=False).head(top_n).reset_index(drop=True)


def monthly_frame(state: AnalysisState) -> pd.DataFrame:
    records = []
    for mk in sorted(state.monthly_rows.keys()):
        rows = state.monthly_rows[mk]
        rank_rows = state.monthly_rank_nonnull.get(mk, 0)
        active_rows = state.monthly_active_rows.get(mk, 0)
        av_count = state.monthly_avg_viewers_count.get(mk, 0)
        hw_count = state.monthly_hours_watched_count.get(mk, 0)
        records.append({
            "month_key": mk,
            "rows": rows,
            "rank_coverage": rank_rows / rows if rows else 0.0,
            "panel_coverage": active_rows / rows if rows else 0.0,
            "avg_viewers_mean": (state.monthly_avg_viewers_sum.get(mk, 0.0) / av_count) if av_count else None,
            "hours_watched_mean": (state.monthly_hours_watched_sum.get(mk, 0.0) / hw_count) if hw_count else None,
        })
    return pd.DataFrame(records)


def save_tables(state: AnalysisState, outdir: Path, top_n: int) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    monthly = monthly_frame(state)
    top_avg = top_channels_frame(state, top_n, metric="avg_viewers")
    top_hours = top_channels_frame(state, top_n, metric="hours_watched")
    top_games = games_frame(state, top_n, mode="hours")

    monthly.to_csv(outdir / "monthly_summary.csv", index=False)
    top_avg.to_csv(outdir / "top_channels_by_avg_viewers.csv", index=False)
    top_hours.to_csv(outdir / "top_channels_by_hours_watched.csv", index=False)
    top_games.to_csv(outdir / "top_games_by_hours.csv", index=False)

    status_df = pd.DataFrame(state.status_counts.items(), columns=["status", "count"]).sort_values("count", ascending=False)
    status_df.to_csv(outdir / "status_distribution.csv", index=False)


def save_plots(state: AnalysisState, outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)

    if state.status_counts:
        s = pd.Series(state.status_counts).sort_values(ascending=False)
        plt.figure(figsize=(10, 5))
        s.plot(kind="bar")
        plt.title("Status distribution")
        plt.xlabel("Status")
        plt.ylabel("Rows")
        plt.tight_layout()
        plt.savefig(outdir / "status_distribution.png", dpi=160)
        plt.close()

    monthly = monthly_frame(state)
    if not monthly.empty:
        plt.figure(figsize=(12, 5))
        plt.plot(monthly["month_key"], monthly["panel_coverage"], marker="o")
        plt.xticks(rotation=90)
        plt.title("Monthly panel coverage")
        plt.xlabel("Month")
        plt.ylabel("Coverage")
        plt.tight_layout()
        plt.savefig(outdir / "monthly_panel_coverage.png", dpi=160)
        plt.close()

        plt.figure(figsize=(12, 5))
        plt.plot(monthly["month_key"], monthly["rank_coverage"], marker="o")
        plt.xticks(rotation=90)
        plt.title("Monthly rank coverage")
        plt.xlabel("Month")
        plt.ylabel("Coverage")
        plt.tight_layout()
        plt.savefig(outdir / "monthly_rank_coverage.png", dpi=160)
        plt.close()

    top_games = games_frame(state, 20, mode="hours")
    if not top_games.empty:
        plt.figure(figsize=(12, 6))
        plt.barh(top_games["game"][::-1], top_games["total_hours"][::-1])
        plt.title("Top games by total hours")
        plt.xlabel("Total hours")
        plt.tight_layout()
        plt.savefig(outdir / "top_games_by_hours.png", dpi=160)
        plt.close()

    top_channels = top_channels_frame(state, 20, metric="avg_viewers")
    if not top_channels.empty:
        plt.figure(figsize=(12, 6))
        plt.barh(top_channels["channel_id"].astype(str)[::-1], top_channels["avg_viewers"][::-1])
        plt.title("Top channels by average viewers")
        plt.xlabel("Average viewers")
        plt.tight_layout()
        plt.savefig(outdir / "top_channels_by_avg_viewers.png", dpi=160)
        plt.close()

    rank_values = [st["rank_min"] for st in state.channel_stats.values() if st["rank_min"] != math.inf]
    if rank_values:
        plt.figure(figsize=(12, 5))
        plt.hist(rank_values, bins=50)
        plt.title("Channel best-rank distribution")
        plt.xlabel("Best rank")
        plt.ylabel("Channels")
        plt.tight_layout()
        plt.savefig(outdir / "rank_histogram.png", dpi=160)
        plt.close()


def write_report(state: AnalysisState, outdir: Path, top_n: int) -> Path:
    outdir.mkdir(parents=True, exist_ok=True)
    monthly = monthly_frame(state)
    top_avg = top_channels_frame(state, top_n, metric="avg_viewers")
    top_hours = top_channels_frame(state, top_n, metric="hours_watched")
    top_games = games_frame(state, top_n, mode="hours")

    report_path = outdir / "analysis_report.md"
    with report_path.open("w", encoding="utf-8") as f:
        f.write("# Twitch Full Panel Enriched Analysis\n\n")
        f.write("## Overview\n")
        f.write(f"- Total rows: **{state.total_rows:,}**\n")
        f.write(f"- Unique channels: **{len(state.unique_channels):,}**\n")
        f.write(f"- Columns seen: **{len(state.columns_seen):,}**\n")
        f.write(f"- Rows with rank: **{state.rank_nonnull:,}**\n")
        f.write(f"- Rows with at least one metric: **{state.metrics_nonnull:,}**\n")
        f.write(f"- Active rows: **{state.active_rows:,}**\n")
        f.write(f"- Inactive rows: **{state.total_rows - state.active_rows:,}**\n\n")

        f.write("## Status distribution\n")
        for status, count in state.status_counts.most_common():
            f.write(f"- {status}: {count:,}\n")
        f.write("\n")

        f.write("## Missingness (key columns)\n")
        key_cols = [c for c in KEY_COLUMNS if c in state.columns_seen]
        for col in key_cols:
            miss = state.missing_counts.get(col, 0)
            pct = (miss / state.total_rows * 100.0) if state.total_rows else 0.0
            f.write(f"- {col}: {miss:,} missing ({pct:.2f}%)\n")
        f.write("\n")

        f.write("## Monthly coverage\n")
        if not monthly.empty:
            f.write(monthly.to_markdown(index=False))
            f.write("\n\n")

        f.write("## Created year distribution\n")
        if state.created_year_counts:
            for year, count in state.created_year_counts.most_common():
                f.write(f"- {year}: {count:,}\n")
        else:
            f.write("- No created timestamps found.\n")
        f.write("\n")

        f.write("## Top channels by average viewers\n")
        if not top_avg.empty:
            f.write(top_avg.to_markdown(index=False))
            f.write("\n\n")
        else:
            f.write("- No channel averages available.\n\n")

        f.write("## Top channels by hours watched\n")
        if not top_hours.empty:
            f.write(top_hours.to_markdown(index=False))
            f.write("\n\n")
        else:
            f.write("- No channel hours available.\n\n")

        f.write("## Top games by hours\n")
        if not top_games.empty:
            f.write(top_games.to_markdown(index=False))
            f.write("\n\n")
        else:
            f.write("- No game data available.\n\n")

        f.write("## Suggested interpretation\n")
        f.write("- `panel_coverage` tells you how much of the panel is active/non-inactive.\n")
        f.write("- `rank_coverage` shows how many rows have a rank value.\n")
        f.write("- If `created` is sparse, the Twitch Helix enrichment step may need retry tuning.\n")

    return report_path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze twitch_full_panel_enriched.csv")
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to twitch_full_panel_enriched.csv")
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for outputs")
    p.add_argument("--chunksize", type=int, default=10_000, help="CSV chunk size")
    p.add_argument("--top-n", type=int, default=20, help="Top N rows for summaries")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    if not args.input.exists():
        raise FileNotFoundError(f"Input file not found: {args.input}")

    LOGGER.info("Analyzing %s", args.input)
    state = analyze_csv(args.input, chunksize=args.chunksize)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    save_tables(state, args.output_dir, args.top_n)
    save_plots(state, args.output_dir)
    report_path = write_report(state, args.output_dir, args.top_n)

    LOGGER.info("Saved report -> %s", report_path)
    LOGGER.info("Done")


if __name__ == "__main__":
    main()
