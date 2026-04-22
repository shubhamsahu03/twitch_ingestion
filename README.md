# Twitch Full Panel Pipeline

> A production-grade async data engineering system that reconstructs complete 48-month longitudinal histories for every channel that ever reached the Twitch Top 500 — sourcing, recovering, and enriching **116,592 channel-month records** across four live APIs with zero estimation or interpolation.

---

## Dataset at a Glance

| Metric | Value |
|---|---|
| Total channel-month records | **116,592** |
| Unique channels tracked | **2,429** |
| Timeline span | **Jan 2022 → Dec 2025 (48 months)** |
| Channels with full 48-row histories | **100%** |
| Real-data coverage | **71.1%** — 82,845 rows with measured activity |
| Active · Top 500 | **24,002** rows |
| Active · Outside Top 500 | **58,843** rows |
| Confirmed inactive | **33,747** rows (28.9%) |
| Columns per record | **28** |
| Total viewer-hours indexed | **> 10 billion** |
| Total stream-hours indexed | **> 1.9 million** |
| Distinct games tracked | **1,000+** |
| Twitch account creation dates resolved | **96.7%** of channels |

Every channel has exactly 48 rows. Every month is assigned a validated status. No gaps, no interpolation, no estimates.

---

## Problem Statement

Twitch exposes no historical ranked data through any public API. SullyGnome publishes monthly leaderboards as paginated JSON endpoints but data for channels outside the current visible window is silently absent — and even channels that did appear in historical leaderboards frequently have missing months due to temporary inactivity, leaderboard rank fluctuation, or API edge cases. Simply scraping the Top 500 per month produces a sparse, biased panel that understates channel longevity and overstates churn.

This pipeline solves the problem in four independent phases. Each phase has its own checkpoint, caching layer, and recovery system. Together they maximise real-data coverage across the full four-year window while maintaining a strict "no fabrication" constraint — every value in the output either came from a live API response or is explicitly null.

---

## Repository Structure

```
.
├── twitch_full_panel_pipeline.py        # Main pipeline — Phases 1, 2, 3
├── enrich.py                            # Game + Twitch Helix enrichment — Phase 4
├── analyze_twitch_full_panel_enriched.py  # Analysis, coverage report, chart export
├── drop_columns.py                      # Utility: remove computed columns before re-run
├── _env                                 # Credentials template — copy to .env
├── requirements.txt
│
├── output/                              # Auto-created by pipeline
│   ├── twitch_full_panel.csv                ← Phase 3 output (core panel)
│   ├── twitch_full_panel_cleaned.csv        ← After drop_columns (enrichment input)
│   ├── twitch_full_panel_enriched.csv       ← Phase 4 output (final deliverable)
│   └── parquet/                             ← Partitioned by month_key
│       └── month_key=2022january/
│           └── part-*.parquet
│
├── cache/                               # SQLite response cache — auto-created
│   ├── api_cache.sqlite3                ← Pipeline pages + rank maps (24 h / 7 d TTL)
│   └── enrich_cache.sqlite3             ← Twitch Helix responses (30 d TTL)
│
├── checkpoints/                         # Crash-safe resume state — auto-created
│   └── rank_progress.json               ← Per-month completion + mid-month offset
│
├── logs/                                # Structured log output — auto-created
│   ├── twitch_full_panel_pipeline.log
│   └── twitch_enrich.log
│
└── analysis_output/                     # Charts and markdown report — auto-created
```

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Add credentials (Twitch account creation dates require Helix API access)
cp _env .env
# Edit .env:  TWITCH_CLIENT_ID=...   TWITCH_CLIENT_SECRET=...

# 3. Full pipeline run
python twitch_full_panel_pipeline.py --mode full

# 4. Enrich with per-game breakdown data
python enrich.py --input output/twitch_full_panel_cleaned.csv

# 5. Generate coverage report and charts
python analyze_twitch_full_panel_enriched.py --input output/twitch_full_panel_enriched.csv

# 6. Resume after a crash or interruption (picks up mid-month)
python twitch_full_panel_pipeline.py --mode incremental

# 7. Test with one month before committing to a full run
python twitch_full_panel_pipeline.py --mode full --years 2024 --months january
```

---

## Architecture

The system is split into four sequential phases, each independently resumable.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Phase 1 — RankIndexer                                                      │
│                                                                             │
│  For each of 48 months, scans the SullyGnome leaderboard across 5 rank     │
│  bands until every target channel is located or the rank ceiling is hit.    │
│                                                                             │
│  Band 1   rank      0 – 500     concurrency=8   retry=1                    │
│  Band 2   rank    500 – 2,000   concurrency=8   retry=1                    │
│  Band 3   rank  2,000 – 5,000   concurrency=8   retry=4                    │
│  Band 4   rank  5,000 – 10,000  concurrency=5   retry=4                    │
│  Band 5   rank 10,000 – 20,000  concurrency=5   retry=2  (reverse order)   │
│                                                                             │
│  Early stop: fired by any of 3 independent heuristics (see below).         │
│  Recovery pass: scans ranks 12,000–30,000 for channels still missing.      │
│  Output: rank_maps + metrics_maps keyed by (channel_id, month_key).        │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────────────┐
│  Phase 2 — CsvLoader + ChannelDataFetcher                                   │
│                                                                             │
│  Loads the existing fact table CSV into two in-memory lookup maps:         │
│    csv_rank_map:     (channel_id, month_key) → rank                        │
│    csv_metrics_map:  (channel_id, month_key) → metric dict                 │
│                                                                             │
│  For any (channel, month) pair with a rank but no metrics in the CSV,      │
│  fetches the per-channel monthly summary from the SullyGnome channel API.  │
│  Tries channel_slug first; falls back to slugify(display_name) on 404.     │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────────────┐
│  Phase 3 — PanelBuilder                                                     │
│                                                                             │
│  Merges all sources into a strict 48-row-per-channel panel DataFrame.      │
│  Conflict resolution: live leaderboard data wins field-by-field over CSV.  │
│  Assigns status labels (see Status System below).                          │
│  Four hard-fail validation assertions before writing output.               │
│  Writes twitch_full_panel.csv + Parquet partitioned by month_key.          │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────────────┐
│  Phase 4 — EnrichmentEngine  (enrich.py, separate script)                  │
│                                                                             │
│  Streams input CSV in chunks of 5,000 rows (memory-safe for large files).  │
│  Per-row skip guard: rows with all_games_json already populated are        │
│  skipped entirely — no redundant fetches on re-runs.                       │
│  Fetches full per-game breakdown from SullyGnome games table API.          │
│  Resolves Twitch account creation dates via Helix /users (batch 100).      │
│  Writes enriched CSV + Parquet as it goes via a streaming chunk writer.    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Status System

Every row in the output carries exactly one of four status values, assigned deterministically by `PanelBuilder`:

| Status | Condition | Rows |
|---|---|---|
| `active_top500` | Rank ≤ 500 and at least one metric present | 24,002 |
| `active_not_top500` | Rank > 500 and at least one metric present | 58,843 |
| `active_unranked` | No rank found, but channel API returned metrics | small subset |
| `inactive` | No metrics from any source for that month | 33,747 |

A row is `inactive` only when both the leaderboard scan and the CSV produced no usable metrics for that channel-month pair. Ranked rows that are missing metrics trigger a logged warning and are never silently assigned `inactive`.

---

## Conflict Resolution

When two sources provide data for the same `(channel_id, month_key)`, the merge follows a strict priority order:

1. The CSV metrics dict is used as the base — the existing fact table is the starting point for all fields.
2. Live leaderboard values are applied field-by-field, overwriting any CSV value where the live value is non-null.
3. The `source` field records the winner: `"leaderboard"` if any live value was applied, `"csv"` otherwise.

This means a channel with enriched game data in the CSV will retain those fields even when the live leaderboard response does not include them, while fresh viewer counts from the leaderboard will overwrite stale CSV values.

---

## Early Stop Heuristics

The `RankIndexer` does not scan all 20,000 rank positions for every month. Three independent signals halt the scan early once the pipeline has located all the hits it reasonably can:

**Consecutive-no-new threshold.** After 10 consecutive pages (20 pages at ranks > 10,000) with no new target channels found, the current band scan stops. Ten pages covering 1,000 rows with zero hits is a reliable signal that the remaining targets are not in this part of the rank distribution.

**Phase-5 zero-yield stop.** If the last three consecutive batches in the highest rank band each produced zero new hits, the month scan is considered exhausted.

**Sparse-tail stop.** In rank bands 4 and 5, if the last five batches combined yielded fewer than three new hits total, scanning continues no further. This catches the sparse mid-range where target channels are scattered too thinly to justify the API cost.

After all five bands complete with channels still missing, a **dedicated recovery pass** scans ranks 12,000–30,000 sequentially and stops after five consecutive zero-yield batches. This catches channels that fell below the primary scan window due to the early-stop heuristics firing too aggressively.

---

## Engineering Components

### Adaptive Rate Limiter
Token-bucket controller that adjusts concurrency live. A 429 response triggers a 30-second full backoff. A 5xx spike or timeout reduces the effective rate by 20%. A streak of 20 consecutive successful requests increments the rate by 1. The rate is capped between 7 and 14 requests per second with hard floor and ceiling.

### Circuit Breaker
Three HTTP 403 responses within 60 seconds, or 10 consecutive failures of any kind, opens the circuit for 5 minutes. On expiry the breaker enters `HALF_OPEN` and probes with 10 requests at 1 rps. Full closure requires all 10 probes to succeed. This prevents IP-level bans during extended error windows without requiring manual intervention.

### SQLite Cache — WAL Mode
Both `api_cache.sqlite3` (pipeline) and `enrich_cache.sqlite3` (enrichment) use SQLite with `PRAGMA journal_mode=WAL` for safe concurrent reads under async access. All entries are TTL-keyed:

| Cache key pattern | TTL |
|---|---|
| `leaderboard:{month}:{offset}` | 24 hours |
| `rankmap:{month}` | 7 days |
| `monthresult:{month}` | 7 days |
| `channel:{slug}:{month}` | 24 hours |
| Twitch Helix user responses | 30 days |

On incremental runs with a warm cache, responses from cache are indistinguishable from live API responses at the application layer. Full re-run time drops from ~3 hours to ~15–20 minutes.

### Crash-Safe Progress Checkpoint
`checkpoints/rank_progress.json` tracks two distinct states per month: `completed` (full scan done, month is closed) and `offsets` (the last successfully processed page offset, for months interrupted mid-scan). All writes use atomic replace — the new state is written to a `.tmp` file first, then `tmp.replace(path)` performs an OS-level atomic rename. A crash during the write leaves the prior checkpoint intact rather than producing a corrupt file.

### Streaming Chunk Writer
`enrich.py` reads the input CSV in chunks of 5,000 rows and writes output incrementally. Peak memory stays below 1 GB even on the full 116,592-row dataset. Parquet output is partitioned by `month_key` using PyArrow's `write_to_dataset`, enabling column-pruned partition-filtered reads in downstream tools like DuckDB or Polars.

### Retry Depth Variance
The number of retries per page fetch is tiered by rank depth. Low-rank pages (0–8,000) get 1 retry — they are reliable and fast, extra retries only waste time. Mid-range pages (8,000–14,000) get 4 retries because error rates are higher in this band. High-rank pages (14,000–20,000) get 2 retries — these rarely contain target channels, so extra retries are low-value.

### Slug Fallback
`ChannelDataFetcher.fetch_one` first attempts the stored `channel_slug`. On 404 it generates a fallback slug from `display_name` via `slugify_display_name()` — lowercase, whitespace stripped, non-alphanumeric characters removed — and retries once. This recovers channels whose URL slugs changed between collection and enrichment, or were stored inconsistently in the source data.

### MonthStats
A per-month `@dataclass` that accumulates `channels_found`, `api_calls`, `cache_hits`, `early_stop_triggered`, `stop_reason`, and `elapsed_s` throughout each scan. Logged in full at month completion. Useful for diagnosing outlier months — February 2023 at 53.8% coverage and March 2024 at 50.8% are both traceable in the logs to specific early-stop events during ingestion.

### Panel Builder Validation
`PanelBuilder.build()` performs four hard assertions before writing any output. A failure on any assertion raises `ValueError` immediately rather than silently writing a corrupt dataset:

1. Row count must equal exactly `len(channels) × len(month_keys)`.
2. No duplicate `(channel_id, year, month)` composite keys.
3. No rank values ≤ 0.
4. Ranked rows missing all six core metrics trigger a logged warning with a percentage of affected rows.

---

## Data Sources

### 1. SullyGnome Leaderboard API
```
GET /api/tables/channeltables/getchannels/{yearmonth}/0/1/3/desc/{offset}/100
```
The primary data source. Called across five rank bands per month. Returns `channel_id`, `display_name`, `followers`, `followers_gained`, `avgviewers`, `maxviewers`, `streamedminutes`, `viewminutes`, `language`, `mature` per row. Response extraction handles multiple known payload shapes (`data`, `aaData`, `rows`, `results`, `items`) for forward compatibility.

### 2. SullyGnome Channel API
```
GET /api/channels/{slug}/{yearmonth}
```
Per-channel monthly summary. Called by `ChannelDataFetcher` for channels that have a rank but no accompanying metrics from the leaderboard response. Metric extraction handles camelCase, snake_case, and mixed-case field names across API versions.

### 3. SullyGnome Games Table API
```
GET /api/tables/channeltables/games/{yearmonth}/{channelId}/%20/1/2/desc/{offset}/100
```
Per-channel, per-month game breakdown, paginated until `recordsTotal` is exhausted. Returns `streamtime`, `viewtime`, `avgviewers`, `maxviewers` per game entry. Called by `enrich.py` only; the per-row skip guard prevents re-fetching rows where `all_games_json` is already populated.

### 4. Twitch Helix API
```
GET https://api.twitch.tv/helix/users?login={slug}&login={slug}...
```
Resolves channel slugs to Twitch account creation dates. Called in batches of 100 logins per request. Token refreshes automatically on 401. Results cached in `enrich_cache.sqlite3` for 30 days. Requires `TWITCH_CLIENT_ID` and `TWITCH_CLIENT_SECRET` in `.env`.

---

## Output Schema

### Core Panel — `twitch_full_panel.csv`

#### Identity
| Column | Type | Description |
|---|---|---|
| `channel_id` | Int64 | SullyGnome internal channel ID |
| `channel_name` | str | Normalised slug — lowercase, whitespace stripped |
| `display_name` | str | Display name as shown on Twitch |
| `year` | Int64 | Observation year (2022–2025) |
| `month` | str | Observation month (`january` – `december`) |
| `month_key` | str | Composite sort key, e.g. `2024march` |
| `month_order` | Int64 | Integer month index (1–12) for chronological sorting |

#### Performance Metrics
| Column | Type | Description |
|---|---|---|
| `rank` | Int64 | Leaderboard rank for that month; null if not ranked |
| `average_viewers` | float | Average concurrent viewers |
| `peak_viewers` | Int64 | Peak concurrent viewers |
| `hours_streamed` | float | Total hours streamed (`streamedminutes ÷ 60`) |
| `hours_watched` | float | Total viewer-hours (`viewminutes ÷ 60`) |
| `followers` | Int64 | Total followers at month end |
| `followers_gained` | Int64 | Net follower change during the month |

#### Channel Metadata
| Column | Type | Description |
|---|---|---|
| `status` | str | `active_top500` / `active_not_top500` / `inactive` |
| `language` | str | Primary broadcast language |
| `mature` | bool | Mature content flag |
| `created` | str | Twitch account creation date, ISO 8601 — requires `.env` |

#### Within-Month Cohort Ranks *(Top 500 channels only)*
| Column | Type | Description |
|---|---|---|
| `peak_viewer_rank` | Int64 | Rank by peak viewers within that month's Top 500; 1 = highest |
| `avg_viewer_rank` | Int64 | Rank by average viewers; ties share the top rank |
| `follower_rank` | Int64 | Rank by total followers |
| `follower_gain_rank` | Int64 | Rank by followers gained |

---

### Enriched Panel — `twitch_full_panel_enriched.csv`

All core columns plus:

| Column | Format | Description |
|---|---|---|
| `all_games_json` | `[["Game", hours, avg_viewers, peak_viewers, view_minutes], ...]` | Full game breakdown, all games, sorted by hours descending |
| `games_by_stream_time_json` | `[["Game", hours], ...]` | Games sorted by hours streamed |
| `games_by_avg_viewers_json` | `[["Game", avg_viewers], ...]` | Games sorted by average viewers |
| `top_game` | str | Most-streamed game that month |
| `top_game_hours` | float | Hours spent on `top_game` |
| `top_game_pct` | float | `top_game` share of total stream hours (%) |
| `game_count` | Int64 | Distinct games streamed that month |
| `is_variety_streamer` | bool | `True` when `top_game_pct < 75%` and `game_count > 2` |

---

## Performance

| Metric | Value |
|---|---|
| Total records produced | 116,592 |
| Total API requests — full run | ~96,000 |
| Sustained request rate | 10–11 rps |
| Concurrency — low rank bands (0–8k) | 8 simultaneous |
| Concurrency — high rank bands (8k+) | 5 simultaneous |
| Per-request jitter | 100–400 ms |
| Batch pause | 3–5 s every 300 records |
| Expected full-run time | ~2.5–3 hours |
| Incremental run (warm cache) | ~15–20 minutes |
| Peak memory — enrichment phase | < 1 GB (streaming writer) |

---

## Dataset Highlights

All statistics are derived from `analysis_report.md`, computed against actual pipeline output.

### Coverage by Status
| Status | Rows | Share |
|---|---|---|
| `active_top500` | 24,002 | 20.6% |
| `active_not_top500` | 58,843 | 50.5% |
| `inactive` | 33,747 | 28.9% |

The `active_not_top500` category is the single largest share of the dataset. These rows exist because the pipeline scans to rank 20,000, not just 500 — meaning channels that were measurably active but outside the elite tier are fully represented.

### Viewer Trends (2022–2025)
Average viewers within the active panel declined from ~5,285 in January 2022 to ~3,735 by April 2025 — a **29% decrease** over four years. Hours watched tracked the same direction, falling from ~651,000 viewer-hours per active channel per month in early 2022 to ~430,000 by 2025. The decline is consistent across all twelve calendar months, ruling out a seasonal explanation.

### Monthly Coverage Variance
Most months sit between 70–77% rank coverage. Two outliers stand out: **February 2023 at 53.8%** and **March 2024 at 50.8%**. Both are traceable in `logs/twitch_full_panel_pipeline.log` to early-stop events triggered by elevated API error rates during those ingestion runs. Targeting these specific months with `--mode incremental --years 2023 --months february` will improve coverage on a re-run.

### Top Channels by Average Viewers
The highest-averaging channel recorded **149,037 average concurrent viewers** across its active months, with a peak of **749,621**. Three channels exceeded **85,000 average viewers** sustained across multiple years. The top channel by accumulated viewer-hours reached **509 million** over the four-year window. Every one of the top 20 channels by hours watched has a complete 48-row history.

### Game Landscape
| Game | Total Hours Streamed | Avg Viewers |
|---|---|---|
| Just Chatting | 1,944,710 | 3,305 |
| League of Legends | 863,964 | 3,643 |
| Grand Theft Auto V | 748,899 | 4,109 |
| VALORANT | 632,348 | 3,222 |
| Counter-Strike | 423,061 | 4,006 |
| Fortnite | 416,734 | 3,661 |
| Rust | 393,459 | 2,102 |
| Escape from Tarkov | 392,831 | 1,904 |
| Dota 2 | 354,284 | 3,958 |
| World of Warcraft | 348,914 | 2,404 |

Just Chatting accounts for **1.94 million stream-hours** — more than the next three titles combined. GTA V leads all games on average viewers despite ranking third by hours, driven by roleplay server peaks.

### Channel Creation Cohorts
96.7% of channels have a resolved Twitch creation date (only 3,812 out of 116,592 rows are missing `created`). The largest creation cohorts are 2016 (9,024 channel-months) and 2020 (7,920), reflecting the 2016 streaming industry growth wave and the 2020 pandemic surge. The dataset spans channels created as far back as 2007.

---

## CLI Reference

### `twitch_full_panel_pipeline.py`

```
python twitch_full_panel_pipeline.py [OPTIONS]

  --mode {full,incremental}     full = fresh run (ignores checkpoints)
                                incremental = resume from checkpoint  [default]
  --input PATH                  Existing fact table CSV to seed from
  --output-dir PATH             Output directory  [default: output/]
  --years INT [INT ...]         Years to process  [default: 2022 2023 2024 2025]
  --months MONTH [MONTH ...]    Months to process  [default: all 12]
  --max-rank INT                Rank ceiling for leaderboard scan  [default: 20000]
  --concurrency INT             Max simultaneous requests  [default: 8]
  --max-api-calls INT           Global API call budget  [default: 15000]
  --force                       Ignore checkpoints; re-fetch all months
  --no-parquet                  Skip Parquet output
  --no-cache                    Disable SQLite response cache
  --progress PATH               Checkpoint file path
  --cache-db PATH               SQLite cache database path
  --log-level {DEBUG,INFO,WARNING,ERROR}  [default: INFO]
```

### `enrich.py`

```
python enrich.py [OPTIONS]

  --input PATH                          [default: output/twitch_full_panel_cleaned.csv]
  --output PATH                         [default: output/twitch_full_panel_enriched.csv]
  --chunksize INT                       Rows per processing chunk  [default: 5000]
  --status-filter STATUS[,STATUS,...]   Only process rows with these status values
  --overwrite-existing                  Re-fetch rows that already have game data
  --log-level {DEBUG,INFO,WARNING,ERROR}
```

### `analyze_twitch_full_panel_enriched.py`

```
python analyze_twitch_full_panel_enriched.py [OPTIONS]

  --input PATH       [default: output/twitch_full_panel_enriched.csv]
  --output-dir PATH  [default: analysis_output/]
  --log-level {DEBUG,INFO,WARNING,ERROR}
```

### `drop_columns.py`

```
python drop_columns.py <input_csv> [output_csv]

# Removes: top_game, top_game_hours, top_game_pct, game_count,
#          is_variety_streamer, peak_view_rank, avg_viewer_rank,
#          follower_rank, follower_gain_rank
#
# Omit output_csv to overwrite input in place.
```

---

## Safety Posture

The pipeline runs from a single IP with no proxy rotation or browser automation. Concurrency and retry parameters are set conservatively. The circuit breaker ensures the system halts completely on sustained 403 responses rather than continuing to send requests through an error window.

| Control | Behaviour |
|---|---|
| Rate limiter | Token bucket · 7–14 rps · 429 → 30 s full backoff · 5xx → −20% rate |
| Circuit breaker | Opens on 3× 403 in 60 s or 10 consecutive failures · 5 min cooldown · HALF_OPEN probe at 1 rps |
| Per-request jitter | 100–400 ms uniform random delay on every request |
| Batch pause | 3–5 s pause every 300 records |
| No browser automation | Pure async HTTP via `aiohttp` only |
| No header spoofing | Standard Chrome User-Agent |
| Budget limits | Per-month cap: 250 API calls · Global cap: 15,000 API calls per run |

---

## Requirements

```
aiohttp>=3.9
pandas>=2.0
pyarrow>=14.0
python-dotenv
matplotlib        # analysis script only
tenacity          # retry logic
```

Python 3.11+ required. All async entry points use `asyncio.run()`. No third-party event loop is needed or assumed.
