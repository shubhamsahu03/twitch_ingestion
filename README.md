# Twitch Analytics Ingestion Engine
**Fully Deterministic · API-First · Adaptive · Residential-Safe**

Extracts 48 months × 500 channels = **24,000 channel-month records** from SullyGnome  
and produces two clean, analytics-ready CSV files.

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. (Optional) Add Twitch credentials for the 'created' column
echo "TWITCH_CLIENT_ID=your_id" > .env
echo "TWITCH_CLIENT_SECRET=your_secret" >> .env

# 3. Full historical ingestion (2022–2025, all months) — both output files
python main.py --mode full

# 4. Resume after any interruption
python main.py --mode incremental

# 5. Test with a single month before full run
python main.py --mode full --years 2024 --months january
```

---

## Output Files

The pipeline runs three phases sequentially and produces two deliverables:

```
data/
├── twitch_monthly_fact_table.csv           ← Deliverable 1  (Phase 1)
├── twitch_monthly_fact_table_enriched.csv  ← Intermediate   (Phase 2)
└── twitch_monthly_fact_table_final.csv     ← Deliverable 2  (Phase 3)
```

| File | Rows | Contents |
|------|------|----------|
| `twitch_monthly_fact_table.csv` | 24,000 | Leaderboard metadata + top-5 pie charts (÷4 corrected) |
| `twitch_monthly_fact_table_enriched.csv` | 24,000 | Above + full games breakdown from games table API |
| `twitch_monthly_fact_table_final.csv` | 24,000 | Above + cohort ranks, `streams` column dropped |

---

## Schema

### Phase 1 — `twitch_monthly_fact_table.csv` (23 columns)

#### Identity
| Column | Description |
|--------|-------------|
| `year` | 2022 – 2025 |
| `month` | january – december |
| `channel_id` | SullyGnome internal channel ID |
| `channel_slug` | URL slug (e.g. `xqc`) |
| `display_name` | Channel display name |

#### Performance Metrics
| Column | Description |
|--------|-------------|
| `rank_position` | Leaderboard rank 1–500 for that month |
| `followers` | Total followers at month end |
| `followers_gained` | Followers gained during the month |
| `average_viewers` | Average concurrent viewers |
| `peak_viewers` | Peak concurrent viewers |
| `hours_streamed` | Total hours streamed (`streamedminutes ÷ 60`) |
| `hours_watched` | Total viewer-hours (`viewminutes ÷ 60`) |
| `streams` | Always empty — not available from any SullyGnome API |

#### Channel Metadata
| Column | Description |
|--------|-------------|
| `status` | Channel status |
| `mature` | Mature content flag |
| `language` | Primary broadcast language |
| `created` | Twitch account creation date (requires `.env` credentials, otherwise empty) |

#### Cohort Ranks *(empty in Phase 1 — populated by Phase 3 transformer)*
| Column | Description |
|--------|-------------|
| `peak_viewer_rank` | Rank by peak viewers within the month's Top 500 |
| `avg_viewer_rank` | Rank by average viewers within the month's Top 500 |
| `follower_rank` | Rank by total followers within the month's Top 500 |
| `follower_gain_rank` | Rank by followers gained within the month's Top 500 |

#### Game Breakdown — Pie Chart API *(top-5 + "Other", ÷4 corrected)*
| Column | Format | Description |
|--------|--------|-------------|
| `top5_games_by_avg_viewers_json` | `[["Game", avg_viewers], ..., ["Other", avg_viewers]]` | Top-5 games by average viewers; remainder aggregated as "Other" |
| `top5_games_by_stream_time_json` | `[["Game", hours], ..., ["Other", hours]]` | Top-5 games by hours streamed; remainder aggregated as "Other" |

> **÷4 correction applied to both pie columns.** SullyGnome's pie chart API returns values 4× the figures displayed on the website. Both columns are divided by 4 at ingestion time to match the site.

---

### Phase 2 — `twitch_monthly_fact_table_enriched.csv` (24 columns)

All Phase 1 columns plus:

| Column | Format | Description |
|--------|--------|-------------|
| `all_games_json` | `[["Game", hours_streamed, avg_viewers, peak_viewers, hours_watched], ...]` | Full game breakdown from the games table API — all games, sorted by hours streamed descending |

---

### Phase 3 — `twitch_monthly_fact_table_final.csv` (23 columns)

All Phase 2 columns except `streams` (dropped), with cohort ranks populated:

- `peak_viewer_rank`, `avg_viewer_rank`, `follower_rank`, `follower_gain_rank` — computed per `(year, month)` group; highest value = Rank 1; ties share the top rank; channels with missing data ranked at the bottom.

---

## Pipeline Architecture

```
python main.py --mode full
       │
       ├── Phase 1 — IngestionEngine
       │     Leaderboard API  (5 calls/month × 48 months)
       │     Pie Chart API ×2 (per channel — top-5, ÷4 corrected)
       │     → twitch_monthly_fact_table.csv
       │
       ├── Phase 2 — EnrichmentEngine
       │     Games Table API  (per channel, paginated)
       │     → twitch_monthly_fact_table_enriched.csv
       │
       └── Phase 3 — Transformer  (inline, no extra script needed)
             Cohort ranks · drops 'streams'
             → twitch_monthly_fact_table_final.csv
```

### Project Structure

```
your_project/
├── main.py                  # Pipeline orchestrator + inline transformer (Phase 3)
├── requirements.txt
├── .env                     # Optional — TWITCH_CLIENT_ID + TWITCH_CLIENT_SECRET
├── engine/
│   ├── __init__.py
│   └── ingestion.py         # IngestionEngine (Phase 1) + EnrichmentEngine (Phase 2)
├── data/                    # Output CSVs written here (auto-created)
├── logs/                    # engine.log written here (auto-created)
└── checkpoints/             # Resume state (auto-created)
    ├── processed.txt        # Phase 1 checkpoint — 24,000 entries when complete
    └── enriched.txt         # Phase 2 checkpoint — 24,000 entries when complete
```

### Engine Components

| Component | Behaviour |
|-----------|-----------|
| **RateLimiter** | Token bucket · 429 → 30 s backoff · 5xx / timeout spike → ×0.8 rate · 20-request success streak → +1 rate · capped at 7–14 rps |
| **CircuitBreaker** | 3× HTTP 403 in 60 s OR 10 consecutive failures → OPEN 5 min → HALF_OPEN probe (10 requests at 1 rps) → CLOSED |
| **Checkpoint** | Append-only `.txt` file; key = `channelId_year_month`; survives crash or CTRL-C |
| **CsvWriter** | asyncio-locked file append; writes CSV header once on first run |
| **HttpClient** | Shared `aiohttp` session; `RETRY_LIMIT=3`; exponential backoff; per-request jitter |
| **TwitchClient** | Optional Helix API client; in-memory cache across months; auto token refresh on 401 |

---

## Data Sources

### 1. Leaderboard API
```
GET /api/tables/channeltables/getchannels/{yearmonth}/0/1/3/desc/{start}/100
```
5 calls per month (start = 0, 100, 200, 300, 400) — fetched concurrently → 500 channels.  
Provides: `channel_id`, `slug`, `display_name`, `followers`, `followers_gained`,
`average_viewers`, `peak_viewers`, `streamedminutes`, `viewminutes`, `status`, `mature`, `language`, `rank_position`.

### 2. Pie Chart APIs (×2 per channel per month)
```
GET /api/charts/piecharts/getconfig/channelgamestreamedtime/0/{id}/{slug}/%20/%20/{year}/{month}/%20/0/
GET /api/charts/piecharts/getconfig/channelgameavgviewers/0/{id}/{slug}/%20/%20/{year}/{month}/%20/0/
```
Returns top-5 games with stream time or average viewers respectively.  
**÷4 bug fix:** Both pie APIs return values 4× the displayed website figures — corrected in `_extract_pie_stream_time()` and `_extract_pie_avg_viewers()`.

### 3. Games Table API (per channel per month, paginated)
```
GET /api/tables/channeltables/games/{yearmonth}/{channelId}/%20/1/2/desc/{start}/100
```
Returns full per-game breakdown: `streamtime`, `viewtime`, `avgviewers`, `maxviewers`.  
Paginates automatically (`start += 100`) until `recordsTotal` is reached.  
Note: `avgviewers` from this endpoint is **not** affected by the ÷4 bug.

### 4. Twitch Helix API (optional, for `created` column)
```
GET https://api.twitch.tv/helix/users?login={slug}
```
Requires `TWITCH_CLIENT_ID` and `TWITCH_CLIENT_SECRET` in `.env`.  
Results are cached in-memory to avoid duplicate requests across months for the same channel.

---

## Performance

| Metric | Value |
|--------|-------|
| Total records | 24,000 (48 months × 500 channels) |
| Total requests — Phase 1 | ~72,000 (5 leaderboard + 2 pie × 24,000 channels) |
| Total requests — Phase 2 | ~24,000 (1 games API call per channel) |
| Total requests — full run | ~96,000 |
| Effective sustained rate | 10.8 – 11 rps |
| Concurrency | 12 simultaneous requests |
| Per-request jitter | 100 – 250 ms |
| Batch pause | 3 – 5 s every 300 records |
| Expected runtime — full run | ~2.5 – 3 hours |

---

## CLI Reference

```
python main.py [OPTIONS]

Options:
  --mode {full,incremental}
        full        = fresh run; deletes existing CSVs and checkpoints first
        incremental = skip already checkpointed records, resume where stopped
        [default: full]

  --data-dir PATH
        Directory for all output CSV files
        [default: data/]

  --checkpoint-dir PATH
        Directory for checkpoint files (processed.txt, enriched.txt)
        [default: checkpoints/]

  --years INT [INT ...]
        Years to process
        [default: 2022 2023 2024 2025]

  --months MONTH [MONTH ...]
        Months to process (january … december)
        [default: all 12]

  --skip-phase2
        Stop after Phase 1 only — skips games API enrichment and transformer.
        Produces twitch_monthly_fact_table.csv only.

  --log-level {DEBUG,INFO,WARNING,ERROR}
        [default: INFO]
```

### Common Commands

```bash
# Full run — both deliverables
python main.py --mode full

# Resume after a crash or interruption
python main.py --mode incremental

# Phase 1 only (no games API — roughly half the runtime)
python main.py --mode full --skip-phase2

# Specific years only
python main.py --mode full --years 2024 2025

# Single month test run
python main.py --mode full --years 2022 --months january

# Verbose debug output
python main.py --mode full --log-level DEBUG
```

---

## Safety Features

- **Single residential IP** — no proxy rotation, no IP switching
- **No Selenium or headless browsers** — pure async HTTP only
- **No header spoofing** — standard Chrome User-Agent
- **Adaptive rate limiter** — stays within 7–14 rps; backs off on errors, recovers on sustained success
- **Circuit breaker** — stops on repeated 403s or consecutive failures to protect the IP
- **Per-request jitter** — 100–250 ms random delay on every request
- **Batch sleep** — 3–5 s pause every 300 records
- **Two independent checkpoints** — Phase 1 and Phase 2 each have their own checkpoint file; `--mode incremental` can resume either phase independently
