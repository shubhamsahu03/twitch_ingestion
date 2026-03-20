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

# 3. Full historical ingestion (2022–2025, all months) → both output files
python main.py --mode full

# 4. Resume after any interruption
python main.py --mode incremental

# 5. Test with a single month before full run
python main.py --mode full --years 2024 --months january
```

---

## Output Files

The pipeline runs three phases and produces two deliverables:

```
data/
├── twitch_monthly_fact_table.csv           ← Deliverable 1  (Phase 1)
├── twitch_monthly_fact_table_enriched.csv  ← Intermediate   (Phase 2)
└── twitch_monthly_fact_table_final.csv     ← Deliverable 2  (Phase 3)
```

| File | Contents |
|------|----------|
| `twitch_monthly_fact_table.csv` | Leaderboard metadata + all-games pie charts (÷4 corrected) |
| `twitch_monthly_fact_table_enriched.csv` | Above + full games table from games API |
| `twitch_monthly_fact_table_final.csv` | Above + cohort ranks + derived game features |

---

## Schema — Final Output (`twitch_monthly_fact_table_final.csv`)

### Identity
| Column | Description |
|--------|-------------|
| `year` | 2022–2025 |
| `month` | january – december |
| `channel_id` | SullyGnome internal channel ID |
| `channel_slug` | URL slug (e.g. `xqc`) |
| `display_name` | Display name |

### Performance Metrics
| Column | Description |
|--------|-------------|
| `rank_position` | Leaderboard rank 1–500 for that month |
| `followers` | Total followers at month end |
| `followers_gained` | Followers gained during the month |
| `average_viewers` | Average concurrent viewers |
| `peak_viewers` | Peak concurrent viewers |
| `hours_streamed` | Total hours streamed (`streamedminutes ÷ 60`) |
| `hours_watched` | Total hours watched by all viewers (`viewminutes ÷ 60`) |

### Channel Metadata
| Column | Description |
|--------|-------------|
| `status` | Channel status |
| `mature` | Mature content flag |
| `language` | Primary language |
| `created` | Twitch account creation date (requires Twitch API credentials) |

### Cohort Ranks *(computed per year+month group, highest value = Rank 1)*
| Column | Description |
|--------|-------------|
| `peak_viewer_rank` | Rank by peak viewers within the month's Top 500 |
| `avg_viewer_rank` | Rank by average viewers within the month's Top 500 |
| `follower_rank` | Rank by total followers within the month's Top 500 |
| `follower_gain_rank` | Rank by followers gained within the month's Top 500 |

### Game Breakdown — Pie Chart API *(all games, ÷4 corrected)*
| Column | Description |
|--------|-------------|
| `games_by_avg_viewers_json` | `[["Game", avg_viewers], ...]` — all games sorted by avg viewers desc |
| `games_by_stream_time_json` | `[["Game", hours], ...]` — all games sorted by hours streamed desc |

### Game Breakdown — Games Table API *(full detail per game)*
| Column | Description |
|--------|-------------|
| `all_games_json` | `[["Game", hours_streamed, avg_viewers, peak_viewers, hours_watched], ...]` |

### Derived Game Features *(feature-engineered in Phase 3)*
| Column | Description |
|--------|-------------|
| `top_game` | Name of the most-streamed game that month |
| `top_game_hours` | Hours spent on the top game |
| `top_game_pct` | % of total stream time on the top game (0–100) |
| `game_count` | Number of distinct games played |
| `is_variety_streamer` | `1` if `game_count ≥ 4` AND `top_game_pct < 60%`, else `0` |

> **Note:** `streams` (number of streams) is not available from any SullyGnome API endpoint and is excluded from the final output.

---

## Pipeline Architecture

```
main.py --mode full
    │
    ├── Phase 1: IngestionEngine
    │     Leaderboard API (5 calls/month)
    │     + Pie Chart API ×2 per channel (all games, ÷4 fixed)
    │     → twitch_monthly_fact_table.csv
    │
    ├── Phase 2: EnrichmentEngine
    │     Games Table API per channel (paginated, all games)
    │     → twitch_monthly_fact_table_enriched.csv
    │
    └── Phase 3: Transformer (inline, no extra file needed)
          Cohort ranks + derived game features
          → twitch_monthly_fact_table_final.csv
```

### Project Structure

```
your_project/
├── main.py                  # Pipeline orchestrator + inline transformer
├── requirements.txt
├── .env                     # Optional — Twitch API credentials
├── engine/
│   ├── __init__.py
│   └── ingestion.py         # All ingestion logic (Phase 1 + Phase 2)
├── data/                    # Output CSVs written here (auto-created)
├── logs/                    # engine.log written here (auto-created)
└── checkpoints/             # Resume state (auto-created)
    ├── processed.txt        # Phase 1 checkpoint
    └── enriched.txt         # Phase 2 checkpoint
```

### Engine Components

| Component | Behaviour |
|-----------|-----------|
| **RateLimiter** | Token bucket · 429 → 30s backoff · 5xx/timeout spike → ×0.8 rate · 20-request success streak → +1 rate |
| **CircuitBreaker** | 3× HTTP 403 in 60s OR 10 consecutive failures → OPEN 5 min → HALF_OPEN probe → CLOSED |
| **Checkpoint** | Append-only `.txt` file; key = `channelId_year_month`; resume-safe across crashes |
| **CsvWriter** | asyncio-locked file append; writes header once on first run |
| **HttpClient** | Shared aiohttp session; RETRY_LIMIT=3; exponential backoff; per-request jitter |
| **TwitchClient** | Optional Helix API client; in-memory cache; auto token refresh |

---

## Data Sources

### 1. Leaderboard API
```
GET /api/tables/channeltables/getchannels/{yearmonth}/0/1/3/desc/{start}/100
```
5 calls per month (start = 0, 100, 200, 300, 400) → 500 channels.  
Provides: `channel_id`, `slug`, `display_name`, `followers`, `followers_gained`, `average_viewers`, `peak_viewers`, `streamedminutes`, `viewminutes`, `status`, `mature`, `language`, `rank_position`.

### 2. Pie Chart APIs (×2 per channel)
```
GET /api/charts/piecharts/getconfig/channelgamestreamedtime/0/{id}/{slug}/%20/%20/{year}/{month}/%20/0/
GET /api/charts/piecharts/getconfig/channelgameavgviewers/0/{id}/{slug}/%20/%20/{year}/{month}/%20/0/
```
Returns all games played with stream time / avg viewers.  
**Bug fix applied:** API returns values 4× the displayed website figures — corrected with `÷ 4` in both extractors.

### 3. Games Table API (per channel, paginated)
```
GET /api/tables/channeltables/games/{yearmonth}/{channelId}/%20/1/2/desc/{start}/100
```
Returns full game breakdown with `streamtime`, `viewtime`, `avgviewers`, `maxviewers` per game.  
Paginates automatically if a channel played more than 100 games in the month.

### 4. Twitch Helix API (optional)
```
GET https://api.twitch.tv/helix/users?login={slug}
```
Provides channel `created_at` date. Requires `TWITCH_CLIENT_ID` and `TWITCH_CLIENT_SECRET` in `.env`. Results are cached in-memory to avoid duplicate requests across months.

---

## Performance

| Metric | Value |
|--------|-------|
| Total requests (full run) | ~96,000 (Phase 1: ~72k · Phase 2: ~24k) |
| Effective sustained rate | 10.8–11 rps |
| Concurrency | 12 simultaneous requests |
| Expected runtime | ~2.5–3 hours |
| Batch pause | 3–5s every 300 records |
| Per-request jitter | 100–250ms |

---

## CLI Reference

```
python main.py [OPTIONS]

Options:
  --mode {full,incremental}   full = fresh run (deletes existing output + checkpoints)
                              incremental = skip already processed records  [default: full]

  --data-dir PATH             Directory for output CSVs  [default: data/]
  --checkpoint-dir PATH       Directory for checkpoint files  [default: checkpoints/]
  --years INT [INT ...]       Years to process  [default: 2022 2023 2024 2025]
  --months MONTH [MONTH ...]  Months to process  [default: all 12]
  --skip-phase2               Stop after Phase 1 only — skip games API and transformer
  --log-level LEVEL           DEBUG / INFO / WARNING / ERROR  [default: INFO]
```

### Common Commands

```bash
# Full run — both deliverables
python main.py --mode full

# Resume after a crash
python main.py --mode incremental

# Phase 1 only (faster — skips games API)
python main.py --mode full --skip-phase2

# Specific years
python main.py --mode full --years 2024 2025

# Single month test
python main.py --mode full --years 2022 --months january

# Debug logging
python main.py --mode full --log-level DEBUG
```

---

## Safety Features

- **Single residential IP** — no proxy rotation, no IP switching
- **No Selenium or headless browsers** — pure HTTP API calls only
- **No header spoofing** — standard Chrome User-Agent
- **Adaptive rate limiter** — stays between 7–14 rps; backs off on errors, recovers on success
- **Circuit breaker** — halts on repeated 403s or consecutive failures; prevents IP bans
- **Per-request jitter** — 100–250ms random delay on every request
- **Batch sleep** — 3–5s pause every 300 records
- **Resume safe** — two independent checkpoints survive crash or CTRL-C; `--mode incremental` picks up exactly where it stopped
