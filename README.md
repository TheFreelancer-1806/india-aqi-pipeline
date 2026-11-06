# Automated India Metro AQI Pipeline

**API → PostgreSQL → dashboard, on a schedule, with nobody at the keyboard.**

[![Daily pipeline](https://github.com/USERNAME/REPO/actions/workflows/daily_etl.yml/badge.svg)](../../actions/workflows/daily_etl.yml)
[![CI](https://github.com/USERNAME/REPO/actions/workflows/ci.yml/badge.svg)](../../actions/workflows/ci.yml)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![PostgreSQL 16](https://img.shields.io/badge/postgresql-16-336791)

Every morning at 01:00 UTC a GitHub Actions runner pulls hourly air quality
readings for six Indian metros from the Open-Meteo Air Quality API, cleans them
in pandas, loads them through a three-tier PostgreSQL schema, and recomputes a
set of rolling-window analytics. A Streamlit dashboard reads the analytical
layer and refreshes itself. [`docs/FINDINGS.md`](docs/FINDINGS.md) is
regenerated from the warehouse on every run, so the write-up never drifts from
the data.

> Replace `USERNAME/REPO` in the badge URLs above once you push this.

---

## What is actually interesting here

Most of the logic lives in SQL, not in Python or in the dashboard.

| Concern | Where it lives | Why |
|---|---|---|
| 7-day rolling average | `AVG() OVER (RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW)` | Calendar-aware: a missing day shortens the window instead of silently reaching further back in time |
| Day-over-day / week-over-week change | `LAG(avg_pm2_5, 1)` and `LAG(avg_pm2_5, 7)` | `prev_obs_date` ships alongside, so a gap in the series is visible rather than passed off as "yesterday" |
| Anomaly flag | 30-day rolling Z-score, **current day excluded** | A spike must be measured against the days *before* it; including it lets a bad day inflate the baseline it is judged against |
| Threshold flags | `is_cpcb_breach` (>60), `is_who_breach` (>15) | Two standards, two questions — kept separate rather than collapsed into one "safe/unsafe" bit |
| Breach streaks | Gaps-and-islands: `obs_date - ROW_NUMBER() OVER (...)` | Longest unbroken run of breach days per city |
| Idempotent load | `ON CONFLICT (city, obs_date) DO UPDATE ... WHERE EXCLUDED.valid_hours_count >= tgt.valid_hours_count` | Re-running is free, and a partial day can never overwrite a complete one |

The dashboard computes nothing. Whatever an analyst gets from `psql` is exactly
what the dashboard shows.

---

## Architecture

```
┌─────────────────────────────┐
│ Open-Meteo Air Quality API  │   hourly PM2.5 / PM10 / NO2 / SO2 / CO / US AQI
│ no key, CC-BY 4.0           │   6 cities, timezone=Asia/Kolkata
└──────────────┬──────────────┘
               │  requests + urllib3 retry/backoff
               ▼
┌─────────────────────────────┐
│ GitHub Actions · 01:00 UTC  │   unit tests gate the run
│ (06:30 IST, daily)          │   pandas validation & cleaning
└──────────────┬──────────────┘
               │  SQLAlchemy 2.0 / psycopg2 — one transaction
               ▼
┌──────────────────────────────────────────────────────────────┐
│ PostgreSQL · schema aqi_pipeline                             │
│                                                              │
│  stg_air_quality      bronze · append-only hourly rows       │
│       │               14-day ingest retention                │
│       ▼                                                      │
│  clean_daily_aqi      silver · one row per city per day      │
│       │               CHECK constraints, completeness gate   │
│       ▼                                                      │
│  mart_aqi_analytics   gold · window functions, LAG deltas,   │
│                       CPCB/WHO flags, rolling Z-score        │
│                                                              │
│  pipeline_runs        audit · one row per invocation         │
└──────────────┬───────────────────────────────────────────────┘
               │  cached 30 min
               ▼
┌─────────────────────────────┐   ┌──────────────────────────┐
│ Streamlit dashboard         │   │ docs/FINDINGS.md         │
│ KPIs, trends, seasonality,  │   │ regenerated from the     │
│ breaches, data explorer     │   │ warehouse each run       │
└─────────────────────────────┘   └──────────────────────────┘
```

### Why three tiers

- **Staging** keeps the API response as received. When a transform turns out to
  be wrong, the fix is a re-run, not a re-fetch.
- **Clean** enforces the daily grain and the constraints. A row that reaches it
  is known to be non-negative, internally consistent (`min ≤ avg ≤ max`) and
  built from at least six hourly observations.
- **Mart** is a view, so it costs nothing to change and can never be stale with
  respect to the clean layer.

---

## Findings

Generated from the warehouse, not written by hand — see
[`docs/FINDINGS.md`](docs/FINDINGS.md) for the current numbers, the coverage
window they rest on, and the queries behind them. The headline sections:

1. **The breach rate, not the mean, separates the airsheds.** Delhi sits in CPCB
   breach on a majority of days; Bengaluru, Chennai and Hyderabad on none —
   while *all six* breach the WHO guideline most days.
2. **Seasonality is a step change, not a drift.** Post-monsoon means jump
   sharply above monsoon means in the Indo-Gangetic cities.
3. **Pollution is highly autocorrelated day to day.** Lag-1 autocorrelation is
   high everywhere, which is the practical argument for showing a rolling
   average beside the daily value.
4. **A static threshold cannot alert; a rolling Z-score can.** Once a city
   breaches nearly every day, the threshold stops carrying information. The
   Z-score keeps isolating acute episodes against the city's own baseline.
5. **Weekends are measurably cleaner than weekdays**, in PM2.5 and in NO2 —
   a weekly cycle is a fingerprint of human activity, since meteorology has no
   reason to know what day it is.

Regenerate with `python -m src.report`.

---

## Quickstart

### Option A — against a free serverless Postgres (what the schedule uses)

1. Create a free project on [Neon](https://neon.tech) or
   [Supabase](https://supabase.com) and copy the connection URI.
2. ```bash
   git clone <your-repo-url> && cd <repo>
   python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   cp .env.example .env        # paste your DATABASE_URL into it
   ```
3. ```bash
   python -m src.etl           # first run backfills 90 days automatically
   python -m src.report        # regenerate docs/FINDINGS.md
   streamlit run app.py
   ```

### Option B — against a throwaway local Postgres (Windows, no admin rights)

`scripts/local_pg.ps1` downloads the official PostgreSQL *binaries* zip (no
installer, no service, no admin), initialises a cluster under `.local\` and runs
it on port 55432. Everything it creates is gitignored.

```powershell
.\scripts\local_pg.ps1 start
```

```powershell
$env:DATABASE_URL      = 'postgresql://postgres@127.0.0.1:55432/aqi_dev'
$env:TEST_DATABASE_URL = 'postgresql://postgres@127.0.0.1:55432/aqi_test'
```

```powershell
python -m src.etl; python -m src.report; streamlit run app.py
```

Other subcommands: `stop`, `status`, `psql`, `url`, `destroy`.

### No database at all

```bash
python -m src.etl --dry-run
```

Exercises extraction and cleaning against the live API and prints the daily
aggregates it would have loaded.

---

## Scheduling it

1. Push to GitHub.
2. **Settings → Secrets and variables → Actions → New repository secret**:
   name `DATABASE_URL`, value your Postgres URI.
3. **Actions → Daily AQI pipeline → Run workflow** to confirm the first green
   check. After that it runs itself at 01:00 UTC daily.

The workflow runs the offline unit tests before touching the warehouse, so a
cleaning regression fails the job rather than loading bad rows. A
`concurrency` group stops a manual run and the cron run from writing at the
same time.

> GitHub disables scheduled workflows on repositories with no activity for 60
> days. A single commit re-enables them.

### Deploying the dashboard

[share.streamlit.io](https://share.streamlit.io) → **New app** → pick the repo,
branch `main`, main file `app.py`. Under **Advanced settings → Secrets**:

```toml
DATABASE_URL = "postgresql://user:pass@host/db?sslmode=require"
```

---

## Tests

```bash
pytest                  # unit tests + SQL integration tests
```

```bash
pytest -m network -v    # live Open-Meteo contract tests (deselected by default)
```

| Suite | What it covers | Needs |
|---|---|---|
| `tests/test_etl_clean.py` | Payload shaping, cleaning rules, settings validation, URL normalization | nothing |
| `tests/test_sql_mart.py` | Every SQL behaviour: daily aggregation, dedup, completeness gate, idempotency, the partial-day guard, rolling windows across gaps, LAG deltas, CPCB bands, Z-score baseline exclusion, streaks, constraints | `TEST_DATABASE_URL` |
| `tests/test_api_live.py` | The upstream contract — field names, shape, plausible units | network |

`tests/test_sql_mart.py` drops and recreates the `aqi_pipeline` schema, so its
fixture refuses any database whose name does not contain `test`. CI provides a
`postgres:16` service container; locally, `scripts/local_pg.ps1` provides one.

The rolling-average test compares the SQL output against an independent pandas
reference rather than against hardcoded numbers, and a separate test proves the
`RANGE`-based window behaves differently from a `ROWS`-based one across a
five-day gap.

---

## Layout

```
.
├── .github/workflows/
│   ├── daily_etl.yml       scheduled pipeline (01:00 UTC)
│   └── ci.yml              tests on real Postgres + live API contract
├── sql/
│   ├── 01_ddl_schema.sql   schema, tables, constraints, mart views
│   ├── 02_transform_load.sql  staging → clean upsert + retention prune
│   └── 03_analytical_queries.sql  the nine queries behind FINDINGS.md
├── src/
│   ├── config.py           cities, API contract, thresholds, settings
│   ├── db.py               engine, SQL file execution, run auditing
│   ├── etl.py              extract → clean → stage → transform
│   └── report.py           runs the analytical queries, renders FINDINGS.md
├── tests/
├── scripts/local_pg.ps1    throwaway local Postgres for development
├── docs/FINDINGS.md        generated
└── app.py                  Streamlit dashboard
```

### Configuration

All optional — see [`.env.example`](.env.example).

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | *required* | Postgres connection string. `postgres://` is normalised automatically |
| `BACKFILL_DAYS` | `90` | History pulled on the first run (capped at the API's 92-day limit) |
| `INCREMENTAL_DAYS` | `4` | Window pulled on every later run; the overlap re-states recent days |
| `MIN_HOURS_PER_DAY` | `6` | Hourly observations required before a day reaches the clean layer |
| `STAGING_RETENTION_DAYS` | `14` | How long raw hourly rows stay replayable |
| `TEST_DATABASE_URL` | unset | Throwaway database for the SQL tests; they skip without it |

---

## Data caveats

Worth stating plainly, because they bound what the findings can claim:

- **Open-Meteo air quality is CAMS model output, not CPCB ground-station
  measurement.** It is a reanalysis/forecast product on a model grid. It tracks
  the shape of real pollution episodes well, but absolute values are smoother
  than a CPCB station's, and peaks are lower. Treat city-to-city comparisons and
  trends as sound, and any single absolute number as approximate.
- **One grid point per city.** Air quality varies sharply within an Indian
  metro; this is the city-centre coordinate, not a city-wide average.
- **A 90-day backfill cannot show a full seasonal cycle.** The API serves at
  most 92 past days, so seasonal claims strengthen as the schedule accumulates
  history. `docs/FINDINGS.md` prints its coverage window at the top for exactly
  this reason.
- **`avg_pm2_5` is a plain mean of valid hours**, not the CPCB 24-hour sub-index,
  so `cpcb_category` is a PM2.5-band label rather than an official National AQI
  value (which takes the worst of several pollutant sub-indices).
- **Today's row is partial.** It is built from the hours elapsed so far;
  `valid_hours_count` and `data_completeness_pct` say how many.

---

## Licence and attribution

Air quality data © [Open-Meteo.com](https://open-meteo.com), licensed
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), derived from the
Copernicus Atmosphere Monitoring Service (CAMS).
