# NYC Taxi + Weather Analytics Pipeline

Medallion lakehouse pipeline on Databricks Free Edition, joining NYC TLC Yellow Taxi
trips (Jan–May 2026, ~19M rows) with NYC hourly weather (Open-Meteo) to analyze
weather's impact on ridership and operations, enriched with borough-level demand,
payment behavior, and a basic fraud/anomaly screen. Built for the Schwarz Digits
data engineer take-home.

## Architecture

Local ingestion scripts → S3 (`taxi-weather-raw/bronze/`) → Databricks Unity Catalog
external location (IAM role, self-assuming trust policy) → bronze Delta tables
(PySpark) → DQ validation gate with quarantine (PySpark) → dbt staging/intermediate
(weather + borough joins) → dbt marts (gold) → analysis.

## Stack & layer-split rationale

- **PySpark** owns bronze ingestion and the DQ/quarantine gate — row-level rejection
  routing with a reason code is awkward to express declaratively in dbt.
- **dbt Core (dbt-databricks)** owns staging conformance, the weather + zone joins,
  gold marts, and all declarative tests — this is where lineage and test coverage live.
- **S3 + Unity Catalog external location** for ingestion instead of manual file
  upload — mirrors a real production access pattern, not just a take-home shortcut.
- **NYC TLC taxi zone lookup** (static reference table) joined in to convert opaque
  location IDs into boroughs — kept as a lightweight one-off script rather than the
  full month-driven ingestion machinery, since it's a static dimension, not a fact
  table that changes per batch. A senior judgment call: not every input deserves
  the same engineering weight.

## How to reproduce

```bash
pip install -r requirements.txt
export AWS_ACCESS_KEY_ID=...        # or set in .env
export AWS_SECRET_ACCESS_KEY=...
python src/ingest_taxi.py            # downloads 5 months, uploads to S3
python src/ingest_weather.py         # fetches Jan-May hourly weather, uploads to S3
python src/ingest_reference.py       # taxi zone lookup
python -m pytest tests/ -v           # 11 tests, no network/cloud calls
```

In Databricks: run `databricks/notebooks/00_bronze_load.py` then `01_dq_gate.py`.

```bash
cd dbt
dbt run --select staging intermediate marts
dbt test
```

```bash
python src/analysis.py               # 9 charts to outputs/
```

## Data quality results

| Check | Count | % of total |
|---|---|---|
| Total taxi rows (bronze) | 18,999,282 | 100% |
| Duplicates removed | 0 | 0% |
| Valid rows → silver | 18,196,074 | 95.77% |
| Quarantined | 803,208 | 4.23% |

Quarantine breakdown: `non_positive_distance` 575,255 · `pickup_outside_range` 120,505
· `negative_total_amount` 107,443 · `dropoff_before_pickup` 5.

**Quarantine rate by month** (`dq_quarantine_trend.png`): 4.37% (Jan), 4.36% (Feb),
3.55% (Mar), 2.79% (Apr), **5.98% (May)**. May's spike traces almost entirely to one
cause: **120,490 of the 120,505 total `pickup_outside_range` rejections across all
5 months — 99.99% — came from May's file alone.** This was investigated rather than
just reported: an initial hypothesis (trips spilling a few hours across month
boundaries, evenly distributed) didn't hold up once broken out by month; the real
cause is specific to May's source file and would be the first thing to check against
TLC's raw file if this were a live production incident.

Weather join match rate: enforced ≥95% via a custom dbt test (`weather_match_rate.sql`).

## Findings

1. **Ridership is structurally driven by hour-of-day and weekday**
   (`hour_weekday_heatmap.png`) — commute peaks dominate the signal.
2. **Precipitation reduces average hourly ridership by ~4.1%**
   (5,303 → 5,083 trips/hour) — a real but secondary effect relative to time-of-day
   structure (`dry_vs_precip_ridership.png`). Cold temperatures show a similar
   modest suppression (`ridership_by_temp_bucket.png`).
3. **Weather also slows trips down, a separate effect from demand**
   (`weather_speed_impact.png`): average speed drops in precipitation and cold
   temperatures across both weekday and weekend. This is an operational signal
   (trips that happen take longer) distinct from finding #2 (fewer trips happen at
   all) — same weather variable, two different business questions, both relevant
   to fleet scheduling.
4. **Demand and revenue are heavily Manhattan-concentrated**: 86.2% of trips and
   88.4% of revenue (`demand_by_borough.png`). Queens is a distant second (8.8%),
   likely airport-driven (JFK/LGA). For fleet placement, the marginal value of
   repositioning is in Manhattan's hourly demand curve, not cross-borough
   rebalancing — the outer boroughs are a rounding error by volume.
5. **Revenue efficiency isn't the same signal as demand volume**
   (`revenue_efficiency_by_hour.png`): the hours with the most trips aren't
   necessarily the hours with the best revenue-per-minute. Fleet placement
   decisions built purely on trip counts (finding #4) would miss this.
6. **24% of trips carry an undocumented payment code (0)**, not one of TLC's
   published 1–6 values (`payment_mix.png`). Found by questioning a suspiciously
   large "Other" bucket rather than accepting it at face value. These trips have
   the *highest* average fare ($32.83) and a non-zero tip rate (1.9%) — unlike
   Cash/Dispute/No-charge, which are all exactly $0.00 tip — suggesting genuine
   completed transactions via a payment method TLC's public code list hasn't
   caught up to, not errors or voided trips. Also confirms the known TLC quirk
   that cash tips are never electronically captured (0.0% avg tip on Cash is a
   data-capture artifact, not a real behavioral claim about cash riders).
7. **Fraud/anomaly screen**: 172,418 trips (0.95%) show fare > $100/mile
   (`silver.trips_input` query). This is an anomaly *screen*, not a fraud
   confirmation — likely a mix of genuine overcharge/meter anomalies and mundane
   GPS/distance-logging errors. A production fraud system would need
   investigation and labeling before treating this as more than a candidate list.
8. **Pipeline observability**: the quarantine-rate-by-month trend caught a real,
   traceable data issue (finding above) that a single aggregate DQ percentage
   would have hidden entirely — this is why DQ metrics belong in a time series,
   not just a point-in-time report.

Caveat on all findings: single 5-month window (Jan–May 2026), no causal claims,
holidays/seasonality not isolated.

## Production next steps

- Orchestration: Airflow DAG (ingest → DQ gate → dbt run → dbt test → publish),
  Cloud Composer on GCP per the target architecture.
- CI: GitHub Actions running `pytest` + `dbt build` on every PR.
- Secrets: `.env` / `~/.dbt/profiles.yml` are gitignored; a CI/production setup
  would use a secret manager instead of local env files.
- Monitoring: dbt test failures + Databricks job alerts wired to Slack/PagerDuty,
  with the monthly quarantine-rate trend as an actual alerting metric, not just
  a one-time chart.
- Data quality: raise the `payment_type = 0` finding with the data source owner —
  a real production DQ gate would flag undocumented enum values explicitly rather
  than let them pass silently into a "valid" table.
- At production scale: silver/gold move to incremental `merge` strategies keyed
  on trip ID rather than full overwrite; the May anomaly would be the first
  candidate for a source-file-level DQ check (row date-range validation before
  it ever reaches bronze, not after).