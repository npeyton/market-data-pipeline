# Market Data Pipeline

An end-to-end data pipeline that ingests daily US stock market data, company reference data, and news events from my trading dashboard, lands them in AWS S3, loads them into Snowflake, and transforms them with dbt into analytics tables for backtesting and research. Dagster orchestrates every step on a schedule.


## Stack

| Layer | Tool |
|---|---|
| Orchestration | Dagster (assets, daily partitions, jobs, schedules) |
| Storage (landing zone) | AWS S3 |
| Warehouse | Snowflake (external stage, file formats, COPY INTO) |
| Transformation and testing | dbt |
| Language | Python (boto3, requests, snowflake-connector-python) |

## What it does (Jobs)
- Daily bars - Massive flat files (one gzipped CSV per trading day) Weekdays 12:00 PM `RAW.RAW_DAY_AGGS`
- Transform -  dbt models and tests Weekdays 12:30 PM `STAGING.STG_DAILY_BARS`, `ANALYTICS.DAILY_RETURNS`, `ANALYTICS.DIM_TICKERS`, `ANALYTICS.FCT_NEWS_WITH_SECTORS` |
- News exports - CSV exports from my trading dashboard Weekdays 4:05 PM `RAW.RAW_DASH_EXPORTS`
- Ticker details -  Massive Ticker Overview API Sundays 9:00 AM `RAW.RAW_TICKER_DETAILS`

## Project structure

```
definitions.py        Dagster assets, jobs, and schedules
market_dbt/           dbt project (sources, staging and analytics models, tests)
snowflake/setup.sql   Snowflake objects the pipeline depends on
scripts/              One-off helpers (key generation, connection test, single-day copy)
docs/                 Screenshots
```

## Running it locally

1. Create a virtual environment and install dependencies:
   ```
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   ```
2. Copy `.env.example` to `.env` and fill in your own credentials.
3. Create the Snowflake objects in `snowflake/setup.sql`.
4. Start Dagster from the project folder:
   ```
   dagster dev -f definitions.py
   ```
5. Open http://localhost:3000 to view assets and launch runs.