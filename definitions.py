import os
from datetime import datetime, timedelta
from pathlib import Path
import csv,io,requests
import boto3
import dagster as dg
import snowflake.connector
from botocore.exceptions import ClientError
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from dotenv import load_dotenv

load_dotenv()


# PARTITIONS: for scheduling tasks on market days. 
trading_days = dg.TimeWindowPartitionsDefinition(
    start="2024-01-01",
    cron_schedule="0 0 * * 1-5",
    fmt="%Y-%m-%d",
)


# HELPERS: Connect to Snowflake 
def snowflake_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
        role="PIPELINE_ROLE",
        warehouse="NEWS_WH",
    )


# ASSETS: Massive API connection. 
@dg.asset(partitions_def=trading_days)
def raw_day_aggs_file(context: dg.AssetExecutionContext):
    date = context.partition_key
    year, month, _ = date.split("-")

    massive = boto3.client(
        "s3",
        endpoint_url="https://files.massive.com",
        aws_access_key_id=os.environ["MASSIVE_S3_KEY_ID"],
        aws_secret_access_key=os.environ["MASSIVE_S3_SECRET"],
    )
    source_path = f"us_stocks_sip/day_aggs_v1/{year}/{month}/{date}.csv.gz"

    try:
        file_data = massive.get_object(Bucket="flatfiles", Key=source_path)["Body"].read()
    except ClientError as error:
        if error.response["Error"]["Code"] == "NoSuchKey":
            context.log.warning(f"No file for {date}. Likely a market holiday.")
            return dg.MaterializeResult(metadata={"status": "no file (market holiday?)"})
        raise

    bucket = os.environ["S3_BUCKET"]
    dest_path = f"raw/massive/day_aggs/date={date}/{date}.csv.gz"
    boto3.client("s3").put_object(Bucket=bucket, Key=dest_path, Body=file_data)
    context.log.info(f"Saved to s3://{bucket}/{dest_path}")

    return dg.MaterializeResult(
        metadata={
            "s3_path": f"s3://{bucket}/{dest_path}",
            "size_bytes": len(file_data),
        }
    )


@dg.asset(partitions_def=trading_days, deps=[raw_day_aggs_file])
def raw_day_aggs_table(context: dg.AssetExecutionContext):
    date = context.partition_key

    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            COPY INTO NEWS_PIPELINE.RAW.RAW_DAY_AGGS
            FROM @NEWS_PIPELINE.RAW.MASSIVE_STAGE/day_aggs/date={date}/
            FILE_FORMAT = (FORMAT_NAME = 'NEWS_PIPELINE.RAW.CSV_GZ_FORMAT')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            INCLUDE_METADATA = (file_name = METADATA$FILENAME,
                                loaded_at = METADATA$START_SCAN_TIME)
        """)
        cursor.execute(
            "SELECT COUNT(*) FROM NEWS_PIPELINE.RAW.RAW_DAY_AGGS WHERE file_name LIKE %s",
            (f"%date={date}/%",),
        )
        row_count = cursor.fetchone()[0]
    finally:
        conn.close()

    if row_count == 0:
        context.log.warning(f"No rows in RAW for {date}. Holiday, or the copy step had no file.")

    return dg.MaterializeResult(metadata={"rows_in_raw_for_day": row_count})

#ASSET: Ticker Details

@dg.asset(deps=[dg.AssetKey("stg_daily_bars")])
def raw_ticker_details_file(context: dg.AssetExecutionContext):
    # Tickers that traded in the last 30 days
    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT ticker FROM NEWS_PIPELINE.STAGING.STG_DAILY_BARS
            WHERE trade_date >= DATEADD(day, -30, CURRENT_DATE)
        """)
        tickers = [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()

    session = requests.Session()
    rows = []
    for i, ticker in enumerate(tickers, 1):
        resp = session.get(
            f"https://api.massive.com/v3/reference/tickers/{ticker}",
            params={"apiKey": os.environ["MASSIVE_API_KEY"]},
            timeout=30,
        )
        if resp.status_code == 200:
            r = resp.json().get("results", {})
            rows.append({
                "ticker": ticker,
                "name": r.get("name"),
                "type": r.get("type"),
                "sic_code": r.get("sic_code"),
                "sic_description": r.get("sic_description"),
            })
        if i % 500 == 0:
            context.log.info(f"{i} of {len(tickers)} tickers done")

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["ticker", "name", "type", "sic_code", "sic_description"])
    writer.writeheader()
    writer.writerows(rows)

    today = datetime.now().strftime("%Y-%m-%d")
    bucket = os.environ["S3_BUCKET"]
    dest_path = f"raw/massive/ticker_details/date={today}/ticker_details.csv"
    boto3.client("s3").put_object(Bucket=bucket, Key=dest_path, Body=buffer.getvalue())

    return dg.MaterializeResult(metadata={"tickers_requested": len(tickers), "tickers_saved": len(rows)})


@dg.asset(deps=[raw_ticker_details_file])
def raw_ticker_details_table(context: dg.AssetExecutionContext):
    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            COPY INTO NEWS_PIPELINE.RAW.RAW_TICKER_DETAILS
            FROM @NEWS_PIPELINE.RAW.MASSIVE_STAGE/ticker_details/
            FILE_FORMAT = (FORMAT_NAME = 'NEWS_PIPELINE.RAW.CSV_FORMAT')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            INCLUDE_METADATA = (file_name = METADATA$FILENAME,
                                loaded_at = METADATA$START_SCAN_TIME)
        """)
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM NEWS_PIPELINE.RAW.RAW_TICKER_DETAILS")
        ticker_count = cursor.fetchone()[0]
    finally:
        conn.close()

    return dg.MaterializeResult(metadata={"tickers_in_table": ticker_count})



# ASSETS: TRADING DASH EXPORTS
@dg.asset
def dash_exports_to_s3(context: dg.AssetExecutionContext):
    folder = Path(os.environ["DASH_EXPORTS_FOLDER"])

    # Only files changed in the last 24 hours
    cutoff = datetime.now() - timedelta(hours=24)
    csv_files = sorted(
        f for f in folder.glob("*.csv")
        if datetime.fromtimestamp(f.stat().st_mtime) > cutoff
    )

    if not csv_files:
        context.log.warning(f"No CSV files found in {folder}")
        return dg.MaterializeResult(metadata={"files_uploaded": 0})

    bucket = os.environ["S3_BUCKET"]
    today = datetime.now().strftime("%Y-%m-%d")
    s3_folder = f"raw/trading_dash/exports/date={today}"
    s3 = boto3.client("s3")

    for csv_file in csv_files:
        dest_path = f"{s3_folder}/{csv_file.name}"
        s3.upload_file(str(csv_file), bucket, dest_path)
        context.log.info(f"Uploaded {csv_file.name} to s3://{bucket}/{dest_path}")

    return dg.MaterializeResult(
        metadata={
            "files_uploaded": len(csv_files),
            "file_names": ", ".join(f.name for f in csv_files),
            "s3_folder": f"s3://{bucket}/{s3_folder}/",
        }
    )


@dg.asset(deps=[dash_exports_to_s3])
def raw_dash_exports_table(context: dg.AssetExecutionContext):
    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            COPY INTO NEWS_PIPELINE.RAW.RAW_DASH_EXPORTS
            FROM @NEWS_PIPELINE.RAW.DASH_EXPORTS_STAGE
            FILE_FORMAT = (FORMAT_NAME = 'NEWS_PIPELINE.RAW.CSV_FORMAT')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            INCLUDE_METADATA = (file_name = METADATA$FILENAME,
                                loaded_at = METADATA$START_SCAN_TIME)
        """)

        # One result row per file; keep the LOADED ones
        results = cursor.fetchall()
        loaded_files = [r for r in results if len(r) > 3 and r[1] == "LOADED"]
        rows_loaded = sum(r[3] for r in loaded_files)

        cursor.execute("SELECT COUNT(*) FROM NEWS_PIPELINE.RAW.RAW_DASH_EXPORTS")
        total_rows = cursor.fetchone()[0]
    finally:
        conn.close()

    if not loaded_files:
        context.log.warning("No new files to load. Already loaded, or nothing was uploaded.")

    return dg.MaterializeResult(
        metadata={
            "files_loaded": len(loaded_files),
            "rows_loaded": rows_loaded,
            "total_rows_in_table": total_rows,
        }
    )


# DBT:
dbt_project = DbtProject(
    project_dir=Path(__file__).parent / "market_dbt",
    profiles_dir=Path.home() / ".dbt",
)
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def market_dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# JOBS:
ingest_job = dg.define_asset_job(
    name="daily_ingest",
    selection=[raw_day_aggs_file, raw_day_aggs_table],
    partitions_def=trading_days,
)

transform_job = dg.define_asset_job(
    name="daily_transform",
    selection=[market_dbt_models],
)

exports_job = dg.define_asset_job(
    name="upload_dash_exports",
    selection=[dash_exports_to_s3, raw_dash_exports_table],
)
ticker_details_job = dg.define_asset_job(
    name="weekly_ticker_details",
    selection=[raw_ticker_details_file, raw_ticker_details_table],
)


# SCHEDULES:
@dg.schedule(
    job=ingest_job,
    cron_schedule="0 12 * * 1-5",
    execution_timezone="America/New_York",
)
def daily_ingest_schedule(context: dg.ScheduleEvaluationContext):
    # Previous trading day (Monday goes back to Friday)
    today = context.scheduled_execution_time.date()
    days_back = 3 if today.weekday() == 0 else 1
    previous_day = (today - timedelta(days=days_back)).strftime("%Y-%m-%d")
    return dg.RunRequest(run_key=previous_day, partition_key=previous_day)


daily_transform_schedule = dg.ScheduleDefinition(
    name="daily_transform_schedule",
    job=transform_job,
    cron_schedule="30 12 * * 1-5",
    execution_timezone="America/New_York",
)

dash_exports_schedule = dg.ScheduleDefinition(
    name="dash_exports_schedule",
    job=exports_job,
    cron_schedule="5 16 * * 1-5",
    execution_timezone="America/New_York",
)
ticker_details_schedule = dg.ScheduleDefinition(
       name="ticker_details_schedule",
       job=ticker_details_job,
       cron_schedule="0 9 * * 0",          # Sundays at 9 AM
       execution_timezone="America/New_York",
   )


# DEFINITIONS:
defs = dg.Definitions(
    assets=[
        raw_day_aggs_file,
        raw_day_aggs_table,
        dash_exports_to_s3,
        raw_dash_exports_table,
        market_dbt_models,raw_ticker_details_file,
        raw_ticker_details_table,
    ],
    jobs=[ingest_job, transform_job, exports_job,ticker_details_job],
    schedules=[daily_ingest_schedule, daily_transform_schedule, dash_exports_schedule,ticker_details_schedule],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)