
# Checks that Python can log in to Snowflake as DAGSTER_SVC.

import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# Log in using the files from .env
conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
    role="PIPELINE_ROLE",
    warehouse="NEWS_WH",
)

# Test the connection to snowflake by asking a simple question: how many rows are in the raw table?
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM NEWS_PIPELINE.RAW.RAW_DAY_AGGS")
print("Rows in RAW_DAY_AGGS:", cursor.fetchone()[0])

conn.close()