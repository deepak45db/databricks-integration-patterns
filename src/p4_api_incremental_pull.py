"""
Pattern 4 - REST API incremental pull.

DATABRICKS : dbutils.secrets, UC control table, MERGE INTO on a Delta table
PYSPARK    : DataFrame construction, MERGE via DeltaTable API

The two things that make this pattern safe rather than merely working:
  1. The watermark advances only after the write commits, so a crash re-pulls an
     overlapping window instead of skipping records.
  2. The write is a MERGE on business key, so re-pulling that overlap is harmless.

Run on single-node compute. This is I/O-bound; a multi-node cluster spends money
to leave workers idle while one driver thread waits on HTTP.
"""

import time
from datetime import datetime, timedelta, timezone

import requests
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
)

from common.config import Config, get_param

spark = SparkSession.builder.getOrCreate()

cfg = Config(catalog=get_param("catalog", "orderhub_dev"))

SOURCE = "carrier_rates"
BRONZE = f"{cfg.catalog}.bronze.carrier_rates_raw"
SILVER = f"{cfg.catalog}.silver.carrier_rates"
WATERMARK_TABLE = f"{cfg.catalog}.ops.ingest_watermark"

# Overlap guards against clock skew between our runtime and the API server.
OVERLAP = timedelta(minutes=5)
PAGE_SIZE = 500
MAX_PAGES = 200          # circuit breaker: never loop forever on a broken cursor
MAX_RETRIES = 4

RATE_SCHEMA = StructType(
    [
        StructField("rate_id", StringType(), False),
        StructField("carrier", StringType(), True),
        StructField("origin_zip", StringType(), True),
        StructField("dest_zip", StringType(), True),
        StructField("service_level", StringType(), True),
        StructField("rate_usd", DoubleType(), True),
        StructField("modified_at", TimestampType(), False),
    ]
)


# =============================================================================
# Watermark helpers - DATABRICKS (Unity Catalog control table)
# =============================================================================
def read_watermark() -> datetime:
    row = (
        spark.read.table(WATERMARK_TABLE)
        .filter(F.col("source_name") == SOURCE)
        .select("watermark_ts")
        .head()
    )
    if row is None or row["watermark_ts"] is None:
        # First run: bounded backfill, not "everything since the beginning of time".
        return datetime.now(timezone.utc) - timedelta(days=7)
    return row["watermark_ts"]


def advance_watermark(new_ts: datetime) -> None:
    spark.sql(
        f"""
        MERGE INTO {WATERMARK_TABLE} AS t
        USING (SELECT '{SOURCE}' AS source_name,
                      TIMESTAMP '{new_ts.isoformat(sep=" ", timespec="seconds")}' AS wm) AS s
          ON t.source_name = s.source_name
        WHEN MATCHED THEN UPDATE SET
          t.watermark_ts = s.wm,
          t.updated_at = current_timestamp(),
          t.updated_by = current_user()
        WHEN NOT MATCHED THEN INSERT
          (source_name, watermark_ts, updated_at, updated_by)
          VALUES (s.source_name, s.wm, current_timestamp(), current_user())
        """
    )


# =============================================================================
# The pull itself - plain Python, no Spark
# =============================================================================
def fetch_page(session: requests.Session, since: datetime, page: int) -> dict:
    """Retry with exponential backoff. Respect Retry-After when the API sends it."""
    url = f"{cfg.api_base_url}/v1/rates"
    params = {
        "modified_since": since.isoformat(),
        "page": page,
        "page_size": PAGE_SIZE,
    }
    for attempt in range(MAX_RETRIES):
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 2 ** attempt))
            print(f"rate limited, sleeping {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code >= 500:
            time.sleep(2 ** attempt)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"{SOURCE}: page {page} failed after {MAX_RETRIES} attempts")


def pull_since(since: datetime) -> list[dict]:
    token = dbutils.secrets.get("orderhub", "carrier_api_token")  # noqa: F821  DATABRICKS
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    records, page = [], 1
    while page <= MAX_PAGES:
        payload = fetch_page(session, since, page)
        batch = payload.get("data", [])
        records.extend(batch)
        if not payload.get("has_more"):
            break
        page += 1
    else:
        raise RuntimeError(f"{SOURCE}: hit MAX_PAGES - check the pagination cursor")

    return records


# =============================================================================
# Main
# =============================================================================
watermark = read_watermark()
pull_from = watermark - OVERLAP
print(f"{SOURCE}: pulling records modified since {pull_from.isoformat()}")

records = pull_since(pull_from)
print(f"{SOURCE}: {len(records)} records returned")

if not records:
    # A zero-row pull on a business day is worth an alert, not a silent success.
    print(f"WARNING: {SOURCE} returned zero rows")
    dbutils.notebook.exit("no_records")  # noqa: F821

# PYSPARK: build the DataFrame, keep the raw payload for replay without re-pulling
raw_df = (
    spark.createDataFrame(records, schema=RATE_SCHEMA)
    .withColumn("_pulled_at", F.current_timestamp())
    .withColumn("_raw_payload", F.to_json(F.struct("*")))
)

raw_df.write.mode("append").saveAsTable(BRONZE)

# PYSPARK/DELTA: idempotent upsert on business key
raw_df.createOrReplaceTempView("incoming_rates")

# Target must exist before the first MERGE.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {SILVER} (
      rate_id STRING, carrier STRING, origin_zip STRING, dest_zip STRING,
      service_level STRING, rate_usd DOUBLE, modified_at TIMESTAMP,
      _pulled_at TIMESTAMP, _raw_payload STRING
    ) COMMENT 'Current carrier rates, merged on rate_id'
    """
)

spark.sql(
    f"""
    MERGE INTO {SILVER} AS t
    USING (
      -- an API can return the same rate_id twice across page boundaries;
      -- collapse to the latest before merging, or MERGE raises a
      -- multiple-source-rows-matched error
      SELECT * EXCEPT (rn) FROM (
        SELECT *, row_number() OVER (
                    PARTITION BY rate_id ORDER BY modified_at DESC) AS rn
        FROM incoming_rates
      ) WHERE rn = 1
    ) AS s
      ON t.rate_id = s.rate_id
    WHEN MATCHED AND s.modified_at > t.modified_at THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

# Only now is it safe to move the watermark forward.
max_modified = raw_df.agg(F.max("modified_at")).head()[0]
advance_watermark(max_modified)
print(f"{SOURCE}: watermark advanced to {max_modified}")
