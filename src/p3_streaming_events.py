"""
Pattern 3 - Event stream ingestion.

DATABRICKS : UC Volume checkpoints, secret scope, table ACLs
PYSPARK    : Structured Streaming, watermarking, dropDuplicatesWithinWatermark,
             stateful aggregation - all portable Spark

Two source modes:
  kafka  - the real pattern
  files  - a fallback so the PoC runs without a broker (Auto Loader over the
           generated event files). Same downstream logic either way.

The interesting part of this pattern is not the read; it is the watermark. It is
the single decision that determines correctness, state size, and cost, and it is
almost always inherited from a slide rather than agreed with the source team.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType,
)

from common.config import Config, get_param

spark = SparkSession.builder.getOrCreate()

cfg = Config(catalog=get_param("catalog", "orderhub_dev"))
SOURCE_MODE = get_param("source_mode", "files")   # files | kafka
RUN_MODE = get_param("run_mode", "batch")         # batch | continuous

SOURCE = "shipment_events"
CHECKPOINT = f"{cfg.volume_checkpoints}/p3_{SOURCE}"
BRONZE = f"{cfg.catalog}.bronze.shipment_events"
SILVER = f"{cfg.catalog}.silver.shipment_events_dedup"
GOLD = f"{cfg.catalog}.gold.shipment_status_current"

# Agreed with the source team, not guessed. Drives both correctness and cost.
LATE_ARRIVAL_BOUND = "24 hours"

def trigger_opts():
    """
    batch      : availableNow - drain and stop. Use this for the PoC and for any
                 consumer whose SLA is minutes rather than seconds.
    continuous : a fixed micro-batch interval, always-on compute.
    Choosing 'batch' here is the single biggest cost lever in this pattern.
    """
    return {"availableNow": True} if RUN_MODE == "batch" else {"processingTime": "1 minute"}


EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("shipment_id", StringType(), False),
        StructField("order_id", StringType(), True),
        StructField("customer_id", LongType(), True),
        StructField("status", StringType(), True),
        StructField("carrier", StringType(), True),
        StructField("event_ts", TimestampType(), False),
    ]
)

# =============================================================================
# Read
# =============================================================================
if SOURCE_MODE == "kafka":
    # DATABRICKS: credentials from a secret scope, never inline
    kafka_user = dbutils.secrets.get("orderhub", "kafka_user")       # noqa: F821
    kafka_pass = dbutils.secrets.get("orderhub", "kafka_password")   # noqa: F821

    raw = (
        spark.readStream.format("kafka")  # PYSPARK
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
        .option("subscribe", "shipment.events.v1")
        .option("startingOffsets", "latest")
        # caps batch size so a backlog cannot produce one enormous micro-batch
        .option("maxOffsetsPerTrigger", 50_000)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{kafka_user}" password="{kafka_pass}";',
        )
        .load()
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
    )
else:
    # DATABRICKS: Auto Loader fallback so the pattern is demonstrable without Kafka
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT}/_schema")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(EVENT_SCHEMA)
        .load(f"{cfg.volume_files}/{SOURCE}")
    )

# =============================================================================
# Bronze - append everything, including duplicates. Bronze is the audit trail.
# =============================================================================
bronze_stream = raw.withColumn("_ingest_ts", F.current_timestamp())

(
    bronze_stream.writeStream.format("delta")
    .option("checkpointLocation", f"{CHECKPOINT}/bronze")
    .outputMode("append")
    .trigger(**trigger_opts())
    .toTable(BRONZE)
)

# =============================================================================
# Silver - deduplicate within the watermark
# =============================================================================
silver_stream = (
    spark.readStream.table(BRONZE)
    # PYSPARK: the watermark bounds how long state is retained. Too short drops
    # legitimate late events; too long grows the state store without limit.
    .withWatermark("event_ts", LATE_ARRIVAL_BOUND)
    # dropDuplicatesWithinWatermark does not need event_ts to match exactly,
    # which matters when a producer retries with a fresh timestamp.
    .dropDuplicatesWithinWatermark(["event_id"])
    .filter(F.col("event_ts") <= F.current_timestamp())  # reject future-dated events
)

(
    silver_stream.writeStream.format("delta")
    .option("checkpointLocation", f"{CHECKPOINT}/silver")
    .outputMode("append")
    .trigger(**trigger_opts())
    .toTable(SILVER)
)

# =============================================================================
# Gold - latest status per shipment
#   Batch, not streaming. A streaming aggregation here would hold state for every
#   shipment forever; a scheduled batch over silver costs a fraction of that.
# =============================================================================
def build_gold():
    """PYSPARK: latest event per shipment, plain batch DataFrame logic."""
    latest = Window.partitionBy("shipment_id").orderBy(F.col("event_ts").desc())
    (
        spark.read.table(SILVER)
        .withColumn("rn", F.row_number().over(latest))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD)
    )


if RUN_MODE == "batch":
    # availableNow queries terminate on their own; wait, then build gold.
    for q in spark.streams.active:
        q.awaitTermination()
    build_gold()
    print("P3: batch pass complete, gold rebuilt")
else:
    # Always-on. Gold is rebuilt by the separate scheduled batch job.
    spark.streams.awaitAnyTermination()
