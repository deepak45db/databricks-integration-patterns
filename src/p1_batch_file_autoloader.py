"""
Pattern 1 - Batch file landing to bronze, with quarantine.

DATABRICKS : Auto Loader (cloudFiles), UC Volumes, _metadata column, dbutils
PYSPARK    : schema definition, column expressions, foreachBatch split

Why a stream for a batch job: Auto Loader tracks which files it has already seen
in its checkpoint, so a daily run only reads new files without any date-partition
gymnastics or a manifest table. Trigger.AvailableNow makes it behave like a batch
job - drain the backlog, then stop.

Docs: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType,
)

from common.config import Config, get_param
from common.transforms import add_ingest_metadata, split_valid_invalid

spark = SparkSession.builder.getOrCreate()

# Job parameters (argv from spark_python_task, or widgets from a notebook)
cfg = Config(catalog=get_param("catalog", "orderhub_dev"))

SOURCE = "supplier_catalog"
LANDING = f"{cfg.volume_files}/{SOURCE}"
CHECKPOINT = f"{cfg.volume_checkpoints}/p1_{SOURCE}"
BRONZE_TABLE = f"{cfg.catalog}.bronze.supplier_catalog_raw"
QUARANTINE_TABLE = f"{cfg.catalog}.quarantine.supplier_catalog_bad"

# -----------------------------------------------------------------------------
# PYSPARK: pin the schema. Inferring in production means a malformed file can
# silently change column types between runs. Inference is for exploration only.
# -----------------------------------------------------------------------------
SOURCE_SCHEMA = StructType(
    [
        StructField("sku", StringType(), True),
        StructField("supplier_id", StringType(), True),
        StructField("description", StringType(), True),
        StructField("category", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("uom", StringType(), True),
        StructField("effective_date", DateType(), True),
    ]
)

# -----------------------------------------------------------------------------
# DATABRICKS: Auto Loader read
# -----------------------------------------------------------------------------
raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT}/_schema")
    # addNewColumns: a new source column stops the stream once, then it restarts
    # with the wider schema. Prefer this to "rescue" when you want to be told.
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    # anything that will not cast lands here instead of failing the batch
    .option("rescuedDataColumn", "_rescued_data")
    .option("header", "true")
    # past ~10k files per run, switch to file notification mode:
    # .option("cloudFiles.useNotifications", "true")
    .schema(SOURCE_SCHEMA)
    .load(LANDING)
)

# PYSPARK: lineage columns. _metadata is Databricks-provided but the rest is plain Spark.
enriched = add_ingest_metadata(raw, source_name=SOURCE)

# -----------------------------------------------------------------------------
# Quality gate at bronze: split rather than drop, so nothing disappears silently.
# -----------------------------------------------------------------------------
RULES = {
    "missing_sku": F.col("sku").isNull(),
    "unparseable_field": F.col("_rescued_data").isNotNull(),
    "negative_price": F.col("unit_price") < 0,
}


def write_batch(batch_df, batch_id: int):
    """PYSPARK: foreachBatch gives one pass over the data and two sinks."""
    batch_df = batch_df.cache()
    valid_df, invalid_df = split_valid_invalid(batch_df, RULES)

    (
        valid_df.drop("_rescued_data")
        .write.mode("append")
        .saveAsTable(BRONZE_TABLE)
    )

    (
        invalid_df.select(
            F.current_timestamp().alias("quarantined_at"),
            F.col("_source_file").alias("source_file"),
            F.col("_failure_reason").alias("failure_reason"),
            F.to_json(F.struct([c for c in batch_df.columns])).alias("raw_payload"),
        )
        .write.mode("append")
        .saveAsTable(QUARANTINE_TABLE)
    )

    good = valid_df.count()
    bad = invalid_df.count()
    print(f"batch {batch_id}: {good} written, {bad} quarantined")

    # ops audit row - this is what the observability dashboard reads
    spark.sql(
        f"""
        INSERT INTO {cfg.catalog}.ops.run_audit VALUES (
          '{batch_id}', 'P1', '{SOURCE}', {good + bad}, {good}, {bad},
          current_timestamp(), current_timestamp(), 'SUCCESS'
        )
        """
    )
    batch_df.unpersist()


# -----------------------------------------------------------------------------
# DATABRICKS: availableNow = batch semantics on a streaming engine.
# -----------------------------------------------------------------------------
query = (
    enriched.writeStream.foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# Post-run maintenance. Predictive optimization handles this automatically on
# managed UC tables; left explicit here so the PoC shows the intent.
spark.sql(f"OPTIMIZE {BRONZE_TABLE}")
