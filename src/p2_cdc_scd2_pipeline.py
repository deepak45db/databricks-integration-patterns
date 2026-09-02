"""
Pattern 2 - Database CDC to an SCD Type 2 dimension.

DATABRICKS : Lakeflow Spark Declarative Pipelines, AUTO CDC, expectations,
             Auto Loader. This file is a *pipeline source file* - it is never run
             as a notebook or a script; the pipeline runtime evaluates it.
PYSPARK    : the DataFrame expressions inside each function are ordinary PySpark.

Naming note: the old `dlt` module still works, but the current module is
`pyspark.pipelines`, imported as `dp`, and `apply_changes()` is now
`create_auto_cdc_flow()`. Same signature, current name.
Docs: https://docs.databricks.com/aws/en/ldp/developer/python-ref
"""

from pyspark import pipelines as dp          # DATABRICKS
from pyspark.sql import functions as F       # PYSPARK

# Pipeline configuration values come from the pipeline definition in
# resources/pipelines.yml, not from widgets.
CATALOG = spark.conf.get("orderhub.catalog")          # noqa: F821
LANDING = f"/Volumes/{CATALOG}/landing/files/customer_cdc"

# =============================================================================
# Bronze - land the change events exactly as they arrive
# =============================================================================
EXPECTATIONS_BRONZE = {
    "valid_key": "customer_id IS NOT NULL",
    "valid_sequence": "commit_ts IS NOT NULL",
    "known_operation": "op IN ('I', 'U', 'D')",
}


@dp.table(
    name="bronze_customer_cdc",
    comment="Raw customer change events, source-faithful.",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
)
@dp.expect_all_or_drop(EXPECTATIONS_BRONZE)
def bronze_customer_cdc():
    """
    DATABRICKS: Auto Loader inside a pipeline.
    expect_all_or_drop means a row missing its key never reaches the CDC flow,
    where it would otherwise become a phantom dimension member. Dropped rows are
    still counted in the pipeline event log, so nothing vanishes unaudited.
    """
    return (
        spark.readStream.format("cloudFiles")          # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(LANDING)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingest_ts"),
        )
    )


# =============================================================================
# Silver - SCD Type 2 dimension
# =============================================================================
dp.create_streaming_table(
    name="silver_customer_scd2",
    comment="Customer dimension with full history. __START_AT / __END_AT are "
            "maintained by AUTO CDC.",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="silver_customer_scd2",
    source="bronze_customer_cdc",
    keys=["customer_id"],
    # Out-of-order events are reordered by this column. Without a reliable
    # sequence, this pattern is not viable - see the catalog entry for P2.
    sequence_by=F.col("commit_ts"),
    # A delete closes the current row rather than removing history.
    apply_as_deletes=F.expr("op = 'D'"),
    # Operational columns must not become dimension attributes.
    except_column_list=["op", "commit_ts", "_rescued_data", "_source_file", "_ingest_ts"],
    stored_as_scd_type="2",
)


# =============================================================================
# Gold - the current-state view most consumers actually want
# =============================================================================
@dp.materialized_view(
    name="gold_customer_current",
    comment="Active customer rows only. Most dashboards want this, not the history.",
)
@dp.expect_or_fail("tier_is_known", "tier IN ('bronze','silver','gold','platinum')")
def gold_customer_current():
    """
    PYSPARK: ordinary DataFrame logic over the SCD2 table.
    An open-ended row (__END_AT IS NULL) is the current version of each key.
    expect_or_fail at gold: bad data here reaches a decision-maker, so the run
    should stop rather than publish.
    """
    return (
        spark.read.table("silver_customer_scd2")       # noqa: F821
        .filter(F.col("__END_AT").isNull())
        .drop("__START_AT", "__END_AT")
    )


# =============================================================================
# SQL equivalent, for the client team that prefers SQL. Same semantics.
# =============================================================================
# CREATE OR REFRESH STREAMING TABLE silver_customer_scd2;
#
# CREATE FLOW customer_cdc_flow AS AUTO CDC INTO silver_customer_scd2
#   FROM stream(bronze_customer_cdc)
#   KEYS (customer_id)
#   APPLY AS DELETE WHEN op = 'D'
#   SEQUENCE BY commit_ts
#   COLUMNS * EXCEPT (op, commit_ts, _rescued_data, _source_file, _ingest_ts)
#   STORED AS SCD TYPE 2;
