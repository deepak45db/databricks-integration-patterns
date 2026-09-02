"""
Pure transforms, isolated so they can be unit tested without a workspace.

PYSPARK only. Nothing in this module touches dbutils, Unity Catalog, or Auto
Loader. That separation is what makes tests/test_transforms.py possible, and it
is the thing most PoC code skips.
"""

from typing import Dict, Tuple

from pyspark.sql import DataFrame, Column, functions as F


def add_ingest_metadata(
    df: DataFrame, source_name: str, with_file_metadata: bool = True
) -> DataFrame:
    """
    Lineage columns every bronze table should carry.

    with_file_metadata=True uses the Databricks-provided _metadata column, which
    only exists for file-based readers. Set it False for tests and for non-file
    sources so the same function serves every pattern.
    """
    source_file = (
        F.col("_metadata.file_path") if with_file_metadata else F.lit(None).cast("string")
    )
    return (
        df.withColumn("_source_name", F.lit(source_name))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", source_file)
    )


def build_failure_reason(rules: Dict[str, Column]) -> Column:
    """
    Turn a dict of {reason: predicate} into one column listing every reason a row
    failed. Concatenating reasons beats stopping at the first: a row with three
    problems should be reported as having three problems.
    """
    parts = [F.when(pred, F.lit(reason)) for reason, pred in rules.items()]
    return F.concat_ws(",", F.array_compact(F.array(*parts)))


def split_valid_invalid(
    df: DataFrame, rules: Dict[str, Column]
) -> Tuple[DataFrame, DataFrame]:
    """
    Split a batch into rows that pass every rule and rows that fail at least one.
    Failing rows carry _failure_reason so quarantine is diagnosable rather than
    just a pile of rejected data nobody ever looks at.
    """
    tagged = df.withColumn("_failure_reason", build_failure_reason(rules))
    valid = tagged.filter(F.col("_failure_reason") == "").drop("_failure_reason")
    invalid = tagged.filter(F.col("_failure_reason") != "")
    return valid, invalid


def conform_currency(df: DataFrame, col_name: str, scale: int = 2) -> DataFrame:
    """Money as DECIMAL, never DOUBLE. Cheap to do here, expensive to fix later."""
    return df.withColumn(col_name, F.col(col_name).cast(f"decimal(18,{scale})"))
