"""
Unit tests for the pure transforms.

PYSPARK only - these run on a local SparkSession with no workspace, which is the
point. Anything that needs Unity Catalog or Auto Loader is not tested here; it is
covered by the integration run in docs/03_build_runbook.md.

    pip install pyspark pytest
    pytest tests/ -v
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common.transforms import (  # noqa: E402
    add_ingest_metadata,
    build_failure_reason,
    conform_currency,
    split_valid_invalid,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("integration-patterns-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def catalog_df(spark):
    return spark.createDataFrame(
        [
            ("SKU-1", "SUP-1", 10.0, None),      # clean
            (None, "SUP-2", 12.0, None),         # missing key
            ("SKU-3", "SUP-3", -1.0, None),      # negative price
            ("SKU-4", "SUP-4", 5.0, '{"x":1}'),  # unparseable source field
            (None, "SUP-5", -3.0, None),         # two problems at once
        ],
        "sku string, supplier_id string, unit_price double, _rescued_data string",
    )


@pytest.fixture
def rules(spark):
    """Built inside a fixture: F.col() needs an active SparkContext."""
    return {
        "missing_sku": F.col("sku").isNull(),
        "unparseable_field": F.col("_rescued_data").isNotNull(),
        "negative_price": F.col("unit_price") < 0,
    }


def test_split_keeps_every_row(catalog_df, rules):
    """Nothing is dropped: valid + invalid must equal the input."""
    valid, invalid = split_valid_invalid(catalog_df, rules)
    assert valid.count() + invalid.count() == catalog_df.count()


def test_only_clean_rows_pass(catalog_df, rules):
    valid, _ = split_valid_invalid(catalog_df, rules)
    assert valid.count() == 1
    assert valid.head()["sku"] == "SKU-1"


def test_failure_reasons_are_diagnosable(catalog_df, rules):
    """A row failing two rules should report both, not just the first."""
    _, invalid = split_valid_invalid(catalog_df, rules)
    reasons = {r["supplier_id"]: r["_failure_reason"] for r in invalid.collect()}
    assert reasons["SUP-2"] == "missing_sku"
    assert reasons["SUP-3"] == "negative_price"
    assert set(reasons["SUP-5"].split(",")) == {"missing_sku", "negative_price"}


def test_empty_rules_pass_everything(catalog_df):
    valid, invalid = split_valid_invalid(catalog_df, {})
    assert invalid.count() == 0
    assert valid.count() == catalog_df.count()


def test_build_failure_reason_is_empty_for_clean_rows(spark):
    df = spark.createDataFrame([("SKU-9", 1.0)], "sku string, unit_price double")
    out = df.withColumn(
        "reason", build_failure_reason({"neg": F.col("unit_price") < 0})
    )
    assert out.head()["reason"] == ""


def test_add_ingest_metadata_without_file_source(spark):
    df = spark.createDataFrame([("a",)], "c string")
    out = add_ingest_metadata(df, "unit_test", with_file_metadata=False)
    row = out.head()
    assert row["_source_name"] == "unit_test"
    assert row["_ingest_ts"] is not None
    assert row["_source_file"] is None


def test_currency_becomes_decimal(spark):
    df = spark.createDataFrame([(10.005,)], "amount double")
    out = conform_currency(df, "amount")
    assert out.schema["amount"].dataType.simpleString() == "decimal(18,2)"
