"""
Shared configuration.

PYSPARK/plain Python. No Databricks imports here on purpose, so the module can be
imported by pytest on a laptop without a workspace.
"""

import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """One place for every path and name. No string literals scattered in the code."""

    catalog: str
    kafka_bootstrap: str = "kafka.internal.example.com:9093"
    api_base_url: str = "https://api.carrier-rates.example.com"

    @property
    def volume_files(self) -> str:
        return f"/Volumes/{self.catalog}/landing/files"

    @property
    def volume_checkpoints(self) -> str:
        return f"/Volumes/{self.catalog}/landing/checkpoints"

    def table(self, layer: str, name: str) -> str:
        return f"{self.catalog}.{layer}.{name}"


def get_param(name: str, default: str = "") -> str:
    """
    Read a job parameter without caring how the task was defined.

    spark_python_task passes "--name value" on argv; notebook_task passes widgets.
    Supporting both means the same file runs from a job, from a notebook, and
    from a local test without edits - which is the difference between a PoC that
    demos once and one a client team can actually pick up.
    """
    argv = sys.argv[1:]
    flag = f"--{name}"
    if flag in argv:
        idx = argv.index(flag)
        if idx + 1 < len(argv):
            return argv[idx + 1]
    try:  # DATABRICKS: only available inside a notebook context
        from pyspark.dbutils import DBUtils  # noqa: F401
        from pyspark.sql import SparkSession

        dbutils = DBUtils(SparkSession.builder.getOrCreate())
        return dbutils.widgets.get(name)
    except Exception:
        return default
