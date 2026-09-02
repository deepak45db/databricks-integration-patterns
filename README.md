# Integration Patterns Reference Implementation (Databricks)

A small, self-contained proof of concept that takes a set of **already-defined data
integration patterns**, writes the detailed spec for each one, and proves them out with
working code on Databricks.

This mirrors the shape of the internal ask: *"patterns are already defined; need someone
who can do the details on those patterns and do steps etc. with PoC."* The deliverable is
therefore three things, not one:

1. **Pattern catalog** — one detailed spec per pattern (`docs/01_pattern_catalog.md`)
2. **Wireframes** — context, flow, and deployment diagrams (`docs/02_architecture_wireframe.md`)
3. **Working PoC** — runnable code for all six patterns (`src/`, `sql/`, `resources/`)

---

## The scenario

`OrderHub` is a mid-size distributor. Six source systems feed one lakehouse:

| # | Pattern | Source | Latency | Mechanism |
|---|---------|--------|---------|-----------|
| P1 | Batch file landing | Supplier catalog drops (CSV/JSON to a Volume) | Daily | Auto Loader, `availableNow` |
| P2 | Database CDC → SCD2 | Postgres `customers` change feed | 15 min | Lakeflow pipelines, `AUTO CDC` |
| P3 | Event stream | Shipment tracking events (Kafka) | Seconds | Structured Streaming, watermark + dedupe |
| P4 | API pull | Carrier rate API | Hourly | Incremental pull, high-water mark, idempotent MERGE |
| P5 | Federated read | Postgres reference data | Live, zero-copy | Lakehouse Federation |
| P6 | Outbound share | Partner + BI consumption | Daily | Delta Sharing, dynamic views |

Cross-cutting concerns handled once, not per-pattern: Unity Catalog governance, data
quality, orchestration, observability, cost control, and CI/CD.

---

## Repo layout

```
docs/
  01_pattern_catalog.md        The spec for each pattern (the "details" deliverable)
  02_architecture_wireframe.md Mermaid diagrams: context, flow, deployment
  03_build_runbook.md          Step-by-step build order, ~5 sessions
  04_demo_script.md            15-minute walkthrough incl. failure demos
setup/
  00_uc_setup.sql              Catalog, schemas, volumes, groups, grants
  01_generate_sample_data.py   Synthetic source data for all six patterns
src/
  common/config.py             Single config surface, no hardcoded paths
  common/transforms.py         Pure PySpark transforms (unit-testable)
  p1_batch_file_autoloader.py  Pattern 1
  p2_cdc_scd2_pipeline.py      Pattern 2 (Lakeflow pipeline source file)
  p3_streaming_events.py       Pattern 3
  p4_api_incremental_pull.py   Pattern 4
sql/
  p5_federation.sql            Pattern 5
  p6_gold_and_delta_sharing.sql Pattern 6
  observability.sql            System tables + pipeline event log queries
resources/
  databricks.yml               Bundle root, dev/prod targets
  jobs.yml                     Lakeflow Jobs definitions
  pipelines.yml                Lakeflow pipeline definition
tests/
  test_transforms.py           Pytest over the pure transforms
```

## Code labelling convention

Every file marks which parts are portable and which are platform-bound:

- `# PYSPARK` — plain Spark/PySpark, runs anywhere Spark runs
- `# DATABRICKS` — Databricks-specific (Auto Loader, `dbutils`, Unity Catalog SQL,
  Lakeflow pipelines, Delta Sharing, system tables)

This matters in a client conversation: it tells you what is portable if the client later
moves, and what is genuine platform lock-in you should be able to defend.

## Prerequisites

- A Unity Catalog-enabled workspace (Databricks Free Edition works for P1–P4 and P6-partial)
- Permission to create a catalog, or an existing catalog you own
- Databricks CLI v0.230+ for bundle deployment
- P3 needs a Kafka endpoint; a rate-source simulator is included as a fallback
- P5 needs a reachable Postgres; a local instance over ngrok is fine for a PoC

## Quick start

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run -t dev orderhub_setup_job
databricks bundle run -t dev orderhub_batch_integration_job
```

Then work through `docs/03_build_runbook.md`.

## Documentation

All Databricks references in this repo point at <https://docs.databricks.com/aws/en>.
Product naming moves fast in this area — DLT is now Lakeflow Spark Declarative Pipelines,
`apply_changes()` is now `create_auto_cdc_flow()`, Workflows are Lakeflow Jobs, and the
`dlt` module is superseded by `pyspark.pipelines`. Check the docs before a demo; the
older names still work but read as dated.
