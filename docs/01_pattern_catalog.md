# Pattern Catalog

Each pattern is specified with the same template. That consistency *is* the deliverable —
when a client says "the patterns are already defined," what they usually have is a name and
a box on a slide. The value you add is the twelve rows below, filled in the same way every
time, so a developer can implement without asking follow-up questions.

**Spec template**

| Field | Meaning |
|---|---|
| Intent | The one-sentence job this pattern does |
| Use when / Don't use when | Selection criteria, so patterns aren't picked by habit |
| Source contract | What the source must guarantee before this pattern is viable |
| Target model | Where data lands and in what shape |
| Delivery semantics | At-least-once / exactly-once, and how idempotency is achieved |
| Schema drift | What happens when the source adds, drops, or retypes a column |
| Data quality | Which checks run, and what happens to failing rows |
| Failure & replay | How to recover, and how to reprocess a window |
| Governance | UC objects, ownership, grants, PII handling |
| Observability | What is monitored and what alerts |
| Cost profile | Compute shape and the main cost driver |
| Implementation | File in this repo |

---

## P1 — Batch file landing

| Field | Detail |
|---|---|
| **Intent** | Ingest files a source system drops into object storage on a schedule, without rescanning the whole directory. |
| **Use when** | Source can only export files; volumes are moderate; daily or hourly SLA is acceptable. |
| **Don't use when** | Files are mutated in place after landing (Auto Loader assumes append-only), or the SLA is sub-minute. |
| **Source contract** | Immutable files, unique filenames, a manifest or `_SUCCESS` marker per batch, agreed encoding and delimiter. |
| **Target model** | `bronze.supplier_catalog_raw` — append-only, one row per source row, plus file metadata and ingest timestamp. |
| **Delivery semantics** | Exactly-once per file via Auto Loader's checkpoint and RocksDB file registry. Reprocessing a file requires either a new checkpoint or a targeted backfill. |
| **Schema drift** | `cloudFiles.schemaEvolutionMode = addNewColumns`. New columns are added and the stream restarts once; unparseable values land in `_rescued_data` rather than failing the batch. |
| **Data quality** | Null key check and rescued-data check at bronze. Failing rows are routed to `quarantine.supplier_catalog_bad` with a reason, not dropped. |
| **Failure & replay** | Job retries twice. For replay, move files back to the landing zone under a new name, or use `cloudFiles.backfillInterval` for missed-notification recovery. |
| **Governance** | Files land in a UC Volume, not a DBFS path. External location + storage credential own the S3 access; no instance profiles on the cluster. |
| **Observability** | Row counts in vs. quarantined per run; file arrival lag; last successful commit timestamp. |
| **Cost profile** | Job compute with `Trigger.AvailableNow` — starts, drains the backlog, terminates. Cost scales with data, not with wall-clock. Directory listing on large buckets is the usual surprise; switch to file notification mode past ~10k files per run. |
| **Implementation** | `src/p1_batch_file_autoloader.py` |

**Steps**
1. Create the external location, storage credential, and Volume (`setup/00_uc_setup.sql`).
2. Agree the file contract with the source team and record it in this table.
3. Define the expected schema; pin it rather than inferring it in prod.
4. Configure Auto Loader with a checkpoint path under the Volume.
5. Split good and quarantined rows in a single `foreachBatch`.
6. Register the job with `availableNow` and a file-arrival trigger or cron.
7. Add the row-count and lag metrics to the observability dashboard.

---

## P2 — Database CDC to SCD Type 2

| Field | Detail |
|---|---|
| **Intent** | Keep a dimension in the lakehouse in step with an operational database, preserving history. |
| **Use when** | The business needs point-in-time answers ("what was this customer's tier when the order was placed?"). |
| **Don't use when** | Only current state matters — use SCD Type 1 and save the storage and the query complexity. |
| **Source contract** | Every change event carries a primary key, an operation flag (`I`/`U`/`D`), and a monotonically increasing sequence or commit timestamp. Out-of-order delivery is allowed; missing sequence is not. |
| **Target model** | `silver.customer_scd2` with `__START_AT` / `__END_AT` maintained by the platform. |
| **Delivery semantics** | Idempotent. `AUTO CDC` reorders by `SEQUENCE BY`, so replaying the same events converges to the same state. |
| **Schema drift** | Handled at bronze by Auto Loader; the CDC flow uses `except_column_list` so new columns flow through without a code change. |
| **Data quality** | Expectations at bronze drop rows missing a key or sequence. A reconciliation query compares active-row counts against the source. |
| **Failure & replay** | Full refresh of the target streaming table rebuilds history from the retained CDF. Partial replay is not supported for SCD2 — plan retention accordingly. |
| **Governance** | The dimension carries PII. Column masks on email and phone; row filter by region for the analyst group. |
| **Observability** | Pipeline event log — expectation pass rate, flow duration, backlog. Alert on expectation drop rate above a threshold. |
| **Cost profile** | Serverless pipeline, triggered every 15 minutes rather than continuous. Continuous mode roughly doubles cost for a dimension that changes this slowly. |
| **Implementation** | `src/p2_cdc_scd2_pipeline.py` |

**Steps**
1. Confirm the source can emit a sequence column. Without one, this pattern is not viable — fall back to `AUTO CDC FROM SNAPSHOT`.
2. Land raw change events to bronze with Auto Loader (or Lakeflow Connect if the source is a supported managed connector).
3. Declare the target streaming table explicitly.
4. Declare the CDC flow with keys, sequence, delete predicate, and `STORED AS SCD TYPE 2`.
5. Add expectations and decide drop vs. fail vs. warn for each.
6. Run a full refresh, then validate history against a known changed record.
7. Schedule the pipeline from the orchestrating job.

---

## P3 — Event stream ingestion

| Field | Detail |
|---|---|
| **Intent** | Land high-frequency events with low latency and make them queryable without duplicates. |
| **Use when** | Downstream needs freshness measured in seconds, and the source publishes to a broker. |
| **Don't use when** | Downstream consumers only read once a day — batch the same source instead and skip the always-on compute. |
| **Source contract** | Broker retains at least 24 hours. Every event has a producer-assigned `event_id` and an `event_ts`. Late arrival bounded (agree the bound — this becomes the watermark). |
| **Target model** | `bronze.shipment_events` (append) → `silver.shipment_events_dedup` (deduplicated within watermark) → `gold.shipment_status_current`. |
| **Delivery semantics** | At-least-once from the broker; effectively-once downstream via `dropDuplicatesWithinWatermark` on `event_id`. |
| **Schema drift** | Payload parsed from JSON against an explicit schema with a rescue column. New fields appear in rescue until the schema is updated deliberately. |
| **Data quality** | Reject events with `event_ts` in the future or older than the watermark bound; count them rather than silently dropping. |
| **Failure & replay** | Checkpoint holds broker offsets. Replay by starting a new query with a fresh checkpoint and `startingOffsets` set to a timestamp. Never delete a checkpoint in place. |
| **Governance** | Broker credentials from a UC-governed secret scope or service credential — never inline in the notebook. |
| **Observability** | Consumer lag, batch duration vs. trigger interval, state store size. A state store growing without bound means the watermark is wrong. |
| **Cost profile** | The one always-on component. Use the smallest viable cluster and a trigger interval that matches the actual SLA. A 30-second trigger on a 5-minute SLA is pure waste. |
| **Implementation** | `src/p3_streaming_events.py` |

**Steps**
1. Agree the late-arrival bound with the source team; it drives the watermark and the state size.
2. Configure the broker read with offsets, auth, and `maxOffsetsPerTrigger` to cap batch size.
3. Parse the payload with an explicit schema.
4. Apply watermark, then deduplicate on `event_id`.
5. Write bronze with a checkpoint on a Volume path.
6. Build the current-status view as a separate aggregation.
7. Set alerts on lag and batch duration.

---

## P4 — API incremental pull

| Field | Detail |
|---|---|
| **Intent** | Pull only what changed from a REST source that has no file export and no CDC. |
| **Use when** | The API supports a `modified_since` filter and stable pagination. |
| **Don't use when** | The API only returns full snapshots — then this is a snapshot pattern, and `AUTO CDC FROM SNAPSHOT` is the right tool. |
| **Source contract** | Server-side filtering on a modification timestamp, deterministic pagination, documented rate limits, stable IDs. |
| **Target model** | `bronze.carrier_rates_raw` (append, one row per API record per pull) → `silver.carrier_rates` (merged current state). |
| **Delivery semantics** | At-least-once pull, idempotent write. The watermark advances only after a successful commit, so a crash re-pulls an overlapping window rather than skipping one. |
| **Schema drift** | Response stored as raw JSON string in bronze alongside the parsed columns. Parsing changes never require re-pulling from the API. |
| **Data quality** | Row count sanity check against the API's reported total; alert if a pull returns zero rows on a business day. |
| **Failure & replay** | Set the watermark row back and re-run. Because the write is a MERGE on business key, replay is safe. |
| **Governance** | API token in a Databricks secret scope with ACLs on a service principal group, never a user PAT. |
| **Observability** | Watermark age, records per pull, HTTP error counts, retry counts. |
| **Cost profile** | Single-node job compute. This is I/O-bound, not CPU-bound — a multi-node cluster here is a common and expensive mistake. |
| **Implementation** | `src/p4_api_incremental_pull.py` |

**Steps**
1. Create the control table for watermarks.
2. Store credentials in a secret scope; grant READ to the job's service principal.
3. Implement the pull with backoff, retry, and page-limit guards.
4. Overlap the window slightly (e.g. watermark minus 5 minutes) to tolerate clock skew.
5. Write raw response to bronze, then MERGE into silver on business key.
6. Advance the watermark inside the same transaction boundary as the commit.
7. Alert on watermark age exceeding two pull intervals.

---

## P5 — Federated read (zero-copy)

| Field | Detail |
|---|---|
| **Intent** | Query a source system in place, without building a pipeline. |
| **Use when** | Data is small, changes constantly, is needed live, or is only needed for exploration or a migration cutover. |
| **Don't use when** | The query is on a dashboard hit hundreds of times a day, or the source is production OLTP under load. Federation moves the cost onto the source system, which is exactly where it is least visible to you. |
| **Source contract** | A read replica, a dedicated read-only account, network reachability from serverless or the SQL warehouse, and an agreed query concurrency cap. |
| **Target model** | None — a foreign catalog mirrors the source. Optionally one materialized view for the handful of queries that get run repeatedly. |
| **Delivery semantics** | Not applicable; reads are live. |
| **Schema drift** | Databricks keeps the foreign catalog definition in sync with the source, so drift surfaces at query time rather than at load time. |
| **Data quality** | Inherited from the source. State this explicitly — federation means you have accepted the source's quality as-is. |
| **Failure & replay** | Source outage means query failure. Any consumer with an availability requirement needs a materialized fallback. |
| **Governance** | The connection is a UC securable. Grant `USE CONNECTION` narrowly; the connection credential is effectively a shared identity into the source, so treat it as a privileged object. |
| **Observability** | Query history filtered to the foreign catalog; watch for full-table scans where predicate pushdown failed. |
| **Cost profile** | No storage, no ingest compute. Cost lands on the SQL warehouse and on the source database. Verify pushdown with `EXPLAIN` — a filter that doesn't push down pulls the whole table across the wire. |
| **Implementation** | `sql/p5_federation.sql` |

**Steps**
1. Create the secret scope entries for the source credentials.
2. `CREATE CONNECTION` with `secret()` references, never literals.
3. `CREATE FOREIGN CATALOG` against the connection.
4. Grant `USE CATALOG` / `SELECT` to the consuming group only.
5. Run `EXPLAIN` on the representative queries and confirm pushdown.
6. Document the concurrency cap agreed with the source DBA.

---

## P6 — Outbound share and serving

| Field | Detail |
|---|---|
| **Intent** | Publish governed data to BI tools and to external partners without copying files around. |
| **Use when** | Consumers are on Databricks, or on any Delta Sharing client, and you want one governed copy. |
| **Don't use when** | The partner needs a flat file on their SFTP — that is a different pattern, and pretending otherwise wastes a sprint. |
| **Source contract** | Reverse direction: *you* are the source. Publish a stable contract — column names, types, grain, refresh time, and a deprecation policy. |
| **Target model** | `gold.order_fact_daily` plus a dynamic view enforcing row and column policy for external recipients. |
| **Delivery semantics** | Consumers read a consistent Delta snapshot. Version pinning available via time travel. |
| **Schema drift** | Additive only. Breaking changes go through a versioned view (`v2_order_fact_daily`), with both live during a deprecation window. |
| **Data quality** | Gold-level checks run before the share is refreshed; a failed check blocks publication rather than shipping bad data. |
| **Failure & replay** | Time travel to the last known-good version and re-share. |
| **Governance** | Recipients are UC objects. Share the *view*, not the base table. Rotate recipient tokens on a schedule and audit `system.access.audit` for recipient reads. |
| **Observability** | Recipient access in audit logs; freshness of the gold table versus its promised refresh time. |
| **Cost profile** | Egress is the cost, and it is the consumer's query pattern that drives it. Set expectations in the data contract. |
| **Implementation** | `sql/p6_gold_and_delta_sharing.sql` |

**Steps**
1. Build and validate the gold table.
2. Define the consumer-facing view with row filter and column mask.
3. Create the share and add the view.
4. Create the recipient; deliver the activation link out of band.
5. Publish the data contract alongside the share.
6. Add recipient-read monitoring to the observability dashboard.

---

## Cross-cutting decisions

These are asked in every client review and are worth having written down before the meeting.

**Orchestration.** One job per integration domain, not one job per table. Pipelines are
triggered as tasks inside jobs so that the run history and alerting live in one place.

**Naming.** `<layer>_<source>_<entity>` for tables, `<domain>_<pattern>_job` for jobs.
Boring, but a client team of fifteen will thank you.

**Environments.** `dev` and `prod` targets in one bundle; catalog is a bundle variable, so
the same code deploys to `orderhub_dev` and `orderhub_prod` without edits.

**Quality tiering.** Bronze quarantines, silver drops with a counted expectation, gold
fails the run. Severity should rise as data gets closer to a decision.

**Cost guardrails.** Serverless where the workload is bursty, job compute where it is
predictable, a budget policy tagging every resource to this project, and no all-purpose
compute in a scheduled path.
