# Build Runbook

Five sessions of roughly three hours. Build in this order — each session ends with
something demonstrable, so if you run out of time you still have a PoC rather than
a half-finished one.

Do not build all six patterns before testing any of them. Get P1 working end to
end first; it establishes the catalog, the volumes, the audit table, and the
quality-split pattern that P2–P4 reuse.

---

## Session 1 — Foundation and P1

**Goal:** files land, bronze fills, bad rows go to quarantine.

1. Create the catalog and schemas: run `setup/00_uc_setup.sql`. If you don't have
   `CREATE CATALOG` on your metastore, use an existing catalog you own and change
   the bundle variable.
2. Create both volumes. Checkpoints go on a Volume, not DBFS — DBFS root is
   deprecated for this and Unity Catalog access modes will fight you.
3. Run `setup/01_generate_sample_data.py` once.
4. Run `src/p1_batch_file_autoloader.py` from a notebook first, not from a job.
   Iterating on Auto Loader options is much faster interactively.
5. **Verify:** `bronze.supplier_catalog_raw` has rows, `quarantine.supplier_catalog_bad`
   has roughly 4% of them, and `ops.run_audit` has one row.
6. Run the generator a second time and re-run P1. **Verify:** only the new files
   were processed. This is the incremental proof — if the counts doubled, the
   checkpoint path is wrong.

**Failure to expect:** a `UnityCatalogAccessException` on the Volume path. Almost
always the compute access mode rather than the grants.

---

## Session 2 — P2, the CDC pipeline

**Goal:** SCD Type 2 history that survives a full refresh.

1. Create the pipeline in the UI first, pointing at `src/p2_cdc_scd2_pipeline.py`,
   with `orderhub.catalog` in the pipeline configuration. Move it into the bundle
   in session 5.
2. Set it to serverless. `AUTO CDC` needs serverless or the Pro/Advanced editions.
3. Run a full refresh. Read the pipeline graph before reading the tables — the
   graph is what you will screen-share in the demo.
4. **Verify** history is real:
   ```sql
   SELECT customer_id, tier, __START_AT, __END_AT
   FROM silver.silver_customer_scd2
   WHERE customer_id = (
     SELECT customer_id FROM silver.silver_customer_scd2
     GROUP BY customer_id HAVING count(*) > 2 LIMIT 1
   )
   ORDER BY __START_AT;
   ```
   You should see closed rows with an `__END_AT` and exactly one open row.
5. **Verify** deletes close rather than delete:
   ```sql
   SELECT count(*) FROM silver.silver_customer_scd2 WHERE __END_AT IS NULL;
   ```
   Deleted customers should be absent from that count but present in history.
6. Check the expectation counts in the pipeline event log.

**The thing to get right:** the generator shuffles events on purpose. If your
history looks wrong, it is almost certainly `sequence_by` pointing at ingest time
instead of commit time. Ingest order is not commit order, and confusing the two is
the classic CDC bug.

---

## Session 3 — P3 and P4

**Goal:** one always-on pattern and one pull pattern, both idempotent.

P3:
1. Run in `files` + `batch` mode first. Prove the logic before adding a broker.
2. **Verify** dedupe: bronze row count should exceed silver by roughly the 3%
   duplicate rate the generator injects.
3. **Verify** the watermark: events older than 24 hours should be absent from
   silver. Change `LATE_ARRIVAL_BOUND` to `1 hour` and re-run with a fresh
   checkpoint to see the count move. Being able to show that on demand is worth
   more in the interview than the pipeline running cleanly.
4. Only then point it at Kafka, if you have one.

P4:
1. Point `api_base_url` at any public JSON API with a modified-since filter, or
   stand up a five-line FastAPI stub locally. The API does not need to be real for
   the pattern to be real.
2. Run it twice in a row. **Verify** the second run pulls only the overlap window
   and the silver row count does not double. That is the idempotency proof.
3. Manually set the watermark back a day and re-run. **Verify** row counts are
   unchanged. That is the replay proof.

---

## Session 4 — P5, P6, and observability

**Goal:** the patterns compose, and you can answer cost and access questions.

1. P5: stand up Postgres anywhere reachable — RDS, or local behind a tunnel. Load
   a small territory table. Create the connection and foreign catalog.
2. Run `EXPLAIN FORMATTED` on the federated query and find the pushdown. If the
   predicate is not in the remote query, say so in the demo. Being able to spot a
   failed pushdown is a more useful skill than the setup itself.
3. P6: build `gold.order_fact_daily`. Note it joins P2's dimension, P3's events,
   and P5's federated reference data. Say that out loud in the demo — it is the
   thing that makes this a platform rather than six unrelated scripts.
4. Create the share and a recipient. Open sharing is fine for the PoC.
5. Run every query in `sql/observability.sql`. Fix anything that returns nothing.

---

## Session 5 — Bundle, tests, CI

**Goal:** it deploys from git, and it fails the build when the logic breaks.

1. `pip install pyspark pytest` locally and run `pytest tests/ -v`. All seven
   should pass before you touch the bundle.
2. Fill in the workspace hosts and warehouse ID in `databricks.yml`.
3. `databricks bundle validate`, then `deploy -t dev`, then `run -t dev`.
4. **Verify** the dev deployment prefixed your resources with your username.
   That prefix is what makes `mode: development` safe for a shared workspace.
5. Add a GitHub Actions workflow: pytest, then `bundle validate`, then
   `bundle deploy -t dev` on merge to main.
6. Break a transform on purpose and confirm CI catches it.

---

## Where it will go wrong

| Symptom | Usual cause |
|---|---|
| Auto Loader reprocesses everything | Checkpoint path changed, or the schema location moved |
| Stream restarts after a source change | Expected with `addNewColumns` — it restarts once, then continues |
| SCD2 history in the wrong order | `sequence_by` on ingest time instead of commit time |
| `MERGE` fails with multiple source rows | Source has duplicate business keys in one batch — dedupe first |
| Federated query is slow | Predicate not pushed down; check `EXPLAIN` |
| Streaming state grows without bound | Watermark too long, or absent |
| Bundle deploy overwrites a colleague's job | `mode: development` missing on the dev target |
| Costs higher than expected | All-purpose compute in a scheduled path, or a continuous trigger on a batch SLA |

---

## Scope discipline

If you are short on time, cut in this order: P5, then P3's Kafka path, then P4.
Do not cut the tests, the bundle, or the observability queries. Almost every PoC
demo has working pipelines; very few have a cost query, a quarantine table, and a
CI run. That contrast is the entire point of building this yourself.
