# Demo Script — 15 minutes

Written for the conversation where someone asks what you built. The structure
matters more than the content: lead with the catalog, not the code. Anyone can
show a pipeline running. Very few candidates open with the decision framework.

---

## 0:00–2:00 — The framing

> "The patterns were given. What was missing was the layer underneath them — what
> each pattern requires from the source, what happens when it breaks, and what it
> costs. So I wrote the spec for six patterns to the same template, then built all
> six to prove the specs were implementable."

Show `docs/01_pattern_catalog.md`, one entry, and point at the **Don't use when**
row. That row is what stops a team from defaulting to streaming for everything.

## 2:00–4:00 — The wireframe

Show the context diagram. Six sources, three layers, three consumer types. Then
the layer-contract diagram. Say the rule out loud: bronze is never edited, silver
is never consumed directly, gold has no column that would confuse someone in
finance.

## 4:00–7:00 — P2 live

The CDC pipeline is the one worth showing running.

1. Pipeline graph — bronze, streaming table, materialized view.
2. The SCD2 query for one customer whose tier changed twice. Point at `__END_AT`.
3. The delete case: history retained, current-state view excludes them.
4. The event log expectation counts.

> "The source emits out of order on purpose. AUTO CDC reorders by commit
> timestamp, which is why this works without any merge logic. If the source can't
> give me a reliable sequence, this pattern isn't viable at all and I'd fall back
> to snapshot-based CDC. That's in the spec."

## 7:00–9:00 — Break something

The best two minutes of the demo. Pick one:

- **Bad file.** Drop a file with a null key. Re-run P1. Show the row in
  `quarantine.supplier_catalog_bad` with its failure reason and its source file.
  > "Nothing is dropped silently. Every rejected row is diagnosable back to its file."
- **Replay.** Set the P4 watermark back a day, re-run, show silver counts
  unchanged.
  > "Re-running is safe. That's what makes 3am recovery boring."

## 9:00–11:00 — Cost and governance

Run the cost-by-pattern query from `sql/observability.sql`.

> "Every job and pipeline carries a `pattern` tag, so spend attributes per pattern
> rather than to one line item called 'Databricks'. Streaming is the expensive one
> and I can show exactly how much."

Then the quarantine-trend query.

> "A rising line here means the source changed and nobody told us. That's the
> signal I'd want a client alerting on."

Mention: pipelines run as a service principal in prod, grants go to account-level
groups, the share exposes a view rather than the base table.

## 11:00–13:00 — Deployment

Show `databricks.yml` and the two targets, then a CI run.

> "The same code deploys to dev and prod with the catalog as a bundle variable.
> Prod runs as a service principal. Tests run before the deploy — the transforms
> are in a module with no Databricks imports, so they're testable on a laptop."

## 13:00–15:00 — What you'd do differently at scale

Never end on "and it all works."

- Six patterns as separate jobs doesn't scale past about twenty sources; the next
  step is metadata-driven ingestion where a config table drives the pattern.
- Lakeflow Connect's managed connectors would replace the hand-built P4 for any
  source it supports.
- Quality checks here are hand-written; Lakehouse Monitoring would handle drift
  detection for real volumes.
- P5's federation cost lands on the source database, which needs a real
  conversation with that team before it goes anywhere near production.

---

## Questions to expect

**"Why not one pattern for everything?"**
Because source constraints differ. Half the specification work is source
contracts, and a source that can't emit a sequence column can't do CDC no matter
what the architecture slide says.

**"How do you handle schema changes?"**
Differently per layer. Bronze rescues and continues, silver has explicit
expectations, gold breaks the build. Severity should rise as data gets closer to
a decision.

**"What's this cost?"**
Point at the query rather than guessing. Then: the levers are trigger mode,
serverless vs. job compute, and whether streaming is genuinely required. Most
overspend is a batch SLA running on a continuous trigger.

**"How long did this take?"**
Answer honestly. Fifteen hours of build sounds better than a vague "a while," and
it tells them what a comparable client PoC would cost.

**"What would you need from us?"**
Source contracts, a named owner per source, and the network path. Not access —
access is the easy part. The specification stalls without those three.
