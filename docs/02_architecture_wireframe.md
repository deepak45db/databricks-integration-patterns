# Architecture Wireframes

Four views. In a client setting these are usually the artifact that gets circulated, so
they are deliberately kept at a level a delivery lead can read without a Databricks
background.

---

## 1. Context — the six patterns on one page

```mermaid
flowchart LR
  subgraph SRC[Source systems]
    S1[Supplier catalog<br/>file drops]
    S2[(Postgres<br/>customers)]
    S3{{Kafka<br/>shipment events}}
    S4[Carrier rates<br/>REST API]
    S5[(Postgres<br/>reference data)]
  end

  subgraph LH[Lakehouse - Unity Catalog]
    B[Bronze<br/>raw + rescued]
    SI[Silver<br/>conformed]
    G[Gold<br/>business model]
    Q[(Quarantine)]
    FC[[Foreign catalog<br/>no copy]]
  end

  subgraph CONS[Consumers]
    BI[Power BI]
    PART[Partner via<br/>Delta Sharing]
    DS[Analysts / SQL]
  end

  S1 -->|P1 Auto Loader| B
  S2 -->|P2 AUTO CDC| B
  S3 -->|P3 Structured Streaming| B
  S4 -->|P4 incremental pull| B
  S5 -.->|P5 federation| FC

  B --> SI
  B -.->|failed checks| Q
  SI --> G
  FC --> G
  G -->|P6| BI
  G -->|P6| PART
  G --> DS
```

---

## 2. Layer contract — what each layer promises

```mermaid
flowchart TD
  A[Landing<br/>UC Volume] --> B
  B[Bronze<br/>append-only, source-faithful<br/>+ file metadata, ingest_ts, _rescued_data]
  B --> C[Silver<br/>typed, deduplicated, conformed keys<br/>SCD2 where history matters]
  C --> D[Gold<br/>business grain, aggregated<br/>contract-stable column names]
  B -.-> Q[Quarantine<br/>row + reason + source file]
  C -.-> Q

  style B fill:#f5e6cc,stroke:#8a6d3b
  style C fill:#e6eef5,stroke:#31708f
  style D fill:#e2f0e2,stroke:#3c763d
  style Q fill:#f5e0e0,stroke:#a94442
```

Rule of thumb worth stating out loud in the review: bronze is never edited, silver is never
consumed directly by business users, gold never contains a column whose name would confuse
someone in finance.

---

## 3. Pattern P2 detail — CDC to SCD Type 2

The one pattern that always draws questions, so it gets its own diagram.

```mermaid
sequenceDiagram
  participant PG as Postgres
  participant CDC as CDC feed (files/connector)
  participant BR as bronze_customer_cdc
  participant AC as AUTO CDC flow
  participant SL as silver.customer_scd2

  PG->>CDC: INSERT / UPDATE / DELETE
  CDC->>BR: change events (key, op, commit_ts)
  Note over BR: expectations drop rows<br/>missing key or sequence
  BR->>AC: streaming read
  Note over AC: reorder by commit_ts<br/>handles late/out-of-order
  AC->>SL: close old row (__END_AT)<br/>open new row (__START_AT)
  Note over SL: DELETE closes the row,<br/>it does not remove history
```

---

## 4. Deployment and orchestration

```mermaid
flowchart LR
  subgraph REPO[Git repo]
    SRC2[src/ + sql/]
    RES[resources/*.yml]
    T[tests/]
  end

  subgraph CI[GitHub Actions]
    L[lint + pytest]
    V[bundle validate]
  end

  subgraph DEV[Workspace: dev target]
    DJ[Jobs + pipeline<br/>catalog: orderhub_dev]
  end

  subgraph PROD[Workspace: prod target]
    PJ[Jobs + pipeline<br/>catalog: orderhub_prod<br/>runs as service principal]
  end

  SRC2 --> L --> V
  T --> L
  RES --> V
  V -->|bundle deploy -t dev| DJ
  DJ -->|merge to main| PJ
```

Orchestration inside the workspace:

```mermaid
flowchart LR
  T1[task: p1_batch_files] --> T4[task: quality_gate]
  T2[task: p2_cdc_pipeline<br/>pipeline_task] --> T4
  T3[task: p4_api_pull] --> T4
  T4 --> T5[task: build_gold]
  T5 --> T6[task: refresh_share]
  P3[p3_streaming_job<br/>separate, continuous] -.->|independent lifecycle| T5
```

P3 is deliberately a separate job. Putting an always-on stream inside a scheduled batch job
is the single most common orchestration mistake in this kind of build — it either blocks the
job forever or gets killed on every run.
