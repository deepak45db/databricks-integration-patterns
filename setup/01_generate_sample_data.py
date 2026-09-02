"""
Setup: generate synthetic source data for every pattern.

PYSPARK          : DataFrame construction, JSON/CSV writes
DATABRICKS       : UC Volume paths, dbutils.fs

Run this as a job task before the first pattern run, and re-run it between demo
passes to produce a second batch (it appends new files with new timestamps, which
is what proves incremental ingestion actually works).

Usage:
    databricks bundle run -t dev orderhub_setup_job
"""

import json
import random
import uuid
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# --- parameters supplied by the job task -------------------------------------
import sys
sys.path.append("../src")
from common.config import get_param  # noqa: E402

CATALOG = get_param("catalog", "orderhub_dev")

VOLUME = f"/Volumes/{CATALOG}/landing/files"
BATCH_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

random.seed()  # deliberately not seeded: each run should differ

# =============================================================================
# P1 source: supplier catalog file drop (CSV)
# =============================================================================
CATEGORIES = ["fasteners", "abrasives", "adhesives", "safety", "electrical"]


def supplier_catalog_rows(n=500, bad_row_rate=0.04):
    """PYSPARK-agnostic: plain Python row generation."""
    rows = []
    for i in range(n):
        sku = f"SKU-{random.randint(10000, 99999)}"
        bad = random.random() < bad_row_rate
        rows.append(
            {
                # a bad row has a null SKU - P1 must quarantine, not crash
                "sku": None if bad else sku,
                "supplier_id": f"SUP-{random.randint(100, 140)}",
                "description": f"{random.choice(CATEGORIES)} item {i}",
                "category": random.choice(CATEGORIES),
                # occasionally a non-numeric price, to exercise _rescued_data
                "unit_price": "N/A" if bad else round(random.uniform(1.5, 480.0), 2),
                "uom": random.choice(["EA", "BX", "CS"]),
                "effective_date": (
                    datetime.now(timezone.utc) - timedelta(days=random.randint(0, 30))
                ).date().isoformat(),
            }
        )
    return rows


catalog_df = spark.createDataFrame(supplier_catalog_rows())
(
    catalog_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{VOLUME}/supplier_catalog/batch={BATCH_ID}")
)
print(f"P1: wrote supplier catalog batch {BATCH_ID}")

# =============================================================================
# P2 source: customer CDC change events (JSON)
#   op: I / U / D, with a commit sequence. Deliberately emits out-of-order
#   events so AUTO CDC's SEQUENCE BY has something to do.
# =============================================================================
TIERS = ["bronze", "silver", "gold", "platinum"]
REGIONS = ["northeast", "southeast", "midwest", "west"]


def customer_cdc_events(n_customers=120):
    events = []
    base = datetime.now(timezone.utc) - timedelta(hours=6)
    for c in range(n_customers):
        cid = 1000 + c
        seq = 0
        # initial insert
        events.append(
            {
                "customer_id": cid,
                "op": "I",
                "commit_ts": (base + timedelta(seconds=seq)).isoformat(),
                "name": f"Customer {cid}",
                "email": f"customer{cid}@example.com",
                "tier": random.choice(TIERS),
                "region": random.choice(REGIONS),
            }
        )
        # some customers change tier, a few get deleted
        for _ in range(random.randint(0, 3)):
            seq += random.randint(60, 900)
            events.append(
                {
                    "customer_id": cid,
                    "op": "U",
                    "commit_ts": (base + timedelta(seconds=seq)).isoformat(),
                    "name": f"Customer {cid}",
                    "email": f"customer{cid}@example.com",
                    "tier": random.choice(TIERS),
                    "region": random.choice(REGIONS),
                }
            )
        if random.random() < 0.05:
            seq += 1200
            events.append(
                {
                    "customer_id": cid,
                    "op": "D",
                    "commit_ts": (base + timedelta(seconds=seq)).isoformat(),
                    "name": None,
                    "email": None,
                    "tier": None,
                    "region": None,
                }
            )
    random.shuffle(events)  # out-of-order on purpose
    return events


cdc_path = f"{VOLUME}/customer_cdc/batch={BATCH_ID}"
dbutils.fs.mkdirs(cdc_path)  # noqa: F821  DATABRICKS
payload = "\n".join(json.dumps(e) for e in customer_cdc_events())
dbutils.fs.put(f"{cdc_path}/events.json", payload, overwrite=True)  # noqa: F821
print(f"P2: wrote customer CDC batch {BATCH_ID}")

# =============================================================================
# P3 source: shipment events
#   Written as files so the streaming pattern can run without Kafka. The Kafka
#   reader in p3 is the primary path; this is the fallback for a laptop demo.
# =============================================================================
STATUSES = ["created", "picked", "in_transit", "out_for_delivery", "delivered"]


def shipment_events(n=2000, dup_rate=0.03, late_rate=0.02):
    now = datetime.now(timezone.utc)
    events = []
    for _ in range(n):
        eid = str(uuid.uuid4())
        ts = now - timedelta(seconds=random.randint(0, 3600))
        if random.random() < late_rate:
            ts = now - timedelta(hours=random.randint(25, 72))  # beyond watermark
        ev = {
            "event_id": eid,
            "shipment_id": f"SHP-{random.randint(1, 400):05d}",
            "order_id": f"ORD-{random.randint(1, 900):06d}",
            # carries the P2 dimension key so gold can join the two patterns
            "customer_id": 1000 + random.randint(0, 119),
            "status": random.choice(STATUSES),
            "carrier": random.choice(["UPS", "FDX", "USPS", "XPO"]),
            "event_ts": ts.isoformat(),
        }
        events.append(ev)
        if random.random() < dup_rate:
            events.append(dict(ev))  # exact duplicate, same event_id
    return events


ship_path = f"{VOLUME}/shipment_events/batch={BATCH_ID}"
dbutils.fs.mkdirs(ship_path)  # noqa: F821
dbutils.fs.put(  # noqa: F821
    f"{ship_path}/events.json",
    "\n".join(json.dumps(e) for e in shipment_events()),
    overwrite=True,
)
print(f"P3: wrote shipment events batch {BATCH_ID}")

# =============================================================================
# P4 source: seed the watermark control table
# =============================================================================
spark.sql(
    f"""
    MERGE INTO {CATALOG}.ops.ingest_watermark AS t
    USING (SELECT 'carrier_rates' AS source_name) AS s
      ON t.source_name = s.source_name
    WHEN NOT MATCHED THEN INSERT (source_name, watermark_ts, updated_at, updated_by)
      VALUES ('carrier_rates', current_timestamp() - INTERVAL 7 DAYS,
              current_timestamp(), current_user())
    """
)
print("P4: watermark seeded")

print(f"\nAll sample data written under {VOLUME} for batch {BATCH_ID}")
