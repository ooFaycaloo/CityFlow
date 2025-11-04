import os
from io import BytesIO
from decimal import Decimal

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

S3 = boto3.client("s3")
DDB = boto3.resource("dynamodb")

BUCKET = os.environ.get("BUCKET", "cityflow-raw0")
GOLD_PREFIX = os.environ.get("GOLD_PREFIX", "gold/")
DDB_TABLE = os.environ.get("DDB_TABLE", "TrafficAggregated")

def lambda_handler(event, context):
    silver_key = event.get("silver_key")
    day = event.get("day")
    if not silver_key or not day:
        raise ValueError("silver_key and day are required")

    print(f"[AGG] Input: s3://{BUCKET}/{silver_key} (day={day})")

    obj = S3.get_object(Bucket=BUCKET, Key=silver_key)
    df = pq.read_table(BytesIO(obj["Body"].read())).to_pandas()

    grp = (
        df.groupby(["Location_Name", "day"], as_index=False)
          .agg(total_counts=("Counts", "sum"),
               avg_counts=("Counts", "mean"))
    )

    # ---- Write Gold ----
    gold_key = f"{GOLD_PREFIX}date={day}/aggregated.parquet"
    buf = BytesIO()
    pq.write_table(pa.Table.from_pandas(grp), buf)
    S3.put_object(Bucket=BUCKET, Key=gold_key, Body=buf.getvalue())
    print(f"[AGG] Wrote: s3://{BUCKET}/{gold_key}")

    # ---- Upsert DynamoDB ----
    table = DDB.Table(DDB_TABLE)
    with table.batch_writer(overwrite_by_pkeys=["Location_Name", "Date"]) as batch:
        for _, r in grp.iterrows():
            batch.put_item(Item={
                "Location_Name": str(r["Location_Name"]),
                "Date": str(r["day"]),
                "total_counts": Decimal(str(r["total_counts"])),
                "avg_counts": Decimal(str(r["avg_counts"])),
            })
    print(f"[AGG] Upserted {len(grp)} items into {DDB_TABLE}")

    return {"ok": True, "gold_key": gold_key, "rows": len(grp)}
