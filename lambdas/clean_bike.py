import json
import os
from io import BytesIO

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

S3 = boto3.client("s3")
LMB = boto3.client("lambda")

BUCKET = os.environ.get("BUCKET", "cityflow-raw0")
AGG_FN = os.environ.get("AGGREGATE_FUNCTION_NAME", "cityflow-aggregate")
SILVER_PREFIX = os.environ.get("SILVER_PREFIX", "silver/")

REQUIRED_COLS = {"Date", "Counts", "Location_Name"}

def _split_coords(s: str):
    if pd.isna(s) or not str(s).strip():
        return None, None
    parts = str(s).split(",")
    if len(parts) != 2:
        return None, None
    try:
        return float(parts[0]), float(parts[1])
    except Exception:
        return None, None

def lambda_handler(event, context):
    # ---- 1) Get S3 object from event ----
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]
    print(f"[CLEAN] Input: s3://{bucket}/{key}")

    # ---- 2) Read CSV ----
    obj = S3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj["Body"])

    # ---- 3) Validate & clean ----
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df["Counts"] = pd.to_numeric(df["Counts"], errors="coerce")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)

    df = df.dropna(subset=["Counts", "Date", "Location_Name"])

    if "Coordinates" in df.columns:
        latlon = df["Coordinates"].apply(_split_coords)
        df["Latitude"] = [xy[0] for xy in latlon]
        df["Longitude"] = [xy[1] for xy in latlon]

    # prune optional/noisy cols if present
    for col in ["isodate", "Status", "counter", "Coordinates"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    df["day"] = df["Date"].dt.strftime("%Y-%m-%d")

    # ---- 4) Write Silver (Parquet partitioned by day) ----
    day = df["day"].iloc[0]
    silver_key = f"{SILVER_PREFIX}date={day}/clean.parquet"  # unique by day (overwrite ok)
    buf = BytesIO()
    pq.write_table(pa.Table.from_pandas(df), buf)
    S3.put_object(Bucket=BUCKET, Key=silver_key, Body=buf.getvalue())
    print(f"[CLEAN] Wrote: s3://{BUCKET}/{silver_key}")

    # ---- 5) Trigger aggregate (async) ----
    payload = {"silver_key": silver_key, "day": day}
    LMB.invoke(
        FunctionName=AGG_FN,
        InvocationType="Event",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    print(f"[CLEAN] Invoked {AGG_FN} for day={day}")

    return {"ok": True, "silver_key": silver_key, "day": day}
