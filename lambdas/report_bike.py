import datetime
import json
from io import BytesIO

import boto3
import pandas as pd
import pyarrow.parquet as pq

S3 = boto3.client("s3")

import os
BUCKET = os.environ.get("BUCKET", "cityflow-raw0")
GOLD_PREFIX = os.environ.get("GOLD_PREFIX", "gold/")
REPORTS_PREFIX = os.environ.get("REPORTS_PREFIX", "reports/")

def lambda_handler(event, context):
    day = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    prefix = f"{GOLD_PREFIX}date={day}/"
    print(f"[REPORT] day={day} prefix={prefix}")

    resp = S3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    keys = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]
    if not keys:
        print("[REPORT] No gold data")
        return {"ok": True, "empty": True}

    dfs = []
    for k in keys:
        body = S3.get_object(Bucket=BUCKET, Key=k)["Body"].read()
        dfs.append(pq.read_table(BytesIO(body)).to_pandas())
    data = pd.concat(dfs, ignore_index=True)

    top10 = data.sort_values("total_counts", ascending=False).head(10)
    congestion = data.sort_values("avg_counts", ascending=False).head(10)

    base = f"{REPORTS_PREFIX}{day}/"
    S3.put_object(Bucket=BUCKET, Key=base + "top10.csv", Body=top10.to_csv(index=False))
    S3.put_object(Bucket=BUCKET, Key=base + "congestion.csv", Body=congestion.to_csv(index=False))
    S3.put_object(
        Bucket=BUCKET,
        Key=base + "summary.json",
        Body=json.dumps({
            "day": day,
            "top10": top10.to_dict(orient="records"),
            "congestion": congestion.to_dict(orient="records")
        }, indent=2, ensure_ascii=False).encode("utf-8")
    )
    print(f"[REPORT] Wrote s3://{BUCKET}/{base}(top10.csv|congestion.csv|summary.json)")
    return {"ok": True}
