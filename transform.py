"""
Raw -> curated transform for the Polymarket trade pipeline.

Reads every raw NDJSON object for one date partition, cleans and dedupes,
writes a single Parquet file to the curated layer, then swaps it into place.

The three design decisions worth defending in an interview:

1. IDEMPOTENT WHOLE-PARTITION OVERWRITE.
   The job never appends. It rebuilds a date partition from scratch, writes
   to a staging prefix, then swaps. Run it once or five times and the output
   is identical. That means a retry after a half-written failure is safe, and
   there is no partial state to reason about.

2. STAGING THEN ATOMIC SWAP.
   S3 has no atomic directory rename, so "atomic" here means: write the new
   object under _staging/, verify it, copy it into the live prefix, then
   delete the old objects. A reader either sees the previous partition or the
   new one, never a half-deleted one.

3. DEDUPE ON A COMPOSITE KEY.
   transactionHash alone is not unique — one order matched against several
   makers returns multiple rows sharing a hash. The key mirrors the bot's
   trade_key(): hash + timestamp + side + asset + outcome + size + price.

Schema tolerance: the reader selects named columns and tolerates new ones
appearing upstream (they're carried into a raw_extra JSON column rather than
breaking the job or being silently dropped).

Usage:
    python transform.py --date 2026-09-09
    python transform.py --date 2026-09-09 --local ./sample_raw   # no AWS
    python transform.py --backfill 2026-09-01 2026-09-09
"""

import argparse
import gzip
import io
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

try:
    import boto3
    _BOTO_AVAILABLE = True
except ImportError:
    _BOTO_AVAILABLE = False


S3_BUCKET = os.environ.get("S3_RAW_BUCKET", "")
RAW_PREFIX = os.environ.get("S3_RAW_PREFIX", "polymarket/raw/trades")
CURATED_PREFIX = os.environ.get("S3_CURATED_PREFIX", "polymarket/curated/trades")
STAGING_PREFIX = os.environ.get("S3_STAGING_PREFIX", "polymarket/_staging/trades")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
KMS_KEY_ID = os.environ.get("S3_KMS_KEY_ID", "")

# Columns we promise downstream consumers. Anything else upstream sends is
# preserved in raw_extra rather than dropped or allowed to break the schema.
CURATED_COLUMNS = [
    "trade_key",
    "transaction_hash",
    "event_ts",
    "event_time",
    "address",
    "label",
    "side",
    "outcome",
    "asset",
    "title",
    "event_slug",
    "size",
    "price",
    "notional",
    "observed_at",
    "ingest_lag_seconds",
    "raw_extra",
]

# Fields consumed into named columns; everything else goes to raw_extra.
_MAPPED_SOURCE_FIELDS = {
    "transactionHash", "timestamp", "side", "outcome", "asset", "title",
    "eventSlug", "size", "price", "_address", "_label", "_observed_at",
    "_ingest_instance",
}


def _s3():
    return boto3.client("s3", region_name=AWS_REGION)


# ---------- reading ----------

def read_raw_local(local_dir: str, dt: str) -> list:
    """Read a local raw partition. Used for tests and for running with no AWS."""
    part_dir = os.path.join(local_dir, f"dt={dt}")
    if not os.path.isdir(part_dir):
        return []
    records = []
    for name in sorted(os.listdir(part_dir)):
        path = os.path.join(part_dir, name)
        opener = gzip.open if name.endswith(".gz") else open
        with opener(path, "rt") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return records


def read_raw_s3(dt: str) -> list:
    """Read every object in one raw date partition."""
    client = _s3()
    prefix = f"{RAW_PREFIX}/dt={dt}/"
    records = []

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            body = client.get_object(Bucket=S3_BUCKET, Key=obj["Key"])["Body"].read()
            if obj["Key"].endswith(".gz"):
                body = gzip.decompress(body)
            for line in body.decode().splitlines():
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        # One bad line shouldn't fail the partition. In a real
                        # pipeline this would go to a quarantine prefix.
                        print(f"  skipping malformed line in {obj['Key']}")
    return records


# ---------- transform ----------

def _trade_key(r: dict) -> str:
    """Composite identity. Mirrors bot.trade_key() exactly."""
    return "|".join(
        str(r.get(f, ""))
        for f in ("transactionHash", "timestamp", "side", "asset", "outcome",
                  "size", "price")
    )


def _to_float(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _to_int(v) -> int:
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


def transform(records: list, dt: str) -> pd.DataFrame:
    """Clean, dedupe, and shape one partition's records."""
    if not records:
        return pd.DataFrame(columns=CURATED_COLUMNS)

    rows = []
    for r in records:
        ts = _to_int(r.get("timestamp"))
        observed = _to_int(r.get("_observed_at"))
        size = _to_float(r.get("size"))
        price = _to_float(r.get("price"))

        # Anything upstream added that we don't have a column for. Keeping it
        # as JSON means a new field is visible in the warehouse the day it
        # appears, without a schema migration and without breaking this job.
        extra = {k: v for k, v in r.items() if k not in _MAPPED_SOURCE_FIELDS}

        rows.append({
            "trade_key": _trade_key(r),
            "transaction_hash": r.get("transactionHash") or "",
            "event_ts": ts,
            "event_time": (
                datetime.fromtimestamp(ts, tz=timezone.utc) if ts > 0 else pd.NaT
            ),
            "address": (r.get("_address") or "").lower(),
            "label": r.get("_label") or "",
            "side": r.get("side") or "",
            "outcome": r.get("outcome") or "",
            "asset": str(r.get("asset") or ""),
            "title": r.get("title") or "",
            "event_slug": r.get("eventSlug") or "",
            "size": size,
            "price": price,
            "notional": size * price,
            "observed_at": observed,
            "ingest_lag_seconds": (observed - ts) if (observed and ts) else 0,
            "raw_extra": json.dumps(extra, separators=(",", ":")) if extra else "",
        })

    df = pd.DataFrame(rows, columns=CURATED_COLUMNS)

    # Drop rows with no usable identity — nothing downstream can join on them.
    df = df[df["transaction_hash"] != ""]

    # Dedupe. Raw is append-only and the poller re-fetches a 150-trade window
    # every cycle, so the same trade appears in many raw objects. Keep the
    # first observation of each key so ingest_lag reflects earliest sighting.
    df = df.sort_values("observed_at").drop_duplicates(
        subset=["trade_key"], keep="first"
    )

    # Guard against a record whose event time doesn't match the partition it
    # was filed under. Shouldn't happen (the sink partitions on event time),
    # but if it does we want to know rather than silently mis-file it.
    if not df.empty:
        expected = pd.Timestamp(dt, tz="UTC")
        mismatched = df[
            (df["event_time"].notna())
            & ((df["event_time"] < expected)
               | (df["event_time"] >= expected + pd.Timedelta(days=1)))
        ]
        if not mismatched.empty:
            print(f"  WARNING: {len(mismatched)} record(s) outside partition {dt}")

    return df.sort_values("event_ts").reset_index(drop=True)


# ---------- writing ----------

def _to_parquet_bytes(df: pd.DataFrame) -> bytes:
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def write_curated_local(df: pd.DataFrame, local_dir: str, dt: str) -> str:
    part_dir = os.path.join(local_dir, f"dt={dt}")
    os.makedirs(part_dir, exist_ok=True)
    # Whole-partition overwrite: clear it first, so a rerun after a schema
    # change doesn't leave stale files behind.
    for name in os.listdir(part_dir):
        os.unlink(os.path.join(part_dir, name))
    path = os.path.join(part_dir, "part-0000.snappy.parquet")
    with open(path, "wb") as f:
        f.write(_to_parquet_bytes(df))
    return path


def write_curated_s3(df: pd.DataFrame, dt: str) -> str:
    """Staging write, verify, swap, then delete the old partition objects."""
    client = _s3()
    data = _to_parquet_bytes(df)

    staging_key = f"{STAGING_PREFIX}/dt={dt}/part-0000.snappy.parquet"
    live_key = f"{CURATED_PREFIX}/dt={dt}/part-0000.snappy.parquet"

    extra = {"ServerSideEncryption": "aws:kms"}
    if KMS_KEY_ID:
        extra["SSEKMSKeyId"] = KMS_KEY_ID

    # 1. write to staging
    client.put_object(Bucket=S3_BUCKET, Key=staging_key, Body=data, **extra)

    # 2. verify it reads back and has the row count we expect, before we touch
    #    anything live. A corrupt write should never take out a good partition.
    check = client.get_object(Bucket=S3_BUCKET, Key=staging_key)["Body"].read()
    verified = pq.read_table(io.BytesIO(check))
    if verified.num_rows != len(df):
        raise RuntimeError(
            f"staging verify failed: wrote {len(df)} rows, read {verified.num_rows}"
        )

    # 3. list what's currently live so we can clean up after the swap
    old_keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=S3_BUCKET, Prefix=f"{CURATED_PREFIX}/dt={dt}/"
    ):
        old_keys.extend(o["Key"] for o in page.get("Contents", []))

    # 4. swap in the new object
    client.copy_object(
        Bucket=S3_BUCKET,
        Key=live_key,
        CopySource={"Bucket": S3_BUCKET, "Key": staging_key},
        **extra,
    )

    # 5. remove old objects that aren't the one we just wrote
    stale = [k for k in old_keys if k != live_key]
    if stale:
        client.delete_objects(
            Bucket=S3_BUCKET,
            Delete={"Objects": [{"Key": k} for k in stale]},
        )

    client.delete_object(Bucket=S3_BUCKET, Key=staging_key)
    return f"s3://{S3_BUCKET}/{live_key}"


def repair_partition(dt: str) -> None:
    """Register the partition with Glue so Athena can see it."""
    if not _BOTO_AVAILABLE:
        return
    athena_db = os.environ.get("ATHENA_DATABASE", "polymarket")
    try:
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.get_partition(
            DatabaseName=athena_db, TableName="trades", PartitionValues=[dt]
        )
    except Exception:
        # Partition doesn't exist yet — create it. Cheaper and more precise
        # than MSCK REPAIR TABLE, which rescans every prefix.
        try:
            glue = boto3.client("glue", region_name=AWS_REGION)
            table = glue.get_table(DatabaseName=athena_db, Name="trades")["Table"]
            sd = dict(table["StorageDescriptor"])
            sd["Location"] = f"s3://{S3_BUCKET}/{CURATED_PREFIX}/dt={dt}/"
            glue.create_partition(
                DatabaseName=athena_db,
                TableName="trades",
                PartitionInput={"Values": [dt], "StorageDescriptor": sd},
            )
            print(f"  registered partition dt={dt}")
        except Exception as e:
            print(f"  partition registration skipped: {type(e).__name__}: {e}")


# ---------- entry point ----------

def run_one(dt: str, local: str | None = None) -> int:
    print(f"processing dt={dt}")

    if local:
        records = read_raw_local(os.path.join(local, "raw"), dt)
    else:
        records = read_raw_s3(dt)
    print(f"  read {len(records)} raw record(s)")

    df = transform(records, dt)
    print(f"  {len(df)} unique trade(s) after dedupe")

    if df.empty:
        print("  nothing to write")
        return 0

    if local:
        out = write_curated_local(df, os.path.join(local, "curated"), dt)
    else:
        out = write_curated_s3(df, dt)
        repair_partition(dt)

    print(f"  wrote {out}")
    print(f"  notional total ${df['notional'].sum():,.0f}")
    return len(df)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", help="partition date, YYYY-MM-DD")
    p.add_argument("--backfill", nargs=2, metavar=("START", "END"),
                   help="inclusive date range to reprocess")
    p.add_argument("--local", help="run against a local directory instead of S3")
    args = p.parse_args()

    if args.backfill:
        start = date.fromisoformat(args.backfill[0])
        end = date.fromisoformat(args.backfill[1])
        total = 0
        d = start
        while d <= end:
            total += run_one(d.isoformat(), args.local)
            d += timedelta(days=1)
        print(f"backfill complete: {total} row(s)")
    elif args.date:
        run_one(args.date, args.local)
    else:
        # Default: yesterday, the normal scheduled run.
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        run_one(yesterday.isoformat(), args.local)


if __name__ == "__main__":
    main()
