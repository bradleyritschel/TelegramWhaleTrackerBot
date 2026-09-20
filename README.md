# Polymarket Whale Tracker — Data Pipeline

An analytics layer over an existing Telegram alerting bot. The bot polls the
Polymarket trades API every 30 seconds for ~19 tracked wallets and alerts on
trades above a $1,000 notional threshold. It never kept any of it.

This adds persistence and query: every trade the API returns lands in S3,
gets cleaned into Parquet, and becomes queryable in Athena.

```
Polymarket API
      │  poll every 30s (existing bot)
      ▼
  ┌────────────────┬─────────────────┐
  │  Telegram      │  S3 raw         │   alerting is unchanged;
  │  alert         │  (NDJSON.gz)    │   persistence is a second path
  └────────────────┴─────────────────┘
                          │  dt=YYYY-MM-DD, event time
                          ▼
                   transform (daily)
                   dedupe · clean · Parquet
                          │  staging → verify → swap
                          ▼
                   S3 curated
                          │
                   Glue Catalog
                          │
                 ┌────────┴────────┐
                 ▼                 ▼
             Athena          gold aggregates
             (ad-hoc)        (CTAS, daily)
```

## Why the layers

**Raw** is append-only and unfiltered. The alert path drops sub-threshold
trades and deduplicates; raw deliberately does neither. If the threshold
changes, or a dedupe bug turns up, the history is intact and everything can be
rebuilt. Filtering at ingest destroys data you can't get back.

**Curated** is the clean, deduplicated, typed version analysts query. It is
always derivable from raw, which is what makes it safe to rebuild.

**Gold** is materialized aggregates — daily per-wallet rollups via Athena
CTAS. Not Spark: at this volume SQL is the right tool and adding Spark would
be architecture cosplay.

## Design decisions

**Partition by event time, not processing time.** The partition key comes from
the trade's own timestamp. A trade the API surfaces late lands in the day it
actually happened, so rebuilding that day picks it up correctly. Partitioning
by ingest time would scatter one day's trades across whichever days we
happened to see them.

**Idempotent whole-partition overwrite.** The transform never appends. It
rebuilds a date partition from scratch: read all raw for that date, dedupe,
write one Parquet file. Running it once or five times produces byte-identical
output — there's a test asserting exactly that. This is what makes retry-after-
failure safe and backfill a JSON payload rather than a manual operation.

**Staging, verify, swap.** S3 has no atomic directory rename. So: write to
`_staging/`, read it back and check the row count, copy into the live prefix,
then delete the old objects. A reader sees the old partition or the new one,
never a half-deleted one. A corrupt write fails at the verify step without
taking out a good partition.

**Dedupe on a composite key.** `transactionHash` is not unique — one order
matched against several makers returns multiple rows sharing a hash. The key
is hash + timestamp + side + asset + outcome + size + price, mirroring the
bot's own `trade_key()`. Keying on the hash alone would silently swallow every
fill after the first, which is a bug the bot itself had at one point.

**Buffered writes.** At a 30-second poll, one object per trade would mean tens
of thousands of tiny files a day. Small files are the classic S3/Athena cost
sink — every object costs a request and a seek. The sink buffers and flushes on
size (500 records) or age (5 minutes); the transform compacts further into one
Parquet file per day.

**Schema tolerance.** The transform selects named fields. Anything new
upstream is preserved as JSON in a `raw_extra` column rather than dropped or
allowed to break the job. A new field is visible in the warehouse the day it
appears, and promoting it to a real column later is a DDL change plus a
backfill — both of which this pipeline supports.

**Split IAM identities.** The bot can `PutObject` to the raw prefix and
nothing else — no read, no delete, no access to curated. If the bot host is
compromised, an attacker can append junk to raw; they cannot read history or
destroy it. The transform can read raw and rebuild curated but can never write
to raw. That asymmetry is what makes raw a trustworthy rebuild source.

## Failure modes and how they're handled

| Failure | Handling |
|---|---|
| S3 unreachable during a poll | Buffer spills to local disk, uploaded on next startup |
| Transform dies halfway | Nothing was swapped; rerun rebuilds the partition |
| Corrupt Parquet written | Verify step catches it before the live swap |
| Trade arrives days late | Lands in its event-time partition; rerun that date |
| Upstream adds a field | Captured in `raw_extra`; job doesn't break |
| Duplicate trades in raw | Expected — the 150-trade window re-returns them; transform dedupes |
| Bad JSON line in a raw object | Skipped and logged; the partition still processes |

## Layout

```
pipeline/
├── sink.py                  buffered S3 raw writer, imported by bot.py
├── transform.py             raw → curated, idempotent, CLI + backfill
├── lambda_handler.py        Lambda entry point for the daily run
├── bot_changes.md           the four edits to bot.py
├── athena/
│   ├── ddl.sql              external tables, partition projection, gold CTAS
│   └── queries.sql          analytical queries + a data quality check
├── infra/
│   └── setup.md             bucket, KMS, lifecycle, IAM, scheduling, cost
└── tests/
    └── test_pipeline.py     dedupe, idempotency, schema drift, late arrival
```

## Running it

Locally, no AWS needed:

```bash
python tests/test_pipeline.py
python transform.py --date 2026-09-09 --local ./sample_data
```

Against S3:

```bash
export S3_RAW_BUCKET=your-bucket
export AWS_REGION=us-east-1

python transform.py --date 2026-09-09
python transform.py --backfill 2026-09-01 2026-09-09
```

## What's deliberately not here

**Streaming.** Nineteen wallets on a 30-second poll is not a streaming
problem. Kafka or Kinesis here would be infrastructure without a reason.

**Spark.** A day's data is megabytes. Glue Spark would cost more in startup
time than the job takes to run. If volume grew 1000x, the transform's shape —
read a partition, dedupe, write a partition — ports to Spark almost directly.

**Airflow.** One daily job with no dependencies. EventBridge is enough.
Airflow earns its keep when there are dependencies between stages and backfill
across a DAG; there aren't yet.

Each of these is a step the pipeline could take when there's a reason. Taking
them now would mean carrying operational cost for capability nobody needs.

## Next

- Ingest lag as a CloudWatch metric with an alarm, rather than a query nobody
  runs
- Market resolution outcomes joined in, so whale accuracy becomes measurable —
  the actual interesting question this dataset could answer
- Compaction job for raw once file count per partition gets large
