-- Glue Catalog / Athena definitions for the Polymarket trade pipeline.
-- Run these once in the Athena console (or via boto3) against the workgroup
-- whose result location you've configured.

CREATE DATABASE IF NOT EXISTS polymarket;

-- ============================================================
-- Curated trades
-- ============================================================
-- External table over the Parquet the transform job writes. Athena reads
-- schema from here, not from the files, so adding a column is a DDL change
-- rather than a rewrite. Columns are declared in file order; Parquet is read
-- by name when parquet.column.index.access is false (the default), which is
-- what makes an added column non-breaking.

CREATE EXTERNAL TABLE IF NOT EXISTS polymarket.trades (
  trade_key            string,
  transaction_hash     string,
  event_ts             bigint,
  event_time           timestamp,
  address              string,
  label                string,
  side                 string,
  outcome              string,
  asset                string,
  title                string,
  event_slug           string,
  size                 double,
  price                double,
  notional             double,
  observed_at          bigint,
  ingest_lag_seconds   bigint,
  raw_extra            string
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://REPLACE_WITH_YOUR_BUCKET/polymarket/curated/trades/'
TBLPROPERTIES (
  'parquet.compression' = 'SNAPPY',
  -- Partition projection: Athena derives the partition list from this range
  -- instead of reading partition metadata. No MSCK REPAIR, no per-partition
  -- registration, and pruning still works. Worth knowing by name.
  'projection.enabled'       = 'true',
  'projection.dt.type'       = 'date',
  'projection.dt.range'      = '2026-09-01,NOW',
  'projection.dt.format'     = 'yyyy-MM-dd',
  'projection.dt.interval'   = '1',
  'projection.dt.interval.unit' = 'DAYS',
  'storage.location.template' =
     's3://REPLACE_WITH_YOUR_BUCKET/polymarket/curated/trades/dt=${dt}'
);

-- ============================================================
-- Raw trades (optional)
-- ============================================================
-- Queryable raw layer for debugging: "what did the API actually return on
-- the 9th?" Deliberately NOT the table analysts use — it has duplicates.

CREATE EXTERNAL TABLE IF NOT EXISTS polymarket.trades_raw (
  transactionHash  string,
  timestamp        bigint,
  side             string,
  outcome          string,
  asset            string,
  title            string,
  eventSlug        string,
  size             string,
  price            string,
  _address         string,
  _label           string,
  _observed_at     bigint
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION 's3://REPLACE_WITH_YOUR_BUCKET/polymarket/raw/trades/'
TBLPROPERTIES (
  'projection.enabled'       = 'true',
  'projection.dt.type'       = 'date',
  'projection.dt.range'      = '2026-09-01,NOW',
  'projection.dt.format'     = 'yyyy-MM-dd',
  'projection.dt.interval'   = '1',
  'projection.dt.interval.unit' = 'DAYS',
  'storage.location.template' =
     's3://REPLACE_WITH_YOUR_BUCKET/polymarket/raw/trades/dt=${dt}'
);

-- ============================================================
-- Gold: daily aggregate per whale
-- ============================================================
-- CTAS materializes an aggregate to S3 as Parquet. This is the "aggregated"
-- layer without needing Spark — for rollups this size, SQL is the right tool.
-- Drop and recreate to rebuild; that's the same whole-partition-overwrite
-- idempotency as the transform job, expressed in DDL.

-- DROP TABLE IF EXISTS polymarket.whale_daily;
CREATE TABLE IF NOT EXISTS polymarket.whale_daily
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  partitioned_by = ARRAY['dt'],
  external_location = 's3://REPLACE_WITH_YOUR_BUCKET/polymarket/gold/whale_daily/'
) AS
SELECT
  address,
  label,
  count(*)                                      AS trade_count,
  sum(notional)                                 AS total_notional,
  sum(CASE WHEN side = 'BUY'  THEN notional ELSE 0 END) AS buy_notional,
  sum(CASE WHEN side = 'SELL' THEN notional ELSE 0 END) AS sell_notional,
  avg(notional)                                 AS avg_notional,
  max(notional)                                 AS largest_trade,
  count(DISTINCT event_slug)                    AS markets_touched,
  dt
FROM polymarket.trades
GROUP BY address, label, dt;
