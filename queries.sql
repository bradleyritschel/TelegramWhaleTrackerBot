-- Analytical queries against the curated layer.
--
-- Every query filters on dt first. That's not stylistic: Athena bills per
-- byte scanned, and the dt predicate is what lets it skip partitions
-- entirely. Dropping the filter on a year of data is the difference between
-- scanning one day and scanning 365.

-- ============================================================
-- 1. Biggest whales by volume, last 7 days
-- ============================================================
SELECT
  label,
  address,
  count(*)                    AS trades,
  round(sum(notional), 0)     AS total_notional,
  round(avg(notional), 0)     AS avg_trade,
  round(max(notional), 0)     AS biggest_trade
FROM polymarket.trades
WHERE dt >= date_format(current_date - interval '7' day, '%Y-%m-%d')
GROUP BY label, address
ORDER BY total_notional DESC;


-- ============================================================
-- 2. Where is the smart money concentrated?
-- ============================================================
-- Markets ranked by how many distinct tracked wallets touched them. More
-- interesting than raw volume: one whale betting big is noise, six whales
-- converging on the same market is a signal.
SELECT
  title,
  event_slug,
  count(DISTINCT address)     AS distinct_whales,
  count(*)                    AS trades,
  round(sum(notional), 0)     AS total_notional
FROM polymarket.trades
WHERE dt >= date_format(current_date - interval '30' day, '%Y-%m-%d')
GROUP BY title, event_slug
HAVING count(DISTINCT address) >= 2
ORDER BY distinct_whales DESC, total_notional DESC
LIMIT 25;


-- ============================================================
-- 3. Directional agreement per market
-- ============================================================
-- Are the whales on the same side, or taking each other's action?
SELECT
  title,
  outcome,
  sum(CASE WHEN side = 'BUY'  THEN notional ELSE 0 END) AS buy_notional,
  sum(CASE WHEN side = 'SELL' THEN notional ELSE 0 END) AS sell_notional,
  round(
    sum(CASE WHEN side = 'BUY' THEN notional ELSE -notional END), 0
  ) AS net_flow
FROM polymarket.trades
WHERE dt >= date_format(current_date - interval '14' day, '%Y-%m-%d')
GROUP BY title, outcome
HAVING sum(notional) > 10000
ORDER BY abs(sum(CASE WHEN side = 'BUY' THEN notional ELSE -notional END)) DESC
LIMIT 25;


-- ============================================================
-- 4. Average entry price per whale per market
-- ============================================================
-- Size-weighted, not a plain average of prices — a $50k fill and a $50 fill
-- should not count equally. This is the kind of thing that's trivial in SQL
-- and annoying in the bot's Python.
SELECT
  label,
  title,
  outcome,
  side,
  round(sum(size * price) / nullif(sum(size), 0), 4) AS vwap,
  round(sum(size), 0)                               AS total_size,
  round(sum(notional), 0)                           AS total_notional
FROM polymarket.trades
WHERE dt >= date_format(current_date - interval '30' day, '%Y-%m-%d')
GROUP BY label, title, outcome, side
HAVING sum(notional) > 5000
ORDER BY total_notional DESC
LIMIT 50;


-- ============================================================
-- 5. Activity by hour of day (UTC)
-- ============================================================
SELECT
  hour(event_time)            AS utc_hour,
  count(*)                    AS trades,
  round(sum(notional), 0)     AS notional
FROM polymarket.trades
WHERE dt >= date_format(current_date - interval '30' day, '%Y-%m-%d')
GROUP BY hour(event_time)
ORDER BY utc_hour;


-- ============================================================
-- 6. Pipeline health: ingest lag
-- ============================================================
-- How long between a trade happening and the poller seeing it. A rising p95
-- means the poll interval is too slow or the API is lagging. This is the
-- query that tells you the pipeline is degrading before anyone complains.
SELECT
  dt,
  count(*)                                       AS trades,
  round(avg(ingest_lag_seconds), 1)              AS avg_lag_s,
  approx_percentile(ingest_lag_seconds, 0.50)    AS p50_lag_s,
  approx_percentile(ingest_lag_seconds, 0.95)    AS p95_lag_s,
  max(ingest_lag_seconds)                        AS max_lag_s
FROM polymarket.trades
WHERE dt >= date_format(current_date - interval '14' day, '%Y-%m-%d')
GROUP BY dt
ORDER BY dt DESC;


-- ============================================================
-- 7. Alert coverage
-- ============================================================
-- What fraction of captured volume actually triggered a Telegram alert at the
-- $1000 threshold? Answers "is my threshold set anywhere near right?" — and
-- it's only answerable because raw captures sub-threshold trades too.
SELECT
  dt,
  count(*)                                                  AS all_trades,
  count(CASE WHEN notional >= 1000 THEN 1 END)              AS alerted_trades,
  round(
    100.0 * count(CASE WHEN notional >= 1000 THEN 1 END) / count(*), 1
  )                                                         AS pct_alerted,
  round(
    100.0 * sum(CASE WHEN notional >= 1000 THEN notional ELSE 0 END)
    / nullif(sum(notional), 0), 1
  )                                                         AS pct_notional_alerted
FROM polymarket.trades
WHERE dt >= date_format(current_date - interval '14' day, '%Y-%m-%d')
GROUP BY dt
ORDER BY dt DESC;


-- ============================================================
-- 8. Data quality check — run after every transform
-- ============================================================
-- Zero rows returned means the partition is clean. Any row is a defect to
-- investigate. Cheap to run, and it's the difference between a pipeline you
-- trust and one you hope about.
SELECT 'duplicate trade_key' AS issue, trade_key AS detail, count(*) AS n
FROM polymarket.trades
WHERE dt = '2026-09-09'
GROUP BY trade_key HAVING count(*) > 1

UNION ALL
SELECT 'null or zero price', transaction_hash, 1
FROM polymarket.trades
WHERE dt = '2026-09-09' AND (price IS NULL OR price <= 0)

UNION ALL
SELECT 'event_time outside partition', transaction_hash, 1
FROM polymarket.trades
WHERE dt = '2026-09-09'
  AND (event_time < timestamp '2026-09-09 00:00:00'
       OR event_time >= timestamp '2026-09-10 00:00:00')

UNION ALL
SELECT 'negative ingest lag', transaction_hash, 1
FROM polymarket.trades
WHERE dt = '2026-09-09' AND ingest_lag_seconds < 0;
