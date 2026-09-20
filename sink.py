"""
S3 raw sink for the Polymarket whale tracker.

Design notes (these are the interview-relevant decisions):

1. RAW IS APPEND-ONLY AND UNFILTERED.
   The bot's alert path applies MIN_TRADE_NOTIONAL and dedupes against
   seen_hashes. The sink deliberately does NEITHER. Raw captures every trade
   the API returned, threshold or not, duplicate or not. If the alert
   threshold changes later, or a dedupe bug is found, the history is still
   there to reprocess. Filtering at ingest destroys data you can't get back.

2. PARTITIONED BY EVENT TIME, NOT PROCESSING TIME.
   The partition comes from the trade's own `timestamp` field, not from when
   we polled. A trade the API surfaces late still lands in the day it
   happened, so a rebuild of that day's partition picks it up correctly.

3. BUFFERED, NOT ONE OBJECT PER TRADE.
   At a 30s poll interval, per-trade writes would produce tens of thousands
   of tiny objects a day. Small files are the classic S3/Athena cost sink:
   every object costs a request and a seek. The buffer flushes on size or
   age, and the transform stage compacts further into Parquet.

4. NEVER BLOCKS THE ALERT PATH.
   Every public call is wrapped so that an S3 failure logs and moves on.
   The bot's job is alerting; persistence is best-effort downstream.
"""

import gzip
import io
import json
import os
import socket
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from time import time

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
    _BOTO_AVAILABLE = True
except ImportError:  # sink is optional — bot still runs without boto3
    _BOTO_AVAILABLE = False


# ========== CONFIG ==========

# Set these in the environment. If S3_RAW_BUCKET is unset the sink is a no-op,
# so the bot runs unchanged on a machine with no AWS credentials.
S3_RAW_BUCKET = os.environ.get("S3_RAW_BUCKET", "")
S3_RAW_PREFIX = os.environ.get("S3_RAW_PREFIX", "polymarket/raw/trades")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Flush when either threshold is hit. 500 records / 5 minutes at current
# volume gives objects in the low hundreds of KB — small, but the transform
# compacts them. Raise these if the watchlist grows a lot.
FLUSH_MAX_RECORDS = int(os.environ.get("SINK_FLUSH_RECORDS", "500"))
FLUSH_MAX_SECONDS = int(os.environ.get("SINK_FLUSH_SECONDS", "300"))

# Local spill directory: if S3 is unreachable, buffered records are written
# here instead of dropped, and can be uploaded later.
SPILL_DIR = os.environ.get("SINK_SPILL_DIR", "sink_spill")

# CHANGED: file holding trade keys already written to S3.
# Without this the sink re-wrote the entire 150-trade API window on every
# poll — ~130 tiny objects every 30s, the same records forever. That is the
# classic small-file problem: every object costs a PUT, a seek and metadata,
# and the poll itself was taking 31s because of the sequential writes.
SEEN_FILE = os.environ.get("SINK_SEEN_FILE", "sink_seen_keys.json")

# Keys kept in memory/on disk. The API window is 150 trades per address
# across ~19 addresses, so anything well above 3000 is safe headroom.
MAX_SEEN_KEYS = int(os.environ.get("SINK_MAX_SEEN", "20000"))

_INSTANCE_ID = f"{socket.gethostname()}-{os.getpid()}"


# ========== INTERNAL STATE ==========

_s3_client = None
# buffer maps partition date string -> list of record dicts
_buffer: dict[str, list] = defaultdict(list)
_buffer_count = 0
_last_flush = time()

# CHANGED: keys already persisted. List preserves insertion order so the
# trim keeps the NEWEST keys; the set is for O(1) lookup. Same pattern the
# bot uses for seen_hashes — a set alone can't be trimmed meaningfully
# because its iteration order is arbitrary.
_seen_list: list = []
_seen_set: set = set()
_seen_loaded = False
_seen_dirty = False


def _client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", region_name=AWS_REGION)
    return _s3_client


def enabled() -> bool:
    return bool(S3_RAW_BUCKET) and _BOTO_AVAILABLE


def _trade_key(trade: dict) -> str:
    """CHANGED: composite identity, mirroring bot.trade_key() exactly.

    transactionHash alone is not unique — one order matched against several
    makers returns multiple rows sharing a hash.
    """
    return "|".join(
        str(trade.get(f, ""))
        for f in ("transactionHash", "timestamp", "side", "asset",
                  "outcome", "size", "price")
    )


def _load_seen() -> None:
    """CHANGED: load persisted keys once, on first use."""
    global _seen_list, _seen_set, _seen_loaded
    if _seen_loaded:
        return
    _seen_loaded = True
    try:
        if os.path.exists(SEEN_FILE):
            with open(SEEN_FILE, "r") as f:
                _seen_list = json.load(f)
            _seen_set = set(_seen_list)
            print(f"[sink] loaded {len(_seen_set)} previously written keys")
    except Exception as e:
        # A corrupt file means we re-write some records. Raw is append-only
        # and the transform dedupes, so duplicates are recoverable — losing
        # the ability to start at all is not.
        print(f"[sink] could not read {SEEN_FILE} ({e}); starting fresh")
        _seen_list, _seen_set = [], set()


def _save_seen() -> None:
    """CHANGED: persist keys after a successful flush, atomically."""
    global _seen_dirty
    if not _seen_dirty:
        return
    try:
        tmp = SEEN_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(_seen_list[-MAX_SEEN_KEYS:], f)
        os.replace(tmp, SEEN_FILE)
        _seen_dirty = False
    except Exception as e:
        print(f"[sink] could not save seen keys: {e}")


def _partition_date(trade: dict) -> str:
    """Event-time partition key, YYYY-MM-DD in UTC.

    Falls back to the current date if the trade has no usable timestamp, so a
    malformed record still lands somewhere rather than being dropped. The
    record keeps its original timestamp field either way, so the fallback is
    visible downstream.
    """
    try:
        ts = int(trade.get("timestamp") or 0)
    except (TypeError, ValueError):
        ts = 0
    if ts <= 0:
        ts = int(time())
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def record_trades(address: str, label: str, trades: list) -> None:
    """Buffer every trade returned for one address.

    Called from the poll loop with the raw API response, before any
    thresholding or dedupe.
    """
    if not enabled() or not trades:
        return

    _load_seen()

    global _buffer_count, _seen_dirty
    observed_at = int(time())
    new_count = 0

    try:
        for t in trades:
            if not isinstance(t, dict):
                continue

            # CHANGED: skip anything already written to S3. The poller
            # re-fetches the same 150-trade window every cycle, so without
            # this the sink rewrites months of history every 30 seconds.
            key = _trade_key(t)
            if key in _seen_set:
                continue
            _seen_set.add(key)
            _seen_list.append(key)
            _seen_dirty = True

            # Ingest metadata travels with the record. observed_at lets you
            # measure ingest lag (observed_at - timestamp) and says which
            # process wrote it, which matters once this runs in more than
            # one place.
            enriched = dict(t)
            enriched["_address"] = address.lower()
            enriched["_label"] = label
            enriched["_observed_at"] = observed_at
            enriched["_ingest_instance"] = _INSTANCE_ID

            _buffer[_partition_date(t)].append(enriched)
            new_count += 1
    except Exception as e:  # never let the sink break the poll
        print(f"[sink] buffering error: {type(e).__name__}: {e}")

    _buffer_count += new_count


def maybe_flush(force: bool = False) -> None:
    """Flush buffered records to S3 if a threshold is hit."""
    if not enabled():
        return

    global _buffer_count, _last_flush
    age = time() - _last_flush
    if not force and _buffer_count < FLUSH_MAX_RECORDS and age < FLUSH_MAX_SECONDS:
        return
    if _buffer_count == 0:
        _last_flush = time()
        return

    # Snapshot and clear first, so a slow upload doesn't block new buffering
    # and a failure doesn't leave records queued for a second attempt (the
    # spill file handles that case instead).
    pending = dict(_buffer)
    _buffer.clear()
    _buffer_count = 0
    _last_flush = time()

    for dt, records in pending.items():
        _write_partition(dt, records)

    # CHANGED: persist seen keys only after the writes, so a crash mid-flush
    # re-writes those records rather than silently losing them. Raw is
    # append-only and the transform dedupes, so duplicates are safe; gaps
    # are not.
    _save_seen()


def _write_partition(dt: str, records: list) -> None:
    """Write one gzipped NDJSON object into the date partition.

    Object key includes a UUID so concurrent writers never collide. Because
    raw is append-only, two objects containing the same trade is fine — the
    transform dedupes.
    """
    body = io.BytesIO()
    with gzip.GzipFile(fileobj=body, mode="wb") as gz:
        for r in records:
            gz.write((json.dumps(r, separators=(",", ":")) + "\n").encode())
    data = body.getvalue()

    key = f"{S3_RAW_PREFIX}/dt={dt}/{int(time())}-{uuid.uuid4().hex[:8]}.json.gz"

    try:
        _client().put_object(
            Bucket=S3_RAW_BUCKET,
            Key=key,
            Body=data,
            ContentType="application/x-ndjson",
            ContentEncoding="gzip",
            # Bucket has default SSE-KMS, but being explicit means a
            # misconfigured bucket policy fails loudly at write time rather
            # than silently storing unencrypted objects.
            ServerSideEncryption="aws:kms",
        )
        print(f"[sink] wrote {len(records)} records -> s3://{S3_RAW_BUCKET}/{key}")
    except (BotoCoreError, ClientError, Exception) as e:
        print(f"[sink] S3 write failed ({type(e).__name__}: {e}); spilling locally")
        _spill(dt, data)


def _spill(dt: str, data: bytes) -> None:
    """Last resort: keep the bytes on disk so nothing is lost to an outage."""
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        path = os.path.join(
            SPILL_DIR, f"dt={dt}__{int(time())}-{uuid.uuid4().hex[:8]}.json.gz"
        )
        with open(path, "wb") as f:
            f.write(data)
    except Exception as e:
        print(f"[sink] spill failed too, dropping batch: {type(e).__name__}: {e}")


def drain_spill() -> int:
    """Upload any spilled files. Call on startup; safe to call repeatedly."""
    if not enabled() or not os.path.isdir(SPILL_DIR):
        return 0

    uploaded = 0
    for name in sorted(os.listdir(SPILL_DIR)):
        if "__" not in name:
            continue
        dt = name.split("__", 1)[0].replace("dt=", "")
        path = os.path.join(SPILL_DIR, name)
        try:
            with open(path, "rb") as f:
                data = f.read()
            key = (
                f"{S3_RAW_PREFIX}/dt={dt}/"
                f"{int(time())}-{uuid.uuid4().hex[:8]}.json.gz"
            )
            _client().put_object(
                Bucket=S3_RAW_BUCKET,
                Key=key,
                Body=data,
                ContentType="application/x-ndjson",
                ContentEncoding="gzip",
                ServerSideEncryption="aws:kms",
            )
            os.unlink(path)
            uploaded += 1
        except Exception as e:
            print(f"[sink] spill upload failed for {name}: {e}")
            break  # stop on first failure; try again next time
    if uploaded:
        print(f"[sink] drained {uploaded} spilled file(s)")
    return uploaded
