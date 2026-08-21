import asyncio  # CHANGED: needed for concurrent fetches + semaphore
import json
import os
import tempfile  # CHANGED: for atomic state writes
from datetime import datetime, timezone
from time import time

import httpx
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)
from telegram.request import HTTPXRequest
from telegram.error import TelegramError, TimedOut, NetworkError

# ========== CONFIG ==========

# CHANGED: token pulled from the environment instead of hardcoded.
# The old literal token is compromised the moment this file is shared —
# revoke it in BotFather (/revoke) and export the new one:
#   export TELEGRAM_BOT_TOKEN="123456:ABC..."
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")

# Users/chats that receive alerts
TELEGRAM_CHAT_IDS = {882534235, 1390191964}

# CHANGED: 20s was very tight for 19 sequential fetches. Fetches are now
# concurrent (see poll_polymarket), so 20 is survivable, but 30 gives headroom
# and keeps you well clear of Polymarket's rate limiting.
POLL_INTERVAL = 30

STATE_FILE = "polymarket_state.json"
ADDRESSES_FILE = "tracked_addresses.json"

# Minimum notional (size * price) in USDC for alerts
MIN_TRADE_NOTIONAL = 1000

POLY_TRADES_URL = "https://data-api.polymarket.com/trades"

# CHANGED: how many trade keys to remember per address. Your fetch window is
# 150 trades, so anything well above that is safe. 2000 costs ~150KB total.
MAX_SEEN_KEYS = 2000

# CHANGED: bumped when the *format* of a seen-key changes. On an upgrade we
# re-baseline instead of treating every historical trade as new.
STATE_KEY_VERSION = 2

# CHANGED: max simultaneous requests to the Polymarket API.
HTTP_CONCURRENCY = 5

# CHANGED: retry count for transient fetch failures (429 / 5xx / timeouts).
FETCH_RETRIES = 2

# CHANGED: group fills that share a transactionHash into one alert, and apply
# MIN_TRADE_NOTIONAL to the *combined* size. Set to False to restore the old
# per-fill behavior.
AGGREGATE_FILLS_BY_TX = True

# Default addresses used if tracked_addresses.json does not exist yet
DEFAULT_TRACKED_ADDRESSES = {
    "0x26437896ed9dfeb2f69765edcafe8fdceaab39ae": "Latina",
    "0x743510ee9f21e24071c4e28edab4653df44ea620": "Comeback",
    "0x91654fd592ea5339fc0b1b2f2b30bfffa5e75b98": "CSIN",
    "0x42592084120b0d5287059919d2a96b3b7acb936f": "Antman-Batman-Superman",
    "0x3657862e57070b82a289b5887ec943a7c2166b14": "Mayuvarama",
    "0xb1d9476e5a5ba938b57cf0a5dc7a91a114605ee1": "Pringles",
    "0x9b3dcd99eec7fe11602e6534e6302c0f318d7422": "NHL",
    "0x82a1b239e7e0ff25a2ac12a20b59fd6b5f90e03a": "Darkrider",
    "0x74bac8116b2762f38e07ab43644d519d9aaceba1": "sollunix",
    "0xb744f56635b537e859152d14b022af5afe485210": "Wasian",
    "0xee3b2f1ace24ef413add5c13484d7a5042528dcf": "Dumbass",
    "0xafbacaeeda63f31202759eff7f8126e49adfe61b": "Sammy",
    "0xe20a1538293903b746ffe6c4ce2d5c3c0300e469": "GoPats",
    "0x6a72f61820b26b1fe4d956e17b6dc2a1ea3033ee": "KCH",
    "0x2c57db9e442ef5ffb2651f03afd551171738c94d": "ZerOptimist",
    "0xaefeb1f121eeccc9b3a2d2283139cc93a08a1e4a": "SavingGrace",
    "0xed72c58176a997ca7f128c1f885b6e48b936b8b0": "Pingotito",
    "0xbddf61af533ff524d27154e589d2d7a81510c684": "Countryside",
    "0x01408711a6255b5a967b6fddc9b83d25fca19028": "28z",
}

# ========== TRACKED ADDRESSES STORAGE ==========


def _atomic_write_json(path, payload):
    """CHANGED: write via temp file + os.replace.

    Your old save_state() truncated the real file before writing. A crash or
    Ctrl-C mid-write left a zero-byte/partial JSON file, which meant the next
    boot saw no state at all and re-baselined every address.
    """
    directory = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp_path = tempfile.mkstemp(dir=directory, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def load_tracked_addresses():
    if not os.path.exists(ADDRESSES_FILE):
        # Write defaults on first run so you have a file to edit if you want
        _atomic_write_json(ADDRESSES_FILE, DEFAULT_TRACKED_ADDRESSES)  # CHANGED
        return DEFAULT_TRACKED_ADDRESSES.copy()
    with open(ADDRESSES_FILE, "r") as f:
        raw = json.load(f)
    # CHANGED: normalize keys to lowercase. /add lowercases the address but a
    # hand-edited JSON file might not, and then the STATE key never matches.
    return {k.lower(): v for k, v in raw.items()}


def save_tracked_addresses(tracked):
    _atomic_write_json(ADDRESSES_FILE, tracked)  # CHANGED


TRACKED_ADDRESSES = load_tracked_addresses()

# ========== STATE (last_ts + seen keys) ==========


def load_state_raw():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        # CHANGED: a corrupt state file used to crash the bot at import time.
        print(f"State file unreadable ({e}); starting fresh.")
        return {}


def normalize_state(raw):
    """
    Normalize older formats to the current one:
    - v0: STATE[address] = <timestamp>
    - v1: STATE[address] = {"last_ts": int, "seen_hashes": [tx_hash, ...]}
    - v2: STATE[address] = {"last_ts": int, "seen_hashes": [trade_key, ...],
                            "key_version": 2}

    CHANGED: seen_hashes now holds composite trade keys, not bare tx hashes
    (see trade_key). Upgrading in place would make every historical trade look
    new, so on a version bump we clear the keys AND push last_ts to now — the
    next poll baselines everything currently visible without alerting.
    """
    normalized = {}
    now_ts = int(time())

    for addr, val in raw.items():
        addr = addr.lower()  # CHANGED: match the lowercasing above

        if isinstance(val, dict) and "last_ts" in val:
            last_ts = int(val.get("last_ts") or 0)
            seen = val.get("seen_hashes") or []
            if not isinstance(seen, list):
                seen = []
            version = int(val.get("key_version") or 1)

            if version < STATE_KEY_VERSION:
                # CHANGED: re-baseline on upgrade rather than re-alert.
                seen = []
                last_ts = now_ts
        else:
            # v0 format: just a timestamp
            try:
                last_ts = int(val)
            except (TypeError, ValueError):
                last_ts = 0
            seen = []

        normalized[addr] = {
            "last_ts": last_ts,
            "seen_hashes": seen[-MAX_SEEN_KEYS:],
            "key_version": STATE_KEY_VERSION,
        }

    return normalized


def save_state(state):
    _atomic_write_json(STATE_FILE, state)  # CHANGED


STATE = normalize_state(load_state_raw())

# On very first run (no state at all), start "now" for all known addresses so
# you don't get spammed
if not STATE:
    _now_ts = int(time())
    for addr in TRACKED_ADDRESSES:
        STATE[addr] = {
            "last_ts": _now_ts,
            "seen_hashes": [],
            "key_version": STATE_KEY_VERSION,
        }
    save_state(STATE)

# ========== BASIC COMMANDS ==========


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Polymarket watcher bot online.\n"
        "Commands:\n"
        "/list - list tracked addresses\n"
        "/add 0xAddress Label - start tracking\n"
        "/remove 0xAddress - stop tracking\n"
        "/status - poll health"  # CHANGED: added, see status() below
    )

# ========== POLYMARKET FETCH / FORMAT ==========

# CHANGED: one shared client for the process. The old code built a new
# httpx.AsyncClient (and a fresh TCP+TLS handshake) for every address on every
# poll — 19 handshakes every 20 seconds for no reason.
_http_client: httpx.AsyncClient | None = None


async def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=5.0),
            limits=httpx.Limits(max_connections=HTTP_CONCURRENCY * 2),
            headers={"User-Agent": "polymarket-watcher/1.0"},
        )
    return _http_client


async def fetch_trades_for_user(client: httpx.AsyncClient, address: str):
    """CHANGED: takes a shared client, and retries transient failures.

    A single 429 or 502 used to skip that address for the whole poll cycle.
    """
    params = {"user": address, "limit": 150, "takerOnly": "false"}
    last_err = None

    for attempt in range(FETCH_RETRIES + 1):
        try:
            resp = await client.get(POLY_TRADES_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            # CHANGED: defend against the API returning an error object
            # instead of a list — this used to blow up in sorted().
            if not isinstance(data, list):
                raise ValueError(f"expected list, got {type(data).__name__}")
            return data
        except (httpx.HTTPError, ValueError) as e:
            last_err = e
            status = getattr(getattr(e, "response", None), "status_code", None)
            # Don't retry genuine client errors other than rate limiting
            if status is not None and 400 <= status < 500 and status != 429:
                break
            if attempt < FETCH_RETRIES:
                await asyncio.sleep(1.5 * (attempt + 1))

    raise last_err if last_err else RuntimeError("fetch failed")


def trade_key(trade: dict) -> str:
    """CHANGED: composite identity instead of bare transactionHash.

    One Polymarket order matched against several makers returns multiple rows
    sharing a single transactionHash. Keying on the hash alone meant every fill
    after the first was silently swallowed. This keys on the fields that
    actually distinguish a fill.
    """
    return "|".join(
        str(trade.get(field, ""))
        for field in (
            "transactionHash",
            "timestamp",
            "side",
            "asset",
            "outcome",
            "size",
            "price",
        )
    )


def group_key(trade: dict):
    """CHANGED: fills belonging to the same logical order."""
    return (
        trade.get("transactionHash"),
        trade.get("side"),
        trade.get("outcome"),
        trade.get("title"),
    )


def group_fills(trades: list) -> list:
    """CHANGED: collapse fills into logical orders, preserving arrival order."""
    if not AGGREGATE_FILLS_BY_TX:
        return [[t] for t in trades]

    groups = {}
    for t in trades:
        groups.setdefault(group_key(t), []).append(t)
    return list(groups.values())


def group_notional(fills: list) -> float:
    """CHANGED: notional across the whole order, not one fill.

    This is the important one: with MIN_TRADE_NOTIONAL = 1000, a $5,000 order
    that filled as ten $500 chunks previously failed the per-fill check and
    never alerted at all.
    """
    total = 0.0
    for f in fills:
        try:
            total += float(f.get("size") or 0) * float(f.get("price") or 0)
        except (TypeError, ValueError):
            continue
    return total


def format_trade_message(address: str, label: str, fills: list) -> str:
    """CHANGED: now takes a list of fills rather than a single trade."""
    head = fills[0]
    side = head.get("side")
    title = head.get("title")
    outcome = head.get("outcome")
    slug = head.get("eventSlug")
    tx = head.get("transactionHash")

    total_size = 0.0
    for f in fills:
        try:
            total_size += float(f.get("size") or 0)
        except (TypeError, ValueError):
            pass

    notional = group_notional(fills)
    avg_price = (notional / total_size) if total_size else 0.0

    # CHANGED: guard against a missing/garbage timestamp. The old
    # datetime.fromtimestamp(None) raised TypeError, which escaped
    # poll_polymarket and killed that entire poll cycle mid-loop.
    ts_values = []
    for f in fills:
        try:
            ts_values.append(int(f.get("timestamp") or 0))
        except (TypeError, ValueError):
            pass
    ts = max(ts_values) if ts_values else 0

    if ts > 0:
        time_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    else:
        time_str = "unknown"

    market_link = f"https://polymarket.com/event/{slug}" if slug else "N/A"
    tx_link = f"https://polygonscan.com/tx/{tx}" if tx else "N/A"

    fill_note = f" ({len(fills)} fills)" if len(fills) > 1 else ""

    # CHANGED: both the title line and the URL line used to be labelled
    # "Market:", which read as a duplicate. Second one is "Link:" now.
    return (
        f"📈 New trade by {label} ({address})\n"
        f"{side} {total_size:,.0f} @ {avg_price:.4f} "
        f"(~${notional:,.0f}){fill_note}\n"
        f"Market: {title} ({outcome})\n"
        f"Time: {time_str}\n"
        f"Link: {market_link}\n"
        f"Tx: {tx_link}"
    )

# ========== ADDRESS MANAGEMENT COMMANDS ==========


def is_valid_address(addr: str) -> bool:
    addr = addr.lower()
    return (
        addr.startswith("0x")
        and len(addr) == 42
        and all(c in "0123456789abcdef" for c in addr[2:])
    )


async def list_addresses(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not TRACKED_ADDRESSES:
        await update.message.reply_text("No addresses are being tracked yet.")
        return

    lines = [f"- {label}: {addr}" for addr, label in TRACKED_ADDRESSES.items()]
    msg = "Currently tracked addresses:\n" + "\n".join(lines)

    # CHANGED: dropped parse_mode="Markdown". There's no markup in this text,
    # and a label containing _ * ` or [ makes Telegram reject the whole message
    # with a parse error. Same removal in add/remove below.
    await update.message.reply_text(msg)


async def add_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # usage: /add 0x... Label words...
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /add 0xAddress Label")
        return

    addr = context.args[0].lower()
    label = " ".join(context.args[1:]).strip()

    if not is_valid_address(addr):
        await update.message.reply_text("That doesn't look like a valid 0x address.")
        return

    TRACKED_ADDRESSES[addr] = label
    save_tracked_addresses(TRACKED_ADDRESSES)

    # initialize last-seen timestamp so you don't get spammed with old trades
    STATE[addr] = {
        "last_ts": int(time()),
        "seen_hashes": [],
        "key_version": STATE_KEY_VERSION,
    }
    save_state(STATE)

    await update.message.reply_text(f"Now tracking {label}: {addr}")  # CHANGED


async def remove_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # usage: /remove 0x...
    if len(context.args) < 1:
        await update.message.reply_text("Usage: /remove 0xAddress")
        return

    addr = context.args[0].lower()

    if addr not in TRACKED_ADDRESSES:
        await update.message.reply_text("That address is not currently being tracked.")
        return

    label = TRACKED_ADDRESSES.pop(addr)
    save_tracked_addresses(TRACKED_ADDRESSES)

    # clean up state too
    if addr in STATE:
        STATE.pop(addr)
        save_state(STATE)

    await update.message.reply_text(f"Stopped tracking {label}: {addr}")  # CHANGED


# CHANGED: new command. Silent failure was the hard part of debugging this —
# if polls are erroring or being skipped you had no visibility into it.
LAST_POLL = {"finished_at": 0, "duration": 0.0, "errors": [], "alerts": 0}


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not LAST_POLL["finished_at"]:
        await update.message.reply_text("No poll has completed yet.")
        return

    age = int(time()) - LAST_POLL["finished_at"]
    errs = LAST_POLL["errors"]
    lines = [
        f"Tracking {len(TRACKED_ADDRESSES)} addresses.",
        f"Last poll: {age}s ago, took {LAST_POLL['duration']:.1f}s.",
        f"Alerts sent last poll: {LAST_POLL['alerts']}",
        f"Fetch errors last poll: {len(errs)}",
    ]
    if errs:
        lines.append("Recent errors:")
        lines.extend(f"  {e}" for e in errs[:5])
    await update.message.reply_text("\n".join(lines))

# ========== SAFE SEND WRAPPER ==========


async def safe_send(bot, chat_id, text, **kwargs):
    """Send a Telegram message without letting timeouts kill the job."""
    try:
        await bot.send_message(chat_id=chat_id, text=text, **kwargs)
        return True
    except (TimedOut, NetworkError) as e:
        print(f"Telegram timeout/network error: {e} - retrying once")
        try:
            await bot.send_message(chat_id=chat_id, text=text, **kwargs)
            return True
        except TelegramError as e2:
            print(f"Failed again, giving up on this message: {e2}")
    except TelegramError as e:
        print(f"Telegram error, skipping this message: {e}")
    return False

# ========== POLLING JOB ==========


async def poll_polymarket(context: ContextTypes.DEFAULT_TYPE):
    """
    Poll Polymarket and send alerts for trades we haven't seen before.

    CHANGED, in order of how much they mattered:

    1. Fetches run concurrently. 19 addresses x up to 10s sequentially could
       exceed POLL_INTERVAL, and APScheduler's default max_instances=1 then
       *drops* the overlapping run. That's a silent missed poll.
    2. seen_hashes is an ordered list, not a set. The old
       `list(seen_hashes)[-1000:]` sliced a set, whose iteration order is
       arbitrary — so it kept a random 1000 keys, not the newest 1000. Keys
       still inside the 150-trade API window could get evicted and re-alert.
    3. Identity is a composite key (see trade_key), so multi-maker fills aren't
       collapsed away.
    4. Alerts are grouped per order and thresholded on combined notional.
    """
    started = time()
    errors = []
    alerts_sent = 0

    client = await get_http_client()
    address_items = list(TRACKED_ADDRESSES.items())
    semaphore = asyncio.Semaphore(HTTP_CONCURRENCY)

    async def fetch_one(addr):
        async with semaphore:
            try:
                return addr, await fetch_trades_for_user(client, addr), None
            except Exception as e:
                return addr, None, f"{addr[:10]}...: {type(e).__name__}: {e}"

    results = await asyncio.gather(*(fetch_one(a) for a, _ in address_items))

    for address, trades, err in results:
        if err:
            print(f"Error fetching trades for {address}: {err}")
            errors.append(err)
            continue

        label = TRACKED_ADDRESSES.get(address, address)

        # Oldest -> newest
        trades_sorted = sorted(trades, key=lambda t: int(t.get("timestamp") or 0))

        state_entry = STATE.get(address)
        if not state_entry:
            # First time seeing this address at all: baseline history, no alerts
            baseline = [trade_key(t) for t in trades_sorted if t.get("transactionHash")]
            STATE[address] = {
                "last_ts": int(time()),
                "seen_hashes": baseline[-MAX_SEEN_KEYS:],
                "key_version": STATE_KEY_VERSION,
            }
            continue

        last_ts = int(state_entry.get("last_ts") or 0)
        # CHANGED: keep the list (ordered) alongside the set (fast lookup).
        seen_list = list(state_entry.get("seen_hashes") or [])
        seen_set = set(seen_list)

        # "Cold start" = address was added/initialized with last_ts set, but we
        # haven't recorded any keys yet. Ignore everything at or before last_ts.
        cold_start = (not seen_set and last_ts > 0)

        new_trades = []

        for t in trades_sorted:
            tx = t.get("transactionHash")
            if not tx:
                continue

            key = trade_key(t)
            ts = int(t.get("timestamp") or 0)

            # Cold start: mark historical trades as seen, don't alert
            if cold_start and ts <= last_ts:
                if key not in seen_set:
                    seen_set.add(key)
                    seen_list.append(key)
                continue

            if key in seen_set:
                continue

            new_trades.append(t)
            # CHANGED: record before sending. If a send fails we'd rather drop
            # one alert than loop on it every 30 seconds.
            seen_set.add(key)
            seen_list.append(key)

        if new_trades:
            last_ts = max(
                last_ts,
                max(int(t.get("timestamp") or 0) for t in new_trades),
            )

        STATE[address] = {
            "last_ts": last_ts,
            "seen_hashes": seen_list[-MAX_SEEN_KEYS:],  # CHANGED: ordered trim
            "key_version": STATE_KEY_VERSION,
        }

        # CHANGED: persist per address. The old code only saved after the full
        # loop, so an exception partway through lost every earlier address's
        # progress and re-alerted them on the next run.
        save_state(STATE)

        # Send alerts, one per logical order
        for fills in group_fills(new_trades):
            if group_notional(fills) < MIN_TRADE_NOTIONAL:
                continue
            msg = format_trade_message(address, label, fills)
            for chat_id in TELEGRAM_CHAT_IDS:
                if await safe_send(context.bot, chat_id, msg):
                    alerts_sent += 1

    save_state(STATE)

    LAST_POLL.update(
        finished_at=int(time()),
        duration=time() - started,
        errors=errors,
        alerts=alerts_sent,
    )

    # CHANGED: warn when a cycle is at risk of overrunning the interval.
    if LAST_POLL["duration"] > POLL_INTERVAL * 0.8:
        print(
            f"WARNING: poll took {LAST_POLL['duration']:.1f}s "
            f"(interval {POLL_INTERVAL}s) - raise POLL_INTERVAL"
        )


# ========== MAIN / BOOTSTRAP ==========


async def _close_http_client(app):
    """CHANGED: don't leak the shared client on shutdown."""
    global _http_client
    if _http_client is not None and not _http_client.is_closed:
        await _http_client.aclose()


def main():
    if not TELEGRAM_BOT_TOKEN:
        raise SystemExit(
            "TELEGRAM_BOT_TOKEN is not set.\n"
            "  export TELEGRAM_BOT_TOKEN='your-token-here'"
        )

    # Configure HTTPX-based request with higher timeouts for Telegram
    request = HTTPXRequest(
        read_timeout=30.0,
        write_timeout=30.0,
        connect_timeout=15.0,
        pool_timeout=10.0,
    )

    # CHANGED: dropped the manual JobQueue() + set_application() dance.
    # ApplicationBuilder creates and wires a JobQueue for you; doing it by hand
    # is a leftover from older python-telegram-bot versions.
    app = (
        ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .request(request)
        .post_shutdown(_close_http_client)  # CHANGED
        .build()
    )

    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("list", list_addresses))
    app.add_handler(CommandHandler("add", add_address))
    app.add_handler(CommandHandler("remove", remove_address))
    app.add_handler(CommandHandler("status", status))  # CHANGED

    # Schedule the repeating Polymarket poll
    app.job_queue.run_repeating(
        poll_polymarket,
        interval=POLL_INTERVAL,
        first=5,
        # CHANGED: made the scheduler behavior explicit. coalesce=True collapses
        # a backlog of missed runs into one instead of firing them back to back;
        # misfire_grace_time lets a slightly-late run still execute rather than
        # being discarded outright.
        job_kwargs={
            "max_instances": 1,
            "coalesce": True,
            "misfire_grace_time": POLL_INTERVAL,
        },
    )

    app.run_polling()


if __name__ == "__main__":
    main()
