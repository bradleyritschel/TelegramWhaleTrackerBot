# Polymarket Watcher

A Telegram bot that polls the Polymarket data API for a watchlist of wallet
addresses and pushes an alert whenever one of them makes a trade above a
notional threshold.

---

## What it does

Every `POLL_INTERVAL` seconds the bot fetches the most recent 150 trades for
each tracked address, filters out anything it has already seen, groups the
remaining fills into logical orders, and sends a Telegram message for each
order worth at least `MIN_TRADE_NOTIONAL` USDC.

An alert looks like this:

```
📈 New trade by Pringles (0xb1d9476e5a5ba938b57cf0a5dc7a91a114605ee1)
BUY 12,400 @ 0.4150 (~$5,146) (3 fills)
Market: Will X happen by December? (Yes)
Time: 2026-08-21 14:02:11 UTC
Link: https://polymarket.com/event/will-x-happen-by-december
Tx: https://polygonscan.com/tx/0xabc...
```

---

## Requirements

- Python 3.10+ (the code uses `X | None` type syntax)
- A Telegram bot token from [@BotFather](https://t.me/BotFather)
- Your Telegram numeric chat ID(s) — get one from [@userinfobot](https://t.me/userinfobot)

```bash
pip install "python-telegram-bot[job-queue]>=20.0" httpx
```

The `[job-queue]` extra matters. Without it `app.job_queue` is `None` and the
bot starts but never polls.

---

## Setup

**1. Set your token as an environment variable.**

```bash
export TELEGRAM_BOT_TOKEN="123456789:AAH..."
```

The bot refuses to start without it. Never put the token back in the source
file — a token in a file you share, paste, or commit is a token someone else
can drive your bot with.

**2. Set your chat IDs.**

Edit `TELEGRAM_CHAT_IDS` near the top of `polymarket_bot.py`. These are the
chats that receive alerts. The bot must have been started (`/start`) by each
user, or added to each group, before it can message them.

**3. Run it.**

```bash
python polymarket_bot.py
```

On first run it creates `tracked_addresses.json` seeded from
`DEFAULT_TRACKED_ADDRESSES`, and baselines every address so you don't get a
flood of historical trades.

---

## Bot commands

| Command | Description |
| --- | --- |
| `/start` | Confirm the bot is alive and list commands |
| `/list` | Show every tracked address and its label |
| `/add 0xAddress Label` | Start tracking an address. Label may contain spaces. |
| `/remove 0xAddress` | Stop tracking and drop its state |
| `/status` | Last poll age, duration, fetch errors, alerts sent |

Newly added addresses are baselined at the moment you add them, so you only
see trades made from that point forward.

`/status` is the first place to look when alerts stop arriving. If it reports
a poll duration close to `POLL_INTERVAL`, or a nonzero error count, that's your
problem.

---

## Configuration

All of this lives at the top of `polymarket_bot.py`.

| Setting | Default | Notes |
| --- | --- | --- |
| `POLL_INTERVAL` | `30` | Seconds between polls. Watch for the overrun warning in the logs if you lower it. |
| `MIN_TRADE_NOTIONAL` | `1000` | Minimum order value in USDC to alert on. |
| `MAX_SEEN_KEYS` | `2000` | Trade keys remembered per address. Must stay comfortably above the 150-trade fetch window. |
| `HTTP_CONCURRENCY` | `5` | Simultaneous requests to the Polymarket API. |
| `FETCH_RETRIES` | `2` | Retries on timeouts, 429s, and 5xx. |
| `AGGREGATE_FILLS_BY_TX` | `True` | Group fills into one alert and threshold on the combined size. |

### On `AGGREGATE_FILLS_BY_TX`

A single Polymarket order matched against several makers comes back from the
API as several rows sharing one transaction hash. With aggregation on, those
collapse into one alert and the threshold is applied to the total — so a
$5,000 order that filled as ten $500 chunks correctly fires. With it off, each
fill is evaluated on its own and that order produces no alert at all.

Leave it on unless you specifically want fill-level granularity.

---

## Data files

Both are plain JSON in the working directory, written atomically (temp file +
`os.replace`) so a crash mid-write can't leave a truncated file.

**`tracked_addresses.json`** — `{address: label}`. Safe to hand-edit while the
bot is stopped. Addresses are lowercased on load, so casing doesn't matter.

**`polymarket_state.json`** — per-address dedup state:

```json
{
  "0xabc...": {
    "last_ts": 1755782400,
    "seen_hashes": ["0xdef...|1755782390|BUY|...", "..."],
    "key_version": 2
  }
}
```

- `last_ts` — newest trade timestamp seen, used for cold-start baselining
- `seen_hashes` — composite trade keys, newest last, trimmed to `MAX_SEEN_KEYS`
- `key_version` — bumped when the key format changes; triggers a re-baseline

Deleting this file makes the bot re-baseline everything on the next run: no
alerts for existing history, no duplicates.

---

## How deduplication works

Identity is a composite key, not the bare transaction hash:

```
transactionHash | timestamp | side | asset | outcome | size | price
```

The transaction hash alone is not unique per fill, so keying on it silently
discards every fill after the first in a multi-maker order.

Keys are stored in an **ordered list** with a set alongside for lookups. Order
matters: trimming an unordered set keeps an arbitrary subset rather than the
newest entries, and a key evicted while still inside the API's 150-trade
window will re-alert as new.

A trade is alerted exactly once, when its key is first seen and the order it
belongs to clears the notional threshold. Keys are recorded *before* the send
attempt, so a Telegram failure drops that one alert rather than retrying it
every poll.

---

## Troubleshooting

**No alerts at all.** Check `/status`. If no poll has completed, the job queue
probably isn't installed — reinstall with the `[job-queue]` extra. If polls are
running cleanly, your threshold may simply be above recent activity; drop
`MIN_TRADE_NOTIONAL` temporarily to confirm the pipeline works end to end.

**Alerts stopped after running fine for a while.** Look for the
`WARNING: poll took Ns` line. Polls that overrun `POLL_INTERVAL` get coalesced,
which thins out your effective polling rate. Raise the interval or lower
`HTTP_CONCURRENCY` if you're being rate limited.

**Duplicate alerts.** Almost always means state was lost — a deleted or
corrupted `polymarket_state.json`, or the bot running from two different
working directories. Both instances would keep separate state files.

**Fetch errors piling up in `/status`.** Usually Polymarket rate limiting.
Raise `POLL_INTERVAL` or lower `HTTP_CONCURRENCY`.

**Telegram "chat not found".** The bot can't initiate conversations. Each user
in `TELEGRAM_CHAT_IDS` must message the bot first.

---

## Notes

The 150-trade fetch window is the real constraint on how long the bot can be
offline. If a tracked address makes more than 150 trades while the bot is down,
the ones that scrolled out of the window are gone — they'll never be seen, and
they won't alert.

Timestamps from the API are treated as UTC seconds. The bot does not attempt to
detect market resolution, position changes, or PnL; it reports fills only.
