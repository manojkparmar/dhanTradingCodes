"""
╔══════════════════════════════════════════════════════════════╗
║         BankNifty Options Trading Bot — Dhan Broker          ║
║  Strategy: Square-Root Level Breakout with ATM CE/PE Entry   ║
╚══════════════════════════════════════════════════════════════╝

Flow:
  1. Wait for 9:20 AM → fetch 9:15 5-min BankNifty SPOT candle close
  2. Calculate buy / sell levels using the sqrt formula
  3. Monitor live spot price every tick:
       • Spot >= Buy + 2  → BUY ATM CE
       • Spot <= Sell - 2 → BUY ATM PE
  4. Exit rules:
       CE: exit when Spot >= T1  OR  CE_premium >= entry_premium + 25
           SL: Spot <= S1 (market exit)
       PE: exit when Spot <= S1  OR  PE_premium >= entry_premium + 25
           SL: Spot >= T1 (market exit)

Requirements:
    pip install dhanhq pandas python-dotenv

Setup:
    Copy .env.example → .env and fill in credentials.
"""

import asyncio
import math
import os
import sys
import time
import logging
import threading
from datetime import datetime, date
from zoneinfo import ZoneInfo

import random
import pandas as pd
from dhanhq import dhanhq, MarketFeed
from dotenv import load_dotenv

# Generate filename: bnf_YYYYMMDD_HHMMSS.out
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = "run_log/" + f"bnf_{timestamp}.out"

# Tee: write every print() / sys.stdout write to BOTH the .out file and the
# original console so output is visible in the terminal AND saved to file.
class _Tee:
    def __init__(self, *streams):
        self._streams = streams

    def write(self, data):
        for s in self._streams:
            try:
                if not getattr(s, 'closed', False):
                    s.write(data)
                    s.flush()
            except Exception:
                pass  # silently ignore write errors during shutdown

    def flush(self):
        for s in self._streams:
            try:
                if not getattr(s, 'closed', False):
                    s.flush()
            except Exception:
                pass

    def fileno(self):
        return self._streams[0].fileno()

_console_out = sys.stdout
_console_err = sys.stderr
log_file = open(log_filename, "w", encoding="utf-8")

sys.stdout = _Tee(_console_out, log_file)
sys.stderr = _Tee(_console_err, log_file)

# --- Your job code below ---
print("Job started...")
# ...

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),          # goes to Tee → console + file
        logging.FileHandler(f"logs/bnf_bot_{timestamp}.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("BNBot")

IST = ZoneInfo("Asia/Kolkata")

# ── Credentials ───────────────────────────────────────────────────────────────
# load_dotenv() searches: current working directory first, then the script's
# own directory.  We explicitly try both so it works regardless of where you
# launch the script from.

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_ENV_PATHS = [
    os.path.join(os.getcwd(), ".env"),  # wherever you ran `python` from
    os.path.join(_SCRIPT_DIR, ".env"),  # same folder as this .py file
    "C:/Trading/AlgoTrading/.env"  # add .env file's path manually
]

_loaded = False
for _p in _ENV_PATHS:
    if os.path.isfile(_p):
        load_dotenv(_p, override=True)
        print(f"[INFO] Loaded credentials from: {_p}")
        _loaded = True
        break

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

if not CLIENT_ID or not ACCESS_TOKEN:
    _sep = "\n  "
    _checked = _sep.join(_ENV_PATHS)
    raise EnvironmentError(
        "\n"
        "╔══════════════════════════════════════════════════════╗\n"
        "║  Missing Dhan credentials — follow these steps:      ║\n"
        "╚══════════════════════════════════════════════════════╝\n"
        "\n"
        "  1. Create a file named exactly:  .env\n"
        f"     Place it in:  {_SCRIPT_DIR}\n"
        "\n"
        "  2. Paste these two lines into .env (no quotes):\n"
        "       DHAN_CLIENT_ID=your_actual_client_id\n"
        "       DHAN_ACCESS_TOKEN=your_actual_access_token\n"
        "\n"
        "  3. Get your credentials from:\n"
        "       https://dhanhq.co → My Account → Access Token\n"
        "\n"
        f"  Looked for .env in:\n  {_sep}{_checked}\n"
        + ("  (No .env file was found in either location)\n" if not _loaded else
           "  (.env was found but DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN are missing or blank)\n")
    )

# dhanhq v2 requires DhanContext(client_id, access_token) passed into dhanhq()
# dhanhq v1 took dhanhq(client_id, access_token) directly
try:
    from dhanhq import DhanContext

    dhan_ctx = DhanContext(CLIENT_ID, ACCESS_TOKEN)
    dhan = dhanhq(dhan_ctx)
    print("[INFO] dhanhq SDK v2 initialised via DhanContext.")
except ImportError:
    dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
    print("[INFO] dhanhq SDK v1 initialised.")

# Get fund limits (account balance)
try:
    balance_info = dhan.get_fund_limits()

    # Print the full response
    # print(balance_info, "\n")
    print(f"\n Client ID : {balance_info['data']['dhanClientId']}")
    print(f" SOD Limit: {balance_info['data']['sodLimit']}")
    print(f" Collateral Amount : {balance_info['data']['collateralAmount']}")
    print(f" Available Balance: {balance_info['data']['availabelBalance']}")
    print(f" Utilized Amount: {balance_info['data']['utilizedAmount']} \n")

except Exception as e:
    print(f"Error fetching balance: {e}")

# ═════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═════════════════════════════════════════════════════════════════════════════

BANKNIFTY_SECURITY_ID   = "25"  # Dhan security ID for BANKNIFTY index
BANKNIFTY_INDEX_EXCH    = "IDX_I"  # Exchange segment for index spot quote
BANKNIFTY_FNO_EXCH      = "NSE_FNO"  # Exchange for F&O orders
INSTRUMENT_INDEX        = "INDEX"
INSTRUMENT_OPTIDX       = "OPTIDX"

LOT_SIZE                = 30  # BankNifty lot size (verify current)
POLL_INTERVAL_SEC       = 1  # Seconds between spot price polls
PREMIUM_TARGET_POINTS   = 25  # Exit when premium gains 25 pts
ENTRY_BUFFER            = 2  # Spot must be >= buy+2 or <= sell-2
ORDER_TYPE              = "MARKET"
PRODUCT                 = "INTRADAY"  # or "CNC" / "MARGIN"
SUMMARY                 = ""

# ── Candle time configuration ─────────────────────────────────────────────────
# Set the 5-minute candle whose CLOSE you want to use for level calculation.
# Change CANDLE_HOUR / CANDLE_MINUTE here — nothing else needs to be touched.
#
#   Examples:
#     9:15  → CANDLE_HOUR = 9,  CANDLE_MINUTE = 15   (market open candle)
#    10:00  → CANDLE_HOUR = 10, CANDLE_MINUTE = 0
#    11:30  → CANDLE_HOUR = 11, CANDLE_MINUTE = 30
#    14:00  → CANDLE_HOUR = 14, CANDLE_MINUTE = 0
#
# Rule: CANDLE_MINUTE must be a multiple of 5 and in range 0-55.
# The bot will wait until (CANDLE_HOUR : CANDLE_MINUTE + 5) before fetching,
# so the candle is guaranteed to be fully closed.
# ─────────────────────────────────────────────────────────────────────────────
CANDLE_HOUR   = 9   # ← change this
CANDLE_MINUTE = 15  # ← change this  (must be multiple of 5)

"""
Architecture note (v10):

 MarketFeed runs in a background daemon thread (_bg_feed_loop).
 The main trading loop reads prices from a shared dict (_ltp_prices)
 that the background thread updates on every incoming tick.
 This gives sub-second freshness with zero blocking in the main loop.

 Feed lifecycle:
   _start_feed()          → called once at startup (spot only)
   _start_feed(option_id) → called once at entry (adds option)
   _stop_feed()           → called at session end / trade done
"""


# ═════════════════════════════════════════════════════════════════════════════
# LEVEL CALCULATOR
# ═════════════════════════════════════════════════════════════════════════════

def calculate_levels(cmp: float) -> dict:
    """Square-root formula to compute buy / sell levels around CMP."""
    r1 = math.floor(math.sqrt(cmp)) - 1

    def level(n: int) -> float:
        return round((r1 + n * 0.125) ** 2, 0)

    buy_step = next(n for n in range(1, 200) if level(n) > cmp)
    sell_step = next(n for n in range(buy_step - 1, 0, -1) if level(n) < cmp)

    return {
        "buy": level(buy_step),
        "t1": level(buy_step + 1),
        "t2": level(buy_step + 2),
        "t3": level(buy_step + 3),
        "sell": level(sell_step),
        "s1": level(sell_step - 1),
        "s2": level(sell_step - 2),
        "s3": level(sell_step - 3),
    }


# ═════════════════════════════════════════════════════════════════════════════
# MARKET DATA HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def get_candle_close(hour: int = CANDLE_HOUR, minute: int = CANDLE_MINUTE) -> float:
    """
    Fetch today's 5-min BankNifty candle for the given hour:minute and return its close.

    To change which candle is used, update CANDLE_HOUR / CANDLE_MINUTE at the top
    of this file — no other code needs to change.

    Args:
        hour   : candle start hour in IST   (default: CANDLE_HOUR)
        minute : candle start minute in IST (default: CANDLE_MINUTE, must be multiple of 5)
    """
    candle_label = f"{hour}:{minute:02d}"
    today_str = date.today().strftime("%Y-%m-%d")
    log.info(f"Fetching BankNifty 5-min candle for {candle_label} | date={today_str} ...")

    resp = dhan.intraday_minute_data(
        security_id=BANKNIFTY_SECURITY_ID,
        exchange_segment=BANKNIFTY_INDEX_EXCH,
        instrument_type=INSTRUMENT_INDEX,
        from_date=today_str,
        to_date=today_str,
        interval=5
    )

    # intraday_minute_data returns dict directly (not nested under "data" in v2)
    # Handle both v1 (resp["data"][...]) and v2 (resp directly has the lists)
    if isinstance(resp, dict) and "data" in resp:
        data = resp["data"]
    else:
        data = resp

    if not data or not data.get("timestamp"):
        raise ValueError(f"Empty or unexpected candle response: {resp}")

    # Timestamps: v2 returns epoch seconds (int); convert accordingly
    timestamps = data["timestamp"]
    try:
        ts_series = pd.to_datetime(timestamps, unit="s", utc=True)
    except Exception:
        ts_series = pd.to_datetime(timestamps, utc=True)

    df = pd.DataFrame({
        "timestamp": ts_series,
        "open": pd.to_numeric(data["open"], errors="coerce"),
        "high": pd.to_numeric(data["high"], errors="coerce"),
        "low": pd.to_numeric(data["low"], errors="coerce"),
        "close": pd.to_numeric(data["close"], errors="coerce"),
    })

    df.set_index("timestamp", inplace=True)
    df.index = df.index.tz_convert(IST)
    df.sort_index(inplace=True)

    log.info(
        f"Received {len(df)} 5-min candles. "
        f"Range: {df.index[0].strftime('%H:%M')} → {df.index[-1].strftime('%H:%M')}"
    )

    # Resample 1-min → 5-min (API returns 5-min bars; resample normalises any gaps)
    df_5 = df.resample("5min", label="left", closed="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}
    ).dropna()

    # Match by hour and minute directly — avoids any tz-offset mismatch
    target = None
    for ts in df_5.index:
        if ts.hour == hour and ts.minute == minute:
            target = ts
            break

    if target is None:
        available = df_5.index.strftime("%H:%M").tolist()
        raise ValueError(
            f"{candle_label} candle not found in today's data. "
            f"Available candles: {available}"
        )

    row = df_5.loc[target]
    close = float(row["close"])
    log.info(
        f"{candle_label} Candle  O:{row['open']}  H:{row['high']}  L:{row['low']}  C:{close}"
    )
    return close


# ═════════════════════════════════════════════════════════════════════════════
# MARKETFEED — BACKGROUND THREAD ARCHITECTURE  (v10)
# ═════════════════════════════════════════════════════════════════════════════
#
# ROOT CAUSE OF v9 SLOWNESS:
#   run_forever() on Dhan's async MarketFeed is a BLOCKING call — it runs
#   the WebSocket event loop until the connection drops, not "process one tick
#   and return".  Calling it in a tight poll loop therefore blocks for ~10s
#   per call while waiting for the socket.  With _MAX_EMPTY=3 that was
#   3 × 10s = 30s minimum per poll cycle, causing the 30-second gaps in the
#   log between every [BATCH] print.
#
# FIX — background thread design:
#   A single daemon thread runs run_forever() continuously in the background.
#   Every incoming tick is parsed and stored in _ltp_prices (a plain dict
#   protected by a threading.Lock).  The main trading loop simply reads from
#   that dict — it always gets the latest price with zero blocking, zero
#   sleep, and zero 429 risk from repeated reconnects.
#
#   Subscribing a new instrument (e.g. an option after entry) triggers a
#   clean feed rebuild: the background thread is stopped, a new MarketFeed
#   with the expanded instrument list is created, and the thread restarts.
#   This rebuild takes <2 seconds and happens at most once per session.
# ─────────────────────────────────────────────────────────────────────────────

_LTP_KEYS         = ("LTP", "last_price", "ltp")
_ltp_prices: dict = {}          # sec_id (str) → float LTP, updated by bg thread
_ltp_timestamps: dict = {}      # sec_id (str) → monotonic time of last update
_ltp_lock         = threading.Lock()

_feed_instance        = None    # active MarketFeed object
_feed_thread          = None    # background threading.Thread
_feed_subscribed_ids  = set()   # security_ids in the current feed
_feed_stop_event      = threading.Event()  # set to ask the bg thread to stop

_MAX_429_RETRIES  = 5
_BASE_BACKOFF_SEC = 5


def _bg_feed_loop(feed: "MarketFeed", stop_event: threading.Event):
    """
    Background thread: call run_forever() in a tight loop so the WebSocket
    stays alive and every incoming tick is immediately stored in _ltp_prices.

    run_forever() blocks until one of:
      (a) a packet arrives and is processed  — returns quickly
      (b) the connection drops               — we log and let the loop retry
      (c) stop_event is set                  — we exit cleanly

    We check stop_event before each call so the main thread can shut us down
    without waiting for a blocking recv().
    """
    global _ltp_prices, _ltp_timestamps

    while not stop_event.is_set():
        try:
            feed.run_forever()
            response = feed.get_data()

            if response and isinstance(response, dict):
                sec_id  = str(response.get("security_id", ""))
                ltp_raw = next(
                    (response[k] for k in _LTP_KEYS
                     if k in response and response[k] is not None),
                    None
                )
                if sec_id and ltp_raw is not None:
                    try:
                        val = float(ltp_raw)
                        with _ltp_lock:
                            _ltp_prices[sec_id]     = val
                            _ltp_timestamps[sec_id] = time.monotonic()
                    except (ValueError, TypeError):
                        pass
        except Exception as exc:
            if stop_event.is_set():
                break   # normal shutdown — ignore the error
            log.warning(f"[BG-FEED] tick error (will retry): {exc}")
            time.sleep(0.5)   # brief pause before retrying


def _start_feed(option_security_id: str | None = None):
    """
    Build a MarketFeed subscribed to BANKNIFTY spot + optional option,
    then launch the background thread.

    Always include spot (security_id="25") and, when a position is open,
    the option security_id as well.

    WHY Quote INSTEAD OF Ticker:
      Ticker fires only on matched trades — between trades the value is stale.
      Quote fires on every bid/ask update AND trade, giving sub-second freshness.
    """
    global _feed_instance, _feed_thread, _feed_subscribed_ids, _feed_stop_event

    # Build instrument list
    instruments = [(MarketFeed.IDX, "25", MarketFeed.Quote)]
    if option_security_id:
        instruments.append(
            (MarketFeed.NSE_FNO, str(option_security_id), MarketFeed.Quote)
        )
    requested_ids = {str(inst[1]) for inst in instruments}

    # Already subscribed to exactly what we need → nothing to do
    if requested_ids == _feed_subscribed_ids and _feed_thread and _feed_thread.is_alive():
        return

    # ── Stop the existing background thread if running ────────────────────────
    _stop_feed()

    # ── Create new MarketFeed with 429 back-off ───────────────────────────────
    for attempt in range(_MAX_429_RETRIES):
        try:
            id_list = [inst[1] for inst in instruments]
            log.info(f"(Re)creating MarketFeed with instruments: {id_list}")
            feed = MarketFeed(dhan_ctx, instruments, version="v2")
            break
        except Exception as exc:
            if "429" in str(exc):
                wait = _BASE_BACKOFF_SEC * (2 ** attempt)
                log.warning(
                    f"MarketFeed 429 on connect "
                    f"(attempt {attempt + 1}/{_MAX_429_RETRIES}). "
                    f"Backing off {wait}s ..."
                )
                time.sleep(wait)
            else:
                raise
    else:
        raise RuntimeError(
            "Could not connect to MarketFeed after repeated 429 errors. "
            "Wait a few minutes and restart the bot."
        )

    # ── Start background thread ───────────────────────────────────────────────
    stop_evt           = threading.Event()
    _feed_stop_event   = stop_evt
    _feed_instance     = feed
    _feed_subscribed_ids = requested_ids

    t = threading.Thread(
        target=_bg_feed_loop,
        args=(feed, stop_evt),
        daemon=True,          # dies automatically when main thread exits
        name="MarketFeedBG",
    )
    t.start()
    _feed_thread = t

    # Give the feed ~2 seconds to receive the first ticks before the caller reads
    log.info("MarketFeed background thread started. Warming up (2s) ...")
    time.sleep(2)
    log.info(f"Feed ready. Prices so far: { {k: v for k, v in _ltp_prices.items()} }")


def _stop_feed():
    """Signal the background thread to stop and wait for it to finish."""
    global _feed_instance, _feed_thread, _feed_subscribed_ids, _feed_stop_event

    if _feed_stop_event is not None:
        _feed_stop_event.set()

    if _feed_thread is not None and _feed_thread.is_alive():
        _feed_thread.join(timeout=5)

    if _feed_instance is not None:
        try:
            _feed_instance.disconnect()
        except Exception:
            pass
        # Cancel any pending asyncio tasks (e.g. websockets keepalive) left
        # behind in the feed's event loop to suppress the warning:
        # "Task was destroyed but it is pending!"
        try:
            loop = getattr(_feed_instance, "_loop", None)
            if loop is None:
                for attr in ("loop", "_event_loop", "event_loop"):
                    loop = getattr(_feed_instance, attr, None)
                    if loop is not None:
                        break
            if loop is not None and not loop.is_closed():
                pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
                if pending:
                    for task in pending:
                        task.cancel()
                    loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True)
                    )
        except Exception:
            pass  # best-effort — never let cleanup crash the bot

    _feed_instance       = None
    _feed_thread         = None
    _feed_subscribed_ids = set()
    _feed_stop_event     = threading.Event()


def _read_ltp(security_id: str, max_age_sec: float = 10.0) -> float:
    """
    Read the latest LTP for a security_id from the shared price dict.

    Raises RuntimeError if:
      • No price has ever been received for this id, OR
      • The last received price is older than max_age_sec
        (feed may be stalled / disconnected).

    max_age_sec=10 is conservative — at Quote subscription level the index
    updates multiple times per second during market hours.
    """
    with _ltp_lock:
        price = _ltp_prices.get(str(security_id))
        ts    = _ltp_timestamps.get(str(security_id), 0)

    if price is None:
        raise RuntimeError(
            f"No price received yet for security_id={security_id}. "
            f"Feed may still be warming up — retry in 1s."
        )

    age = time.monotonic() - ts
    if age > max_age_sec:
        raise RuntimeError(
            f"Price for security_id={security_id} is {age:.1f}s old "
            f"(>{max_age_sec}s threshold). Feed may be stalled."
        )

    return price


def get_live_spot() -> float:
    """Return the latest BankNifty spot LTP from the background feed."""
    spot = _read_ltp("25")
  #  print(f"\n SPOT is {spot} \n")
    return spot


def get_spot_and_option_ltp(security_id: str) -> tuple[float, float]:
    """
    Return (spot_ltp, option_ltp) — both read instantly from the shared
    price dict maintained by the background feed thread.
    No blocking, no WebSocket call, no 429 risk.
    """
    spot = _read_ltp("25")
    opt  = _read_ltp(str(security_id))
   # print(f"\n [BATCH] SPOT={spot}  |  Option LTP={opt} \n")
    return spot, opt


# ═════════════════════════════════════════════════════════════════════════════
# OPTION CHAIN HELPER
# ═════════════════════════════════════════════════════════════════════════════

def get_atm_option(spot: float, option_type: str) -> dict:

    # print(f"\n Spot : {spot}")
    bf_all_exp = dhan.expiry_list(25, "IDX_I")
    bf_latest_exp = bf_all_exp["data"]["data"][0]

    print("\n Working on Bank Nifty's Expiry", bf_latest_exp, ",\n")

    atm_strike = round(spot / 100) * 100
    print(f" Spot is {spot}, Rounded ATM Strike: ", atm_strike)
    if atm_strike > spot and option_type == 'CE':
        atm_strike = atm_strike - 100
        print("\n Adjusted FINAL Strike for CE :", atm_strike, "ATM(ITM side)")
    elif atm_strike < spot and option_type == 'PE':
        atm_strike = atm_strike + 100
        print("\n Adjusted FINAL Strike for PE :", atm_strike, "ATM(ITM side)")

    # remove below line in production code
    print("\n Bank Nifty ATM Strike & Expiry is: ", atm_strike, option_type, bf_latest_exp, "\n")

    resp = dhan.option_chain(25, "IDX_I", bf_latest_exp)

    if resp.get("status") != "success":
        raise RuntimeError(f"Option chain error: {resp}")

    chain_data = resp.get("data", {}).get("data", [])

    oc = chain_data.get("oc", {})

    # Uncomment below line for debugging the code:

    #  print(list(oc.keys())[:10])  # see exact format
    #  print(f"Looking for: {atm_strike}, type: {type(atm_strike)} \n\n ")
    #  print(list(oc.keys())[:5],"\n")        # should show strike prices
    #  print(list(oc.values())[:1],"\n\n")      # should show CE/PE data per strike

    #  print("type(chain_data):\n")
    # print(type(chain_data))
    #  print("\n\nlist(chain_data.keys())[:5]\n")
    #  print(list(chain_data.keys())[:5])  # see the keys
    #  print("\n\nlist(chain_data.values())[:1]\n")
    #  print(list(chain_data.values())[:1])  # see the structure of one entry

    if not chain_data:
        raise ValueError("Empty option chain.")

    # Direct lookup using exact key format
    strike_key = f"{float(atm_strike):.6f}"  # → "53400.000000"
    strike_data = oc.get(strike_key, {})

    contract = strike_data.get(option_type.lower())  # "CE" → "ce", "PE" → "pe"

    if contract:
        log.info(
            f"ATM {option_type} found: strike={atm_strike}, "
            f"expiry={bf_latest_exp}, "
            f"security_id={contract['security_id']}"
        )
        return {
            "security_id": str(contract["security_id"]),
            "strike": atm_strike,
            "expiry": bf_latest_exp,
            "symbol": contract.get("tradingSymbol", ""),
        }

    log.warning(f"ATM {option_type} not found for strike={atm_strike} in option chain.")
    return None


# ═════════════════════════════════════════════════════════════════════════════
# ORDER MANAGEMENT
# ═════════════════════════════════════════════════════════════════════════════

# ── Order helpers ─────────────────────────────────────────────────────────────
# dhanhq v2 uses plain strings for transaction_type / order_type / product_type.
# dhanhq v1 used class constants (dhan.BUY, dhan.MARKET, dhan.INTRADAY).
# We use strings here — they work on both versions.

def _place_order(security_id: str, txn_type: str) -> str:
    #Internal: place a MARKET INTRADAY order and return order_id.
    resp = dhan.place_order(
        security_id      = security_id,
        exchange_segment = dhan.NSE_FNO,
        transaction_type = dhan.BUY if txn_type == "BUY" else dhan.SELL,
        quantity         = LOT_SIZE,
        order_type       = dhan.MARKET,
        product_type     = dhan.INTRA,
        price            = 0,
    )
    if resp.get("status") != "success":
        raise RuntimeError(f"Order failed ({txn_type}): {resp}")
    order_id = resp["data"]["orderId"]
    return order_id


def place_buy_order(security_id: str, symbol: str) -> str:
    """Place a market BUY order and return the order_id."""
    log.info(f"Placing BUY order → {symbol} ({security_id}), qty={LOT_SIZE}")
    order_id = _place_order(security_id, "BUY")
    log.info(f"BUY order placed. order_id={order_id}")
    return order_id


def place_sell_order(security_id: str, symbol: str, reason: str) -> str:
    """Place a market SELL (exit) order."""
    log.info(f"EXIT [{reason}] → {symbol} ({security_id}), qty={LOT_SIZE}")
    order_id = _place_order(security_id, "SELL")
    log.info(f"SELL order placed. order_id={order_id}")
    return order_id


# ═════════════════════════════════════════════════════════════════════════════
# POSITION STATE
# ═════════════════════════════════════════════════════════════════════════════

class Position:
    def __init__(self):
        self.active: bool = False
        self.option_type: str = ""  # "CE" or "PE"
        self.security_id: str = ""
        self.symbol: str = ""
        self.entry_premium: float = 0.0
        self.entry_spot: float = 0.0
        self.entry_time: str = ""

    def open(self, option_type, security_id, symbol, premium, spot):
        self.active = True
        self.option_type = option_type
        self.security_id = security_id
        self.symbol = symbol
        self.entry_premium = premium
        self.entry_spot = spot
        self.entry_time = datetime.now(IST).strftime("%H:%M:%S")
        log.info(
            f"Position OPENED | {option_type} | {symbol} | "
            f"entry_premium={premium} | spot={spot}"
        )

    def close(self):
        self.active = False
        log.info(f"Position CLOSED | {self.option_type} | {self.symbol}")

    def __repr__(self):
        return (
            f"<Position {self.option_type} {self.symbol} "
            f"entry_prem={self.entry_premium} active={self.active}>"
        )


# ═════════════════════════════════════════════════════════════════════════════
# BOT MAIN LOOP
# ═════════════════════════════════════════════════════════════════════════════

def print_levels(levels: dict, close: float, candle_label: str = None):
    if candle_label is None:
        candle_label = f"{CANDLE_HOUR}:{CANDLE_MINUTE:02d}"
    w = 46
    print(f"\n{'═' * w}")
    print(f"  BANKNIFTY  |  {candle_label} Close: {close:,.2f}")
    print(f"{'═' * w}")
    print(f"  {'BUY ABOVE':<22} {levels['buy']:>10,.0f}  (+{ENTRY_BUFFER} → entry)")
    print(f"  {'T1':<22} {levels['t1']:>10,.0f}")
    print(f"  {'T2':<22} {levels['t2']:>10,.0f}")
    print(f"  {'T3':<22} {levels['t3']:>10,.0f}")
    print(f"  {'-' * 38}")
    print(f"  {'SELL BELOW':<22} {levels['sell']:>10,.0f}  (-{ENTRY_BUFFER} → entry)")
    print(f"  {'S1':<22} {levels['s1']:>10,.0f}")
    print(f"  {'S2':<22} {levels['s2']:>10,.0f}")
    print(f"  {'S3':<22} {levels['s3']:>10,.0f}")
    print(f"{'═' * w}\n")


def wait_until_candle_ready(hour: int = CANDLE_HOUR, minute: int = CANDLE_MINUTE):
    """
    Block until 5 minutes after the chosen candle's start time, so the candle
    is fully closed before we fetch it.

    Example: CANDLE_HOUR=9, CANDLE_MINUTE=15  → waits until 9:20 AM IST
             CANDLE_HOUR=10, CANDLE_MINUTE=0  → waits until 10:05 AM IST
    """
    now = datetime.now(IST)

    # Candle closes at (hour : minute + 5); add 5 min to the start
    ready_minute = minute + 5
    ready_hour   = hour + ready_minute // 60
    ready_minute = ready_minute % 60

    target = now.replace(hour=ready_hour, minute=ready_minute, second=0, microsecond=0)
    candle_label = f"{hour}:{minute:02d}"
    ready_label  = f"{ready_hour}:{ready_minute:02d}"

    if now < target:
        wait_sec = (target - now).total_seconds()
        log.info(
            f"Waiting {wait_sec:.0f}s for {candle_label} candle to close "
            f"(ready at {ready_label} IST) ..."
        )
        time.sleep(wait_sec)
    else:
        log.info(f"{candle_label} candle already closed (past {ready_label}). Fetching now.")


def wait_until_market_open():
    """Block until 9:15 AM IST (exchange open)."""
    now = datetime.now(IST)
    target = now.replace(hour=9, minute=15, second=0, microsecond=0)
    if now < target:
        wait_sec = (target - now).total_seconds()
        log.info(f"Market opens in {wait_sec:.0f}s. Sleeping ...")
        time.sleep(wait_sec)


def is_market_open() -> bool:
    now = datetime.now(IST)
    print(now)

    #""" un comment below 3 lines in production
    start = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    end   = now.replace(hour=15, minute=25, second=0, microsecond=0)
    return start <= now <= end

    #return 1


def run_bot():
    candle_label = f"{CANDLE_HOUR}:{CANDLE_MINUTE:02d}"

    log.info("══════════════════════════════════════════")
    log.info("  BankNifty Options Bot — Starting Up")
    log.info(f"  Date        : {date.today()}")
    log.info(f"  Candle time : {candle_label} (5-min close used for levels)")
    log.info("══════════════════════════════════════════")

    # ── Step 1: Wait for market open, then for chosen candle to close ─────────
    wait_until_market_open()
    wait_until_candle_ready(CANDLE_HOUR, CANDLE_MINUTE)

    # ── Step 2: Fetch candle & compute levels ──────────────────────────────────
    close = get_candle_close(CANDLE_HOUR, CANDLE_MINUTE)
    levels = calculate_levels(close)

    print_levels(levels, close, candle_label)

    buy_trigger  = levels["buy"]  + ENTRY_BUFFER
    sell_trigger = levels["sell"] - ENTRY_BUFFER

    log.info(f"Entry triggers → CE if spot >= {buy_trigger} | PE if spot <= {sell_trigger}")

    # ── Step 3: Start background MarketFeed (spot only) ───────────────────────
    # Feed runs in a daemon thread — the main loop reads prices instantly with
    # no blocking.  We rebuild the feed once after entry to add the option.
    _start_feed()   # spot only to begin with

    # ── Step 4: Main monitoring loop ───────────────────────────────────────────
    pos = Position()
    trade_done = False  # one trade per session

    while is_market_open():
        try:
            if pos.active:
                spot, prem = get_spot_and_option_ltp(pos.security_id)
            else:
                spot = get_live_spot()

            time.sleep(1)

            # ── No position: look for entry ────────────────────────────────────
            if not pos.active and not trade_done:

                if spot >= buy_trigger:
                    log.info(f"BUY SIGNAL: spot {spot} >= {buy_trigger}")
                    option = get_atm_option(spot, "CE")
                    # Rebuild feed to add option — takes ~2s, happens once only
                    _start_feed(option_security_id=option["security_id"])
                    spot, premium = get_spot_and_option_ltp(option["security_id"])
                    place_buy_order(option["security_id"], option["symbol"])
                    pos.open("CE", option["security_id"], option["symbol"], premium, spot)
                    print("\n\n Entry premium is CE: ", premium, f" & SPOT is {spot} \n\n")

                elif spot <= sell_trigger:
                    log.info(f"SELL SIGNAL: spot {spot} <= {sell_trigger}")
                    option = get_atm_option(spot, "PE")
                    # Rebuild feed to add option — takes ~2s, happens once only
                    _start_feed(option_security_id=option["security_id"])
                    spot, premium = get_spot_and_option_ltp(option["security_id"])
                    place_buy_order(option["security_id"], option["symbol"])
                    pos.open("PE", option["security_id"], option["symbol"], premium, spot)
                    print("\n\n Entry premium is PE: ", premium, f" & SPOT is {spot} \n\n")

                else:
                    log.info(
                        f"Watching | spot={spot:.2f} "
                        f"| CE trigger>={buy_trigger} | PE trigger<={sell_trigger}"
                    )

            # ── Active CE position: check exits ───────────────────────────────
            elif pos.active and pos.option_type == "CE":
                target_prem = pos.entry_premium + PREMIUM_TARGET_POINTS
                prem_mtm = prem - pos.entry_premium
                spot_mtm = spot - pos.entry_spot
                log.info(
                    f"CE | spot={spot:.2f} | prem={prem:.2f} "
                    f"| entry_prem={pos.entry_premium:.2f} "
                    f"| target_prem={target_prem:.2f} "
                    f"| T1={levels['t1']} | SL_spot={levels['sell']}"
                    f"| Premium MTM={prem_mtm:.2f} "
                    f"| Spot MTM ={spot_mtm:.2f} "
                )

                if spot >= levels["t1"]:
                    place_sell_order(pos.security_id, pos.symbol, f"T1 hit ({levels['t1']})")
                    print(
                        f"\n\n[CE EXIT - T1 Hit]  Entry Spot={pos.entry_spot:.2f}  Exit Spot={spot:.2f}"
                        f"  |  Entry Premium={pos.entry_premium:.2f}  Exit Premium={prem:.2f}\n")
                    pos.close(); trade_done = True
                    print("\n\n Points earned in Spot=",  spot - pos.entry_spot)
                    print("\n Points earned in premium=", round(prem, 2) - round(pos.entry_premium, 2), "\n")

                elif prem >= target_prem:
                    place_sell_order(pos.security_id, pos.symbol, f"Premium target ({target_prem:.2f})")
                    print(
                        f"\n\n[CE EXIT - Premium Target]  Entry Spot={pos.entry_spot:.2f}  Exit Spot={spot:.2f}"
                        f"  |  Entry Premium={pos.entry_premium:.2f}  Exit Premium={prem:.2f}\n")
                    pos.close(); trade_done = True
                    print("\n\n Points earned in Spot=",  spot - pos.entry_spot)
                    print("\n Points earned in premium=", round(prem, 2) - round(pos.entry_premium, 2), "\n")

                elif spot <= levels["sell"]:
                    place_sell_order(pos.security_id, pos.symbol, f"SL hit (S1={levels['sell']})")
                    print(
                        f"\n\n[CE EXIT - SL Hit]  Entry Spot={pos.entry_spot:.2f}  Exit Spot={spot:.2f}"
                        f"  |  Entry Premium={pos.entry_premium:.2f}  Exit Premium={prem:.2f}\n")
                    pos.close(); trade_done = True
                    print("\n\n Points BURNED in Spot=",  spot - pos.entry_spot)
                    print("\n Points BURNED in premium=", round(prem, 2) - round(pos.entry_premium, 2), "\n")

            # ── Active PE position: check exits ───────────────────────────────
            elif pos.active and pos.option_type == "PE":
                target_prem = pos.entry_premium + PREMIUM_TARGET_POINTS
                prem_mtm = prem - pos.entry_premium
                spot_mtm = pos.entry_spot - spot
                log.info(
                    f"PE | spot={spot:.2f} | prem={prem:.2f} "
                    f"| entry_prem={pos.entry_premium:.2f} "
                    f"| target_prem={target_prem:.2f} "
                    f"| S1={levels['s1']} | SL_spot={levels['buy']}"
                    f"| Premium MTM={prem_mtm:.2f} "
                    f"| Spot MTM ={spot_mtm:.2f} "
                )

                if spot <= levels["s1"]:
                    place_sell_order(pos.security_id, pos.symbol, f"S1 hit ({levels['s1']})")
                    print(
                        f"\n\n[PE EXIT - S1 Hit]  Entry Spot={pos.entry_spot:.2f}  Exit Spot={spot:.2f}"
                        f"  |  Entry Premium={pos.entry_premium:.2f}  Exit Premium={prem:.2f}\n")
                    pos.close(); trade_done = True
                    print("\n\n Points earned in Spot=",  pos.entry_spot - spot)
                    print("\n Points earned in premium=", round(prem, 2) - round(pos.entry_premium, 2), "\n")

                elif prem >= target_prem:
                    place_sell_order(pos.security_id, pos.symbol, f"Premium target ({target_prem:.2f})")
                    print(
                        f"\n\n[PE EXIT - Premium Target]  Entry Spot={pos.entry_spot:.2f}  Exit Spot={spot:.2f}"
                        f"  |  Entry Premium={pos.entry_premium:.2f}  Exit Premium={prem:.2f}\n")
                    pos.close(); trade_done = True
                    print("\n\n Points earned in Spot=",  pos.entry_spot - spot)
                    print("\n Points earned in premium=", round(prem, 2) - round(pos.entry_premium, 2), "\n")

                elif spot >= levels["buy"]:
                    place_sell_order(pos.security_id, pos.symbol, f"SL hit (T1={levels['buy']})")
                    print(
                        f"\n\n[PE EXIT - SL Hit]  Entry Spot={pos.entry_spot:.2f}  Exit Spot={spot:.2f}"
                        f"  |  Entry Premium={pos.entry_premium:.2f}  Exit Premium={prem:.2f}\n")
                    pos.close(); trade_done = True
                    print("\n\n Points BURNED in Spot=",  pos.entry_spot - spot)
                    print("\n Points BURNED in premium=", round(prem, 2) - round(pos.entry_premium, 2), "\n")

        except KeyboardInterrupt:
            log.info("Manual interrupt received.")
            if pos.active:
                log.warning("Closing open position before exit ...")
                place_sell_order(pos.security_id, pos.symbol, "Manual interrupt")
            break

        except Exception as exc:
            log.error(f"Error in loop: {exc}", exc_info=True)

        if trade_done:
            _stop_feed()
            close_summary = get_candle_close(CANDLE_HOUR, CANDLE_MINUTE)
            print_levels(levels, close_summary, candle_label)
            print("Today's trading done.....")
            break
        else:
            time.sleep(POLL_INTERVAL_SEC)

    # ── End-of-day: square off any open position ───────────────────────────────
    _stop_feed()
    if pos.active:
        log.warning("Market closing. Squaring off open position.")
        try:
            place_sell_order(pos.security_id, pos.symbol, "EOD square-off")
        except Exception as e:
            log.error(f"EOD square-off failed: {e}")

    log.info("Bot session complete.")
# At the end (optional, Python closes on exit anyway)


# ═════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        run_bot()
    finally:
        _stop_feed()
        if _feed_thread and _feed_thread.is_alive():
            _feed_thread.join(timeout=3)   # wait up to 3s for clean exit
        # restore stdout/stderr before closing the log
        sys.stdout = _console_out
        sys.stderr = _console_err
        log_file.close()