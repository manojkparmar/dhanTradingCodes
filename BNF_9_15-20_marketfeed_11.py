"""
╔══════════════════════════════════════════════════════════════╗
║         BankNifty Options Trading Bot — Dhan Broker          ║
║  Strategy: Square-Root Level Breakout with ATM CE/PE Entry   ║
╚══════════════════════════════════════════════════════════════╝

Flow:
  1. Wait for 9:15 AM (market open), then wait for chosen candle to fully close
  2. Fetch the 5-min BankNifty SPOT candle close and calculate buy/sell levels
     using the square-root formula
  3. Start MarketFeed in a background thread; monitor live spot every second:
       • Spot >= Buy + 2  → BUY ATM CE (long call)
       • Spot <= Sell - 2 → BUY ATM PE (long put)
  4. Once a position is open, check exit conditions every tick:
       CE exits: Spot >= T1  OR  CE_premium >= entry_premium + 25  OR  Spot <= Sell (SL)
       PE exits: Spot <= S1  OR  PE_premium >= entry_premium + 25  OR  Spot >= Buy  (SL)
  5. Only one trade is allowed per session. After exit, bot prints summary and stops.
  6. On Ctrl+C, EOD, or hard crash: the bot queries Dhan's live positions via
     get_positions() before placing any sell. If the position was already closed
     manually from the Dhan app (netQty == 0), no sell order is placed, preventing
     an accidental short. Only if netQty > 0 does the bot place a market SELL to exit.

Requirements:
    pip install dhanhq pandas python-dotenv

Setup:
    Create a .env file in the script directory with:
        DHAN_CLIENT_ID=your_client_id
        DHAN_ACCESS_TOKEN=your_access_token
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

# ── Log file setup ─────────────────────────────────────────────────────────────
# Every print() and log line is written to BOTH the terminal and a timestamped
# .out file under run_log/ so the full session output is always saved to disk.
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = "run_log/" + f"bnf_{timestamp}.out"

class _Tee:
    """
    Duplicates every write to all given streams simultaneously.
    Used to mirror stdout/stderr to both the terminal and the log file.
    Silently ignores write errors during shutdown (e.g. when the file is closing).
    """
    def __init__(self, *streams):
        self._streams = streams

    def write(self, data):
        for s in self._streams:
            try:
                if not getattr(s, 'closed', False):
                    s.write(data)
                    s.flush()
            except Exception:
                pass

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

# Redirect stdout and stderr through the Tee so all output goes to terminal + file
sys.stdout = _Tee(_console_out, log_file)
sys.stderr = _Tee(_console_err, log_file)

print("Job started...")

# ── Logging ───────────────────────────────────────────────────────────────────
# Two handlers: StreamHandler writes to the Tee (terminal + .out file),
# FileHandler writes a separate structured .log file under logs/.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"logs/bnf_bot_{timestamp}.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("BNBot")

IST = ZoneInfo("Asia/Kolkata")

# ── Credentials ───────────────────────────────────────────────────────────────
# Searches for .env in three locations in order:
#   1. Current working directory (wherever you ran `python` from)
#   2. Same folder as this .py file
#   3. Hardcoded fallback path (update if needed)
# The first .env file found is loaded; the search stops there.

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_ENV_PATHS = [
    os.path.join(os.getcwd(), ".env"),
    os.path.join(_SCRIPT_DIR, ".env"),
    "C:/Trading/AlgoTrading/.env"
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

# ── SDK initialisation ─────────────────────────────────────────────────────────
# dhanhq v2 requires a DhanContext wrapper; v1 accepted credentials directly.
# We try v2 first and fall back to v1 if DhanContext is not importable.
try:
    from dhanhq import DhanContext

    dhan_ctx = DhanContext(CLIENT_ID, ACCESS_TOKEN)
    dhan = dhanhq(dhan_ctx)
    print("[INFO] dhanhq SDK v2 initialised via DhanContext.")
except ImportError:
    dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
    print("[INFO] dhanhq SDK v1 initialised.")

# ── Account summary ────────────────────────────────────────────────────────────
# Print key balance fields at startup so you can verify the correct account
# is connected before any orders are placed.
try:
    balance_info = dhan.get_fund_limits()
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

BANKNIFTY_SECURITY_ID   = "25"      # Dhan security ID for the BankNifty index
BANKNIFTY_INDEX_EXCH    = "IDX_I"   # Exchange segment for index spot data
BANKNIFTY_FNO_EXCH      = "NSE_FNO" # Exchange segment for F&O orders
INSTRUMENT_INDEX        = "INDEX"
INSTRUMENT_OPTIDX       = "OPTIDX"

LOT_SIZE                = 30    # BankNifty lot size — verify before trading
POLL_INTERVAL_SEC       = 1     # Seconds to sleep between each price poll
PREMIUM_TARGET_POINTS   = 25    # Exit when option premium gains 25 pts from entry
PREMIUM_STOPLOSS_POINTS = 25    # Exit when option premium gains 25 pts from entry
ENTRY_BUFFER            = 1     # Extra buffer: enter CE only if spot >= buy+1, PE if spot <= sell-1
ORDER_TYPE              = "MARKET"
PRODUCT                 = "INTRADAY"
SUMMARY                 = ""

# ── Candle time configuration ─────────────────────────────────────────────────
# Controls which 5-minute BankNifty candle close is used to compute levels.
# Only CANDLE_HOUR and CANDLE_MINUTE need to be changed — no other code changes.
#
#   Examples:
#     9:15  → CANDLE_HOUR = 9,  CANDLE_MINUTE = 15   (market open candle)
#    10:00  → CANDLE_HOUR = 10, CANDLE_MINUTE = 0
#    11:30  → CANDLE_HOUR = 11, CANDLE_MINUTE = 30
#    14:00  → CANDLE_HOUR = 14, CANDLE_MINUTE = 0
#
# CANDLE_MINUTE must be a multiple of 5 (0, 5, 10, ... 55).
# The bot waits until CANDLE_HOUR:(CANDLE_MINUTE + 5) before fetching,
# ensuring the candle is fully closed.
# ─────────────────────────────────────────────────────────────────────────────
CANDLE_HOUR   = 9   # ← change this
CANDLE_MINUTE = 15  # ← change this  (must be multiple of 5)

"""
Architecture — MarketFeed background thread:

  MarketFeed's run_forever() is a blocking WebSocket call. Running it directly
  in the main loop would freeze the loop for seconds per call, causing large
  gaps between price checks.

  Instead, a single daemon thread runs run_forever() continuously in the
  background (_bg_feed_loop). Every incoming tick is stored in _ltp_prices
  (a dict protected by a threading.Lock). The main trading loop reads from
  that dict instantly — no blocking, no delay, no repeated reconnects.

  Feed lifecycle:
    _start_feed()              → called at startup; subscribes to BankNifty spot only
    _start_feed(option_id)     → called once after entry; rebuilds feed to add the option
    _stop_feed()               → called after trade completes, on EOD, or on any exit
"""


# ═════════════════════════════════════════════════════════════════════════════
# LEVEL CALCULATOR
# ═════════════════════════════════════════════════════════════════════════════

def calculate_levels(cmp: float) -> dict:
    """
    Compute buy/sell/target/stop levels around the given CMP using the
    square-root formula. Returns a dict with keys:
      buy, t1, t2, t3  — upside levels (buy trigger and targets)
      sell, s1, s2, s3 — downside levels (sell trigger and stops)
    """
    r1 = math.floor(math.sqrt(cmp)) - 1

    def level(n: int) -> float:
        return round((r1 + n * 0.125) ** 2, 0)

    buy_step  = next(n for n in range(1, 200) if level(n) > cmp)
    sell_step = next(n for n in range(buy_step - 1, 0, -1) if level(n) < cmp)

    return {
        "buy":  level(buy_step),
        "t1":   level(buy_step + 1),
        "t2":   level(buy_step + 2),
        "t3":   level(buy_step + 3),
        "sell": level(sell_step),
        "s1":   level(sell_step - 1),
        "s2":   level(sell_step - 2),
        "s3":   level(sell_step - 3),
    }


# ═════════════════════════════════════════════════════════════════════════════
# MARKET DATA HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def get_candle_close(hour: int = CANDLE_HOUR, minute: int = CANDLE_MINUTE) -> float:
    """
    Fetch today's 5-min BankNifty candle for the given hour:minute and return
    its close price. Called twice per session: once at startup for level
    calculation and once after trade completion for the end-of-trade summary.

    Args:
        hour   : candle start hour in IST   (default: CANDLE_HOUR)
        minute : candle start minute in IST (default: CANDLE_MINUTE)
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

    # SDK v1 nests data under resp["data"]; v2 returns the lists at the top level
    if isinstance(resp, dict) and "data" in resp:
        data = resp["data"]
    else:
        data = resp

    if not data or not data.get("timestamp"):
        raise ValueError(f"Empty or unexpected candle response: {resp}")

    # SDK v2 returns timestamps as epoch seconds (int); v1 may return ISO strings
    timestamps = data["timestamp"]
    try:
        ts_series = pd.to_datetime(timestamps, unit="s", utc=True)
    except Exception:
        ts_series = pd.to_datetime(timestamps, utc=True)

    df = pd.DataFrame({
        "timestamp": ts_series,
        "open":  pd.to_numeric(data["open"],  errors="coerce"),
        "high":  pd.to_numeric(data["high"],  errors="coerce"),
        "low":   pd.to_numeric(data["low"],   errors="coerce"),
        "close": pd.to_numeric(data["close"], errors="coerce"),
    })

    df.set_index("timestamp", inplace=True)
    df.index = df.index.tz_convert(IST)
    df.sort_index(inplace=True)

    log.info(
        f"Received {len(df)} 5-min candles. "
        f"Range: {df.index[0].strftime('%H:%M')} → {df.index[-1].strftime('%H:%M')}"
    )

    # Resample to normalise any gaps in the returned 5-min bars
    df_5 = df.resample("5min", label="left", closed="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}
    ).dropna()

    # Match by hour and minute to avoid timezone-offset edge cases
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
# MARKETFEED — BACKGROUND THREAD
# ═════════════════════════════════════════════════════════════════════════════

_LTP_KEYS         = ("LTP", "last_price", "ltp")  # accepted field names from Dhan tick
_ltp_prices: dict = {}       # security_id (str) → latest float LTP, updated by bg thread
_ltp_timestamps: dict = {}   # security_id (str) → monotonic time of last tick received
_ltp_lock         = threading.Lock()

_feed_instance        = None   # currently active MarketFeed object
_feed_thread          = None   # background daemon thread running the feed
_feed_subscribed_ids  = set()  # set of security_ids the current feed is subscribed to
_feed_stop_event      = threading.Event()  # signal to ask the bg thread to stop cleanly

_MAX_429_RETRIES  = 5   # max retries when Dhan returns HTTP 429 (rate limit)
_BASE_BACKOFF_SEC = 5   # initial back-off seconds; doubles on each retry (exponential)

# Module-level reference to the Position object created in run_bot().
# Assigned immediately when pos is created so that the __main__ finally block
# can check whether this script has an open position on any hard exit or crash,
# without needing to pass it through function arguments.
_active_position: "Position | None" = None


def _bg_feed_loop(feed: "MarketFeed", stop_event: threading.Event):
    """
    Runs in a background daemon thread. Calls feed.run_forever() in a tight
    loop to keep the WebSocket alive and process incoming ticks continuously.

    Each tick is extracted from feed.get_data() and stored in _ltp_prices
    so the main trading loop can read the latest price with no blocking.

    The loop exits cleanly when stop_event is set (triggered by _stop_feed()).
    On unexpected errors it logs a warning and retries after a short pause.
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
                break   # normal shutdown — suppress the error
            log.warning(f"[BG-FEED] tick error (will retry): {exc}")
            time.sleep(0.5)


def _start_feed(option_security_id: str | None = None):
    """
    Build a MarketFeed subscribed to BankNifty spot and (optionally) an option,
    then start the background thread.

    Called twice per session:
      1. At startup with no argument  → subscribes to spot only (security_id="25")
      2. After a BUY signal           → rebuilds the feed to also include the option

    Uses Quote subscription (not Ticker) so prices update on every bid/ask
    change, not just on matched trades.

    Retries up to _MAX_429_RETRIES times with exponential back-off if Dhan
    returns a 429 rate-limit error on connect.
    """
    global _feed_instance, _feed_thread, _feed_subscribed_ids, _feed_stop_event

    instruments = [(MarketFeed.IDX, "25", MarketFeed.Quote)]
    if option_security_id:
        instruments.append(
            (MarketFeed.NSE_FNO, str(option_security_id), MarketFeed.Quote)
        )
    requested_ids = {str(inst[1]) for inst in instruments}

    # Already subscribed to the exact same set — nothing to rebuild
    if requested_ids == _feed_subscribed_ids and _feed_thread and _feed_thread.is_alive():
        return

    # Stop any existing feed before creating a new one
    _stop_feed()

    # Create new MarketFeed with exponential back-off on 429
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

    stop_evt             = threading.Event()
    _feed_stop_event     = stop_evt
    _feed_instance       = feed
    _feed_subscribed_ids = requested_ids

    t = threading.Thread(
        target=_bg_feed_loop,
        args=(feed, stop_evt),
        daemon=True,      # thread dies automatically when the main thread exits
        name="MarketFeedBG",
    )
    t.start()
    _feed_thread = t

    # Allow 2 seconds for the first ticks to arrive before the caller reads prices
    log.info("MarketFeed background thread started. Warming up (2s) ...")
    time.sleep(2)
    log.info(f"Feed ready. Prices so far: { {k: v for k, v in _ltp_prices.items()} }")


def _stop_feed():
    """
    Signal the background feed thread to stop, wait for it to finish,
    disconnect the WebSocket, and cancel any leftover asyncio tasks
    to prevent 'Task was destroyed but it is pending!' warnings.
    """
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
        # Cancel pending asyncio tasks left in the feed's event loop
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
            pass  # best-effort cleanup — never let this crash the bot

    _feed_instance       = None
    _feed_thread         = None
    _feed_subscribed_ids = set()
    _feed_stop_event     = threading.Event()


def _read_ltp(security_id: str, max_age_sec: float = 10.0) -> float:
    """
    Read the latest LTP for a given security_id from the shared price dict.

    Raises RuntimeError if:
      - No price has ever been received for this security_id (feed still warming up), OR
      - The most recent price is older than max_age_sec (feed may be stalled)

    max_age_sec=10 is intentionally conservative — at Quote level the BankNifty
    index updates multiple times per second during market hours.
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
    Return (spot_ltp, option_ltp) read instantly from the shared price dict.
    Both values are updated continuously by the background feed thread,
    so this call never blocks and carries no 429 risk.
    """
    spot = _read_ltp("25")
    opt  = _read_ltp(str(security_id))
   # print(f"\n [BATCH] SPOT={spot}  |  Option LTP={opt} \n")
    return spot, opt


# ═════════════════════════════════════════════════════════════════════════════
# OPTION CHAIN HELPER
# ═════════════════════════════════════════════════════════════════════════════

def get_atm_option(spot: float, option_type: str) -> dict:
    """
    Fetch the nearest-expiry BankNifty option chain and return the ATM
    (or slightly ITM) contract for the requested option_type ("CE" or "PE").

    Strike selection:
      - Round spot to nearest 100 to get the base ATM strike.
      - For CE: if rounded strike is above spot, step down 100 (ITM side).
      - For PE: if rounded strike is below spot, step up  100 (ITM side).

    Returns a dict with: security_id, strike, expiry, symbol.
    """
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

    # Uncomment below lines for debugging the option chain structure:
    #  print(list(oc.keys())[:10])
    #  print(f"Looking for: {atm_strike}, type: {type(atm_strike)} \n\n ")
    #  print(list(oc.keys())[:5],"\n")
    #  print(list(oc.values())[:1],"\n\n")
    #  print("type(chain_data):\n")
    #  print(type(chain_data))
    #  print("\n\nlist(chain_data.keys())[:5]\n")
    #  print(list(chain_data.keys())[:5])
    #  print("\n\nlist(chain_data.values())[:1]\n")
    #  print(list(chain_data.values())[:1])

    if not chain_data:
        raise ValueError("Empty option chain.")

    # Dhan's option chain uses float-formatted strike keys, e.g. "53400.000000"
    strike_key  = f"{float(atm_strike):.6f}"
    strike_data = oc.get(strike_key, {})

    # option_type "CE" maps to key "ce", "PE" maps to key "pe"
    contract = strike_data.get(option_type.lower())

    if contract:
        log.info(
            f"ATM {option_type} found: strike={atm_strike}, "
            f"expiry={bf_latest_exp}, "
            f"security_id={contract['security_id']}"
        )
        return {
            "security_id": str(contract["security_id"]),
            "strike":      atm_strike,
            "expiry":      bf_latest_exp,
            "symbol":      contract.get("tradingSymbol", ""),
        }

    log.warning(f"ATM {option_type} not found for strike={atm_strike} in option chain.")
    return None


# ═════════════════════════════════════════════════════════════════════════════
# ORDER MANAGEMENT
# ═════════════════════════════════════════════════════════════════════════════

def _place_order(security_id: str, txn_type: str) -> str:
    """
    Internal helper: place a MARKET INTRADAY order on NSE_FNO and return
    the order_id. txn_type must be "BUY" or "SELL".
    Raises RuntimeError if Dhan returns a non-success status.
    """
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
    """Place a market BUY (entry) order and return the order_id."""
    log.info(f"Placing BUY order → {symbol} ({security_id}), qty={LOT_SIZE}")
    order_id = _place_order(security_id, "BUY")
    log.info(f"BUY order placed. order_id={order_id}")
    return order_id


def has_open_long_on_exchange(security_id: str) -> bool:
    """
    Query Dhan's live positions via get_positions() and check whether this
    script's option still has an open long (netQty > 0).

    This is the key safety check that prevents accidental short sells:
    if the position was manually closed from the Dhan app before this script
    triggers its own exit, netQty will be 0 and this function returns False,
    causing place_sell_order() to skip the sell entirely.

    netQty field per Dhan API docs:
      > 0  → still long (position open)
      = 0  → flat (position already closed, manually or by a prior exit)
      < 0  → short (should never occur in this script's flow)

    On API call failure, returns True (assumes position is open) so the bot
    still attempts the exit rather than silently skipping it.
    """
    try:
        resp = dhan.get_positions()
        positions = []
        if isinstance(resp, dict):
            positions = resp.get("data") or resp.get("positions") or []
        elif isinstance(resp, list):
            positions = resp

        for p in positions:
            pid = str(p.get("securityId", p.get("security_id", "")))
            if pid != str(security_id):
                continue
            # netQty confirmed as the correct field name per Dhan API documentation
            net_qty = int(p.get("netQty", 0))
            if net_qty > 0:
                return True
        return False

    except Exception as e:
        log.error(f"has_open_long_on_exchange check failed: {e}. Assuming position is open.")
        return True


def place_sell_order(security_id: str, symbol: str, reason: str) -> str:
    """
    Place a market SELL (exit) order — but ONLY after confirming via
    has_open_long_on_exchange() that an open long still exists on the exchange.

    If netQty == 0 (position already closed manually from the Dhan app or
    another terminal), the sell is skipped and an empty string is returned.
    This prevents creating an accidental naked short.

    All exit paths (T1, SL, premium target, Ctrl+C, EOD, hard crash) call
    this function, so the exchange check applies uniformly everywhere.
    """
    if not has_open_long_on_exchange(security_id):
        log.warning(
            f"EXIT [{reason}] skipped — no open long found on exchange for "
            f"{symbol} ({security_id}). Position may have been closed manually."
        )
        return ""
    log.info(f"EXIT [{reason}] → {symbol} ({security_id}), qty={LOT_SIZE}")
    order_id = _place_order(security_id, "SELL")
    log.info(f"SELL order placed. order_id={order_id}")
    return order_id


# ═════════════════════════════════════════════════════════════════════════════
# POSITION STATE
# ═════════════════════════════════════════════════════════════════════════════

class Position:
    """
    Tracks the state of the single trade this script is allowed per session.

    active       : True while a BUY order has been placed and not yet exited
    option_type  : "CE" or "PE"
    security_id  : Dhan security_id of the option contract
    symbol       : trading symbol (e.g. BANKNIFTY2561050000CE)
    entry_premium: option LTP at the time of entry
    entry_spot   : BankNifty spot at the time of entry
    entry_time   : IST time string when the position was opened

    Note: active is a local in-memory flag only. It does NOT reflect real-time
    exchange state. Use has_open_long_on_exchange() before placing any SELL to
    verify the position is still open on Dhan's side.
    """
    def __init__(self):
        self.active: bool         = False
        self.option_type: str     = ""  # "CE" or "PE"
        self.security_id: str     = ""
        self.symbol: str          = ""
        self.entry_premium: float = 0.0
        self.entry_spot: float    = 0.0
        self.entry_time: str      = ""

    def open(self, option_type, security_id, symbol, premium, spot):
        """Mark the position as open and record all entry details."""
        self.active        = True
        self.option_type   = option_type
        self.security_id   = security_id
        self.symbol        = symbol
        self.entry_premium = premium
        self.entry_spot    = spot
        self.entry_time    = datetime.now(IST).strftime("%H:%M:%S")
        log.info(
            f"Position OPENED | {option_type} | {symbol} | "
            f"entry_premium={premium} | spot={spot}"
        )

    def close(self):
        """Mark the position as closed in local state."""
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
    """Print a formatted table of buy/sell levels and targets to the console."""
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
    Block until 5 minutes after the candle's start time so the candle is
    fully closed before it is fetched.

    Examples:
      CANDLE_HOUR=9,  CANDLE_MINUTE=15 → waits until 9:20 IST
      CANDLE_HOUR=10, CANDLE_MINUTE=0  → waits until 10:05 IST
    """
    now = datetime.now(IST)

    ready_minute = minute + 5
    ready_hour   = hour + ready_minute // 60
    ready_minute = ready_minute % 60

    target       = now.replace(hour=ready_hour, minute=ready_minute, second=0, microsecond=0)
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
    """Block until 9:15 AM IST when the NSE exchange opens."""
    now = datetime.now(IST)
    target = now.replace(hour=9, minute=15, second=0, microsecond=0)
    if now < target:
        wait_sec = (target - now).total_seconds()
        log.info(f"Market opens in {wait_sec:.0f}s. Sleeping ...")
        time.sleep(wait_sec)


def is_market_open() -> bool:
    """Return True if current IST time is within the trading window (9:15–15:25)."""
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

    # ── Step 1: Wait for market open, then for chosen candle to fully close ───
    wait_until_market_open()
    wait_until_candle_ready(CANDLE_HOUR, CANDLE_MINUTE)

    # ── Step 2: Fetch candle close and compute buy/sell levels ─────────────────
    close  = get_candle_close(CANDLE_HOUR, CANDLE_MINUTE)
    levels = calculate_levels(close)

    print_levels(levels, close, candle_label)

    buy_trigger  = levels["buy"]  + ENTRY_BUFFER
    sell_trigger = levels["sell"] - ENTRY_BUFFER

    log.info(f"Entry triggers → CE if spot >= {buy_trigger} | PE if spot <= {sell_trigger}")

    # ── Step 3: Start MarketFeed background thread (spot only) ────────────────
    # The feed runs continuously in a daemon thread. After entry the feed is
    # rebuilt once to include the option. Main loop reads prices with no blocking.
    _start_feed()

    # ── Step 4: Main monitoring loop ───────────────────────────────────────────
    global _active_position
    pos = Position()
    _active_position = pos   # expose to __main__ finally block and signal handlers
    trade_done = False       # only one trade is allowed per session

    while is_market_open():
        try:
            if pos.active:
                spot, prem = get_spot_and_option_ltp(pos.security_id)
            else:
                spot = get_live_spot()

            time.sleep(1)

            # ── No open position: wait for a breakout entry signal ─────────────
            if not pos.active and not trade_done:

                if spot >= buy_trigger:
                    log.info(f"BUY SIGNAL: spot {spot} >= {buy_trigger}")
                    option = get_atm_option(spot, "CE")
                    # Rebuild feed to include the option — takes ~2s, happens once only
                    _start_feed(option_security_id=option["security_id"])
                    spot, premium = get_spot_and_option_ltp(option["security_id"])
                    place_buy_order(option["security_id"], option["symbol"])
                    pos.open("CE", option["security_id"], option["symbol"], premium, spot)
                    print("\n\n Entry premium is CE: ", premium, f" & SPOT is {spot} \n\n")

                elif spot <= sell_trigger:
                    log.info(f"SELL SIGNAL: spot {spot} <= {sell_trigger}")
                    option = get_atm_option(spot, "PE")
                    # Rebuild feed to include the option — takes ~2s, happens once only
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

            # ── Active CE position: monitor exit conditions ────────────────────
            # Exits (in priority order):
            #   1. Spot >= T1           → target hit (spot-based)
            #   2. Premium >= entry+25  → premium target hit
            #   3. Spot <= Sell level   → stop-loss hit
            # Before each sell, has_open_long_on_exchange() verifies the position
            # is still open on Dhan. If closed manually, the sell is skipped.
            elif pos.active and pos.option_type == "CE":
                target_prem = pos.entry_premium + PREMIUM_TARGET_POINTS
                sl_prem = pos.entry_premium - PREMIUM_STOPLOSS_POINTS
                prem_mtm = prem - pos.entry_premium
                spot_mtm = spot - pos.entry_spot
                log.info(
                    f"CE | spot={spot:.2f} | prem={prem:.2f} "
                    f"| entr_prem={pos.entry_premium:.2f} "
                    f"| tgt_prem={target_prem:.2f} "
                    f"| sl_prem={sl_prem:.2f} "
                    f"| T1={levels['t1']} | SL_spot={levels['sell']}"
                    f"| Spot MTM ={spot_mtm:.2f} "
                    f"| Prem MTM={prem_mtm:.2f} "
                )

                if spot >= levels["t1"]:
                    place_sell_order(pos.security_id, pos.symbol, f"T1 hit ({levels['t1']})")
                    print(
                        f"\n\n[CE EXIT - T1 Hit]  Entry Spot={pos.entry_spot:.2f}  Exit Spot={spot:.2f}"
                        f"  |  Entry Premium={pos.entry_premium:.2f}  Exit Premium={prem:.2f}\n")
                    pos.close(); trade_done = True
                    print("\n\n Points earned in Spot=",  spot - pos.entry_spot)
                    print("\n Points earned in premium=", round(prem, 2) - round(pos.entry_premium, 2), "\n")
                    print("\n Total Profit =", LOT_SIZE * (round(prem, 2) - round(pos.entry_premium, 2)), "\n")

                elif prem >= target_prem:
                    place_sell_order(pos.security_id, pos.symbol, f"Premium target ({target_prem:.2f})")
                    print(
                        f"\n\n[CE EXIT - Premium Target]  Entry Spot={pos.entry_spot:.2f}  Exit Spot={spot:.2f}"
                        f"  |  Entry Premium={pos.entry_premium:.2f}  Exit Premium={prem:.2f}\n")
                    pos.close(); trade_done = True
                    print("\n\n Points earned in Spot=",  spot - pos.entry_spot)
                    print("\n Points earned in premium=", round(prem, 2) - round(pos.entry_premium, 2), "\n")
                    print("\n Total Profit =", LOT_SIZE * (round(prem, 2) - round(pos.entry_premium, 2)), "\n")

                elif spot <= levels["sell"]:
                    place_sell_order(pos.security_id, pos.symbol, f"SL hit (S1={levels['sell']})")
                    print(
                        f"\n\n[CE EXIT - SL Hit]  Entry Spot={pos.entry_spot:.2f}  Exit Spot={spot:.2f}"
                        f"  |  Entry Premium={pos.entry_premium:.2f}  Exit Premium={prem:.2f}\n")
                    pos.close(); trade_done = True
                    print("\n\n Points BURNED in Spot=",  spot - pos.entry_spot)
                    print("\n Points BURNED in premium=", round(prem, 2) - round(pos.entry_premium, 2), "\n")
                    print("\n Total Loss =", LOT_SIZE*(round(prem, 2) - round(pos.entry_premium, 2)), "\n")

                elif prem <= sl_prem:
                    place_sell_order(pos.security_id, pos.symbol, f"Premium SL Hit ({sl_prem:.2f})")
                    print(
                        f"\n\n[CE EXIT - SL Hit]  Entry Spot={pos.entry_spot:.2f}  Exit Spot={spot:.2f}"
                        f"  |  Entry Premium={pos.entry_premium:.2f}  Exit Premium={prem:.2f}\n")
                    pos.close(); trade_done = True
                    print("\n\n Points BURNED in Spot=",  spot - pos.entry_spot)
                    print("\n Points BURNED in premium=", round(prem, 2) - round(pos.entry_premium, 2), "\n")
                    print("\n Total Loss =", LOT_SIZE*(round(prem, 2) - round(pos.entry_premium, 2)), "\n")

            # ── Active PE position: monitor exit conditions ────────────────────
            # Exits (in priority order):
            #   1. Spot <= S1           → target hit (spot-based)
            #   2. Premium >= entry+25  → premium target hit
            #   3. Spot >= Buy level    → stop-loss hit
            # Before each sell, has_open_long_on_exchange() verifies the position
            # is still open on Dhan. If closed manually, the sell is skipped.
            elif pos.active and pos.option_type == "PE":
                target_prem = pos.entry_premium + PREMIUM_TARGET_POINTS
                sl_prem = pos.entry_premium - PREMIUM_STOPLOSS_POINTS
                prem_mtm = prem - pos.entry_premium
                spot_mtm = pos.entry_spot - spot
                log.info(
                    f"PE | spot={spot:.2f} | prem={prem:.2f} "
                    f"| entr_prem={pos.entry_premium:.2f} "
                    f"| tgt_prem={target_prem:.2f} "
                    f"| sl_prem={sl_prem:.2f} "
                    f"| S1={levels['s1']} | SL_spot={levels['buy']}"
                    f"| Spot MTM ={spot_mtm:.2f} "
                    f"| Prem MTM={prem_mtm:.2f} "
                )

                if spot <= levels["s1"]:
                    place_sell_order(pos.security_id, pos.symbol, f"S1 hit ({levels['s1']})")
                    print(
                        f"\n\n[PE EXIT - S1 Hit]  Entry Spot={pos.entry_spot:.2f}  Exit Spot={spot:.2f}"
                        f"  |  Entry Premium={pos.entry_premium:.2f}  Exit Premium={prem:.2f}\n")
                    pos.close(); trade_done = True
                    print("\n\n Points earned in Spot=",  pos.entry_spot - spot)
                    print("\n Points earned in premium=", round(prem, 2) - round(pos.entry_premium, 2), "\n")
                    print("\n Total Profit =", LOT_SIZE * (round(prem, 2) - round(pos.entry_premium, 2)), "\n")

                elif prem >= target_prem:
                    place_sell_order(pos.security_id, pos.symbol, f"Premium target ({target_prem:.2f})")
                    print(
                        f"\n\n[PE EXIT - Premium Target]  Entry Spot={pos.entry_spot:.2f}  Exit Spot={spot:.2f}"
                        f"  |  Entry Premium={pos.entry_premium:.2f}  Exit Premium={prem:.2f}\n")
                    pos.close(); trade_done = True
                    print("\n\n Points earned in Spot=",  pos.entry_spot - spot)
                    print("\n Points earned in premium=", round(prem, 2) - round(pos.entry_premium, 2), "\n")
                    print("\n Total Profit =", LOT_SIZE * (round(prem, 2) - round(pos.entry_premium, 2)), "\n")

                elif spot >= levels["buy"]:
                    place_sell_order(pos.security_id, pos.symbol, f"SL hit (T1={levels['buy']})")
                    print(
                        f"\n\n[PE EXIT - SL Hit]  Entry Spot={pos.entry_spot:.2f}  Exit Spot={spot:.2f}"
                        f"  |  Entry Premium={pos.entry_premium:.2f}  Exit Premium={prem:.2f}\n")
                    pos.close(); trade_done = True
                    print("\n\n Points BURNED in Spot=",  pos.entry_spot - spot)
                    print("\n Points BURNED in premium=", round(prem, 2) - round(pos.entry_premium, 2), "\n")
                    print("\n Total Loss =", LOT_SIZE*(round(prem, 2) - round(pos.entry_premium, 2)), "\n")

                elif prem <= sl_prem:
                    place_sell_order(pos.security_id, pos.symbol, f"Premium SL Hit ({sl_prem:.2f})")
                    print(
                        f"\n\n[PE EXIT - SL Hit]  Entry Spot={pos.entry_spot:.2f}  Exit Spot={spot:.2f}"
                        f"  |  Entry Premium={pos.entry_premium:.2f}  Exit Premium={prem:.2f}\n")
                    pos.close(); trade_done = True
                    print("\n\n Points BURNED in Spot=",  spot - pos.entry_spot)
                    print("\n Points BURNED in premium=", round(prem, 2) - round(pos.entry_premium, 2), "\n")
                    print("\n Total Loss =", LOT_SIZE*(round(prem, 2) - round(pos.entry_premium, 2)), "\n")

        except KeyboardInterrupt:
            log.info("Manual interrupt received (Ctrl+C).")
            if pos.active:
                # Verify position is still open on exchange before selling.
                # If it was already closed manually via the Dhan app, the sell is skipped.
                log.warning("Checking exchange and closing position if still open ...")
                place_sell_order(pos.security_id, pos.symbol, "Manual interrupt")
                pos.close()
            else:
                log.info("No open position from this script — no sell order placed.")
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

    # ── End-of-day square-off ──────────────────────────────────────────────────
    # Reached when is_market_open() returns False (after 15:25 IST).
    # If pos.active is True the bot never received a normal exit signal,
    # so it attempts to square off. The sell is only placed if Dhan confirms
    # netQty > 0; if the position was closed manually it is silently skipped.
    _stop_feed()
    if pos.active:
        log.warning("Market closing. Attempting EOD square-off ...")
        try:
            place_sell_order(pos.security_id, pos.symbol, "EOD square-off")
            pos.close()
        except Exception as e:
            log.error(f"EOD square-off failed: {e}")
    else:
        log.info("EOD: no open position from this script — no sell order placed.")

    log.info("Bot session complete.")


# ═════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        run_bot()
    finally:
        # Hard-exit safety net: catches unhandled exceptions, sys.exit(), or
        # any other path that bypasses the normal loop exit.
        # Checks _active_position (set by run_bot) to decide whether a sell
        # is needed. Calls place_sell_order() which internally verifies
        # netQty > 0 on exchange, so no accidental short is ever created.
        _stop_feed()
        if _active_position is not None and _active_position.active:
            log.warning("Hard exit detected: attempting to close open position ...")
            try:
                place_sell_order(
                    _active_position.security_id,
                    _active_position.symbol,
                    "Hard exit (finally block)",
                )
                _active_position.close()
            except Exception as e:
                log.error(f"Hard-exit square-off failed: {e}")
        else:
            log.info("Hard exit: no open position from this script — no sell order placed.")
        if _feed_thread and _feed_thread.is_alive():
            _feed_thread.join(timeout=3)
        # Restore original stdout/stderr before closing the log file
        sys.stdout = _console_out
        sys.stderr = _console_err
        log_file.close()