"""
Dhan Broker - Nifty ATM Short Straddle Strategy with VWAP
==========================================================
Strategy Logic:
  1. At ENTRY_TIME (default 09:30), fetch Nifty Spot via websocket
  2. Find nearest expiry ATM strike → compute CE + PE LTP (straddle price)
  3. Compute VWAP of the straddle
  4. If straddle price < VWAP → Short the straddle
  5. Every 30 seconds: if straddle price > current VWAP → EXIT
     If same-strike straddle price falls back below VWAP → RE-ENTER
  6. Hard exit at EXIT_TIME (default 14:55)
  7. Ctrl+C → close all open positions gracefully

Requirements:
    pip install dhanhq python-dotenv
"""

import os
import time
import signal
import logging
import threading
from datetime import datetime, date, timedelta
from typing import Optional
from dotenv import load_dotenv

# ── DhanHQ SDK (v2 API) ──────────────────────────────────────────────────────
from dhanhq import dhanhq, DhanContext
from dhanhq.marketfeed import MarketFeed

# ── Load credentials from .env ───────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
for _p in [
    os.path.join(os.getcwd(), ".env"),
    os.path.join(_SCRIPT_DIR, ".env"),
    "C:/Trading/AlgoTrading/.env"  # add .env file's path manually
]:
    if os.path.isfile(_p):
        load_dotenv(_p, override=True)
        print(f"[INFO] Loaded credentials from: {_p}")
        break

# ════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION  (edit here or via .env / environment variables)
# ════════════════════════════════════════════════════════════════════════════

CLIENT_ID    = os.getenv("DHAN_CLIENT_ID",   "YOUR_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "YOUR_ACCESS_TOKEN")

# ── Timing (HH:MM, 24-hour) — change ENTRY_TIME in .env for custom time ──────
ENTRY_TIME = os.getenv("ENTRY_TIME", "09:30")
EXIT_TIME  = os.getenv("EXIT_TIME",  "14:55")

# ── Strategy parameters ───────────────────────────────────────────────────────
LOT_SIZE       = 65     # Nifty lot size (update if NSE revises)
QUANTITY       = 1      # Number of lots per leg
CHECK_INTERVAL = 30     # Seconds between VWAP/price checks

# ── MarketFeed exchange segment constants ─────────────────────────────────────
#   MarketFeed.IDX     = 0   → Index (Nifty spot)
#   MarketFeed.NSE     = 1   → NSE cash
#   MarketFeed.NSE_FNO = 2   → NSE F&O options
NIFTY_SECURITY_ID = "13"              # Nifty 50 index security_id on Dhan
NIFTY_EXCH_SEG    = MarketFeed.IDX   # use IDX segment for index spot feed

# ── Feed subscription type ────────────────────────────────────────────────────
#   MarketFeed.Ticker = 15  (LTP only — lightest, recommended)
#   MarketFeed.Quote  = 17  (LTP + bid/ask)
#   MarketFeed.Full   = 21  (full depth)
FEED_TYPE = MarketFeed.Ticker

# ════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ════════════════════════════════════════════════════════════════════════════

import sys
_stream_handler = logging.StreamHandler(stream=sys.stdout)
_stream_handler.stream.reconfigure(encoding="utf-8", errors="replace") if hasattr(_stream_handler.stream, "reconfigure") else None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        _stream_handler,
        logging.FileHandler("straddle_strategy.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ════════════════════════════════════════════════════════════════════════════
#  GLOBAL STATE
# ════════════════════════════════════════════════════════════════════════════

class StrategyState:
    def __init__(self):
        self.nifty_spot:     Optional[float] = None
        self.atm_strike:     Optional[int]   = None
        self.expiry_date:    Optional[str]   = None   # "YYYY-MM-DD"
        self.ce_security_id: Optional[str]   = None
        self.pe_security_id: Optional[str]   = None
        self.ce_ltp:         Optional[float] = None
        self.pe_ltp:         Optional[float] = None

        # VWAP accumulators
        self.vwap_pv:  float = 0.0
        self.vwap_vol: float = 0.0
        self.vwap:     Optional[float] = None

        # Position tracking
        self.in_position: bool = False
        self.entry_count: int  = 0
        self.ce_order_id: Optional[str] = None
        self.pe_order_id: Optional[str] = None

        self.lock     = threading.Lock()
        self.shutdown = False

state = StrategyState()

# ════════════════════════════════════════════════════════════════════════════
#  DHAN CLIENT  (SDK v2: requires DhanContext wrapper)
# ════════════════════════════════════════════════════════════════════════════

dhan_ctx = DhanContext(CLIENT_ID, ACCESS_TOKEN)
dhan     = dhanhq(dhan_ctx)

# ════════════════════════════════════════════════════════════════════════════
#  HELPER UTILITIES
# ════════════════════════════════════════════════════════════════════════════

def parse_time(hhmm: str) -> datetime:
    h, m = map(int, hhmm.split(":"))
    return datetime.now().replace(hour=h, minute=m, second=0, microsecond=0)


def get_nearest_tuesday() -> date:
    """Nifty weekly expiry is every Tuesday."""
    today = date.today()
    days_ahead = (1 - today.weekday()) % 7   # 1 = Tuesday
    if days_ahead == 0 and datetime.now().hour >= 15:
        days_ahead = 7    # today's expiry has passed
    return today + timedelta(days=days_ahead)


def round_to_strike(spot: float, step: int = 50) -> int:
    """Round spot to nearest ATM strike (multiples of 50)."""
    return int(round(spot / step) * step)


def straddle_price() -> Optional[float]:
    """Return CE + PE LTP (thread-safe)."""
    with state.lock:
        if state.ce_ltp is not None and state.pe_ltp is not None:
            return round(state.ce_ltp + state.pe_ltp, 2)
    return None


def update_vwap(price: float, volume: float = 1.0):
    """Accumulate running VWAP. Must be called inside state.lock."""
    state.vwap_pv  += price * volume
    state.vwap_vol += volume
    if state.vwap_vol > 0:
        state.vwap = round(state.vwap_pv / state.vwap_vol, 2)

# ════════════════════════════════════════════════════════════════════════════
#  OPTION CHAIN LOOKUP
# ════════════════════════════════════════════════════════════════════════════

def fetch_option_security_ids(strike: int, expiry: str):
    """
    Fetch CE and PE security_ids for a given Nifty strike + expiry.
    expiry format: "YYYY-MM-DD"
    """
    log.info(f"Fetching option chain | Strike={strike}  Expiry={expiry}")
    try:
        resp = dhan.option_chain(
            optionality = "CALL",
            underlying  = "NIFTY",
            expiry_date = expiry,
        )
        data  = resp.get("data", {})
        ce_id = None
        pe_id = None

        for row in data.get("oc", []):
            if int(row.get("strikePrice", 0)) == strike:
                ce_id = str(row["callOption"]["securityId"])
                pe_id = str(row["putOption"]["securityId"])
                break

        if ce_id and pe_id:
            log.info(f"  CE securityId={ce_id}   PE securityId={pe_id}")
        else:
            log.error(f"  security IDs not found for strike {strike}")

        return ce_id, pe_id

    except Exception as exc:
        log.error(f"option_chain API error: {exc}")
        return None, None

# ════════════════════════════════════════════════════════════════════════════
#  WEBSOCKET MARKET FEED
# ════════════════════════════════════════════════════════════════════════════

def on_tick(data: dict):
    """
    Callback for every market-feed tick.
    Common keys: 'security_id', 'LTP', 'volume'
    """
    try:
        sec_id = str(data.get("security_id", ""))
        ltp    = data.get("LTP") or data.get("last_price")
        vol    = float(data.get("volume") or data.get("vol") or 1)

        if ltp is None:
            return
        ltp = float(ltp)

        with state.lock:
            if sec_id == NIFTY_SECURITY_ID:
                state.nifty_spot = ltp

            elif sec_id == state.ce_security_id:
                state.ce_ltp = ltp

            elif sec_id == state.pe_security_id:
                state.pe_ltp = ltp
                if state.ce_ltp is not None:
                    update_vwap(state.ce_ltp + ltp, vol)

    except Exception as exc:
        log.debug(f"on_tick error: {exc}  raw={data}")


def on_connect(feed):
    log.info("Market feed WebSocket connected.")


def on_close(feed):
    log.warning("Market feed WebSocket closed.")


def on_error(feed, err):
    log.error(f"Market feed error: {err}")


_active_feed: Optional[MarketFeed] = None

def start_feed(instruments: list) -> MarketFeed:
    """
    Start (or restart) MarketFeed WebSocket.
    instruments = [(exchange_segment_int, security_id_str, feed_type_int), ...]

    Uses MarketFeed(dhan_context, instruments, version='v2', ...)
    """
    global _active_feed

    if _active_feed is not None:
        try:
            _active_feed.disconnect()
        except Exception:
            pass

    feed = MarketFeed(
        dhan_context = dhan_ctx,
        instruments  = instruments,
        version      = "v2",
        on_connect   = on_connect,
        on_ticks     = on_tick,     # maps to on_message internally
        on_close     = on_close,
        on_error     = on_error,
    )
    feed.run_forever()   # starts async event loop in a daemon thread
    _active_feed = feed
    return feed

# ════════════════════════════════════════════════════════════════════════════
#  ORDER MANAGEMENT
# ════════════════════════════════════════════════════════════════════════════

def place_sell_order(security_id: str, qty: int) -> Optional[str]:
    """Short sell at market (MARGIN intraday)."""
    try:
        resp = dhan.place_order(
            security_id      = security_id,
            exchange_segment = dhan.NSE_FNO,   # 'NSE_FNO'
            transaction_type = dhan.SELL,       # 'SELL'
            quantity         = qty,
            order_type       = dhan.MARKET,     # 'MARKET'
            product_type     = dhan.MARGIN,     # 'MARGIN' — intraday short
            price            = 0,
        )
        order_id = resp.get("data", {}).get("orderId")
        log.info(f"SELL placed | sec={security_id} qty={qty} orderId={order_id}")
        return str(order_id) if order_id else None
    except Exception as exc:
        log.error(f"place_sell_order error: {exc}")
        return None


def place_buy_order(security_id: str, qty: int) -> Optional[str]:
    """Buy back to cover a short leg."""
    try:
        resp = dhan.place_order(
            security_id      = security_id,
            exchange_segment = dhan.NSE_FNO,
            transaction_type = dhan.BUY,        # 'BUY'
            quantity         = qty,
            order_type       = dhan.MARKET,
            product_type     = dhan.MARGIN,
            price            = 0,
        )
        order_id = resp.get("data", {}).get("orderId")
        log.info(f"BUY  placed | sec={security_id} qty={qty} orderId={order_id}")
        return str(order_id) if order_id else None
    except Exception as exc:
        log.error(f"place_buy_order error: {exc}")
        return None


def enter_straddle():
    """Short both CE and PE at market."""
    qty = LOT_SIZE * QUANTITY
    log.info(f"▶ ENTERING Short Straddle | Strike={state.atm_strike} | Qty={qty}")
    ce_oid = place_sell_order(state.ce_security_id, qty)
    pe_oid = place_sell_order(state.pe_security_id, qty)
    if ce_oid and pe_oid:
        state.ce_order_id  = ce_oid
        state.pe_order_id  = pe_oid
        state.in_position  = True
        state.entry_count += 1
        log.info(
            f"✅ Straddle entered #{state.entry_count} | "
            f"Price={straddle_price()} | VWAP={state.vwap}"
        )
    else:
        log.error("❌ One or both legs failed to place.")


def exit_straddle(reason: str = ""):
    """Buy back CE and PE to close the straddle."""
    qty = LOT_SIZE * QUANTITY
    log.info(f"◀ EXITING straddle | Reason: {reason} | Qty={qty}")
    place_buy_order(state.ce_security_id, qty)
    place_buy_order(state.pe_security_id, qty)
    state.in_position = False
    log.info(f"✅ Straddle exited | Price={straddle_price()} | VWAP={state.vwap}")


def close_all_positions():
    """Emergency flatten — called on Ctrl+C."""
    log.warning("🚨 CLOSING ALL POSITIONS")
    if state.in_position:
        exit_straddle(reason="emergency/abort")
    else:
        log.info("No open position to close.")

# ════════════════════════════════════════════════════════════════════════════
#  SIGNAL HANDLER — Ctrl+C
# ════════════════════════════════════════════════════════════════════════════

def handle_sigint(signum, frame):
    log.warning("\n⌨  Keyboard interrupt — closing positions …")
    state.shutdown = True
    close_all_positions()
    log.info("Exited cleanly.")
    os._exit(0)

signal.signal(signal.SIGINT, handle_sigint)

# ════════════════════════════════════════════════════════════════════════════
#  WAIT HELPER
# ════════════════════════════════════════════════════════════════════════════

def wait_until(target: datetime, label: str):
    while True:
        remaining = (target - datetime.now()).total_seconds()
        if remaining <= 0:
            break
        log.info(f"⏳ {label} in {int(remaining // 60)}m {int(remaining % 60)}s …")
        time.sleep(min(60, remaining))

# ════════════════════════════════════════════════════════════════════════════
#  MAIN STRATEGY
# ════════════════════════════════════════════════════════════════════════════

def main():
    log.info("═" * 60)
    log.info("  Dhan Nifty ATM Short Straddle — VWAP Strategy")
    log.info(f"  Entry : {ENTRY_TIME}    Exit : {EXIT_TIME}")
    log.info(f"  Lots  : {QUANTITY}  x  Lot size : {LOT_SIZE}  =  Qty : {QUANTITY * LOT_SIZE}")
    log.info("═" * 60)

    entry_dt = parse_time(ENTRY_TIME)
    exit_dt  = parse_time(EXIT_TIME)

    # ── 1. Subscribe Nifty Spot immediately ─────────────────────────────────
    nifty_instruments = [
        (MarketFeed.IDX, NIFTY_SECURITY_ID, FEED_TYPE),
    ]
    start_feed(nifty_instruments)
    log.info("Subscribed to Nifty Spot (IDX) feed.")

    # ── 2. Wait until entry time ─────────────────────────────────────────────
    wait_until(entry_dt, f"Entry time ({ENTRY_TIME})")

    # ── 3. Snapshot Nifty Spot ───────────────────────────────────────────────
    time.sleep(2)
    with state.lock:
        spot = state.nifty_spot

    if spot is None:
        log.error("Nifty Spot not received — check credentials / market hours.")
        return

    log.info(f"Nifty Spot at {ENTRY_TIME} : {spot}")

    # ── 4. ATM strike & expiry ───────────────────────────────────────────────
    state.atm_strike  = round_to_strike(spot)
    nearest_tuesday   = get_nearest_tuesday()
    state.expiry_date = nearest_tuesday.strftime("%Y-%m-%d")
    log.info(f"ATM Strike : {state.atm_strike}")
    log.info(f"Expiry     : {state.expiry_date}  (nearest Tuesday)")

    # ── 5. Fetch option security IDs ─────────────────────────────────────────
    ce_id, pe_id = fetch_option_security_ids(state.atm_strike, state.expiry_date)
    if not ce_id or not pe_id:
        log.error("Security IDs unavailable — cannot continue.")
        return

    state.ce_security_id = ce_id
    state.pe_security_id = pe_id

    # ── 6. Subscribe option legs ─────────────────────────────────────────────
    option_instruments = [
        (MarketFeed.NSE_FNO, ce_id, FEED_TYPE),
        (MarketFeed.NSE_FNO, pe_id, FEED_TYPE),
    ]
    start_feed(nifty_instruments + option_instruments)
    log.info("Subscribed to CE + PE (NSE_FNO) feeds.")

    # ── 7. Warm-up: collect ticks for initial VWAP ───────────────────────────
    log.info("Collecting initial ticks for 5 seconds …")
    time.sleep(5)

    sp = straddle_price()
    if sp is None:
        log.error("No straddle price received — verify security IDs.")
        return

    log.info(f"Straddle Price : {sp}")
    log.info(f"VWAP           : {state.vwap}")

    # ── 8. Initial entry decision ────────────────────────────────────────────
    if state.vwap is not None and sp < state.vwap:
        log.info(f"Straddle {sp} < VWAP {state.vwap} → entering straddle")
        enter_straddle()
    else:
        log.info(f"Straddle {sp} >= VWAP {state.vwap} → waiting for drop below VWAP")

    # ── 9. Monitoring loop ───────────────────────────────────────────────────
    log.info(f"Monitoring every {CHECK_INTERVAL}s until {EXIT_TIME} …")

    while not state.shutdown:
        if datetime.now() >= exit_dt:
            log.info(f"{EXIT_TIME} — hard exit.")
            if state.in_position:
                exit_straddle(reason=f"{EXIT_TIME} EOD exit")
            break

        time.sleep(CHECK_INTERVAL)

        sp   = straddle_price()
        vwap = state.vwap

        if sp is None or vwap is None:
            log.warning("Price/VWAP unavailable — skipping check.")
            continue

        log.info(
            f"Straddle={sp:>8.2f}  VWAP={vwap:>8.2f}  "
            f"Pos={'IN' if state.in_position else 'OUT'}  "
            f"Trades={state.entry_count}"
        )

        if state.in_position:
            if sp > vwap:
                log.info(f"Straddle {sp} > VWAP {vwap} → exiting")
                exit_straddle(reason="price > VWAP")
        else:
            if sp < vwap:
                log.info(f"Straddle {sp} < VWAP {vwap} → re-entering")
                enter_straddle()

    log.info(f"Strategy finished. Total entries: {state.entry_count}")


if __name__ == "__main__":
    main()