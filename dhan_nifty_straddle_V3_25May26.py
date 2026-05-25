"""
Dhan Broker - Nifty ATM Short Straddle Strategy with VWAP
==========================================================
Strategy Logic:
  1. At ENTRY_TIME (default 09:30), fetch Nifty Spot via REST Market Quote API
  2. Find nearest expiry ATM strike → compute CE + PE LTP (straddle price)
  3. Compute VWAP of the straddle
  4. If straddle price < VWAP → Short the straddle
  5. Every 30 seconds: if straddle price > current VWAP → EXIT
     If same-strike straddle price falls back below VWAP → RE-ENTER
  6. Hard exit at EXIT_TIME (default 14:55)
  7. Ctrl+C → close all open positions gracefully

Market data approach:
  Uses dhan.market_feed_ltp() REST API (polling) instead of WebSocket.
  WebSocket (MarketFeed) in dhanhq SDK v2.1+ uses a different polling
  interface and the callback-style constructor is not supported — REST
  polling is simpler and equally reliable for 30-second check intervals.

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
from dhanhq._option_chain import OptionChain as _OptionChain    # REST option chain

# ── Load credentials from .env ───────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
for _p in [
    os.path.join(os.getcwd(), ".env"),
    os.path.join(_SCRIPT_DIR, ".env"),
    "C:/Trading/AlgoTrading/.env"
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

# ── Timing (HH:MM, 24-hour) ──────────────────────────────────────────────────
ENTRY_TIME = os.getenv("ENTRY_TIME", "12:02")
EXIT_TIME  = os.getenv("EXIT_TIME",  "14:55")

# ── Strategy parameters ───────────────────────────────────────────────────────
LOT_SIZE       = 65     # Nifty lot size (update if NSE revises)
QUANTITY       = 1      # Number of lots per leg
CHECK_INTERVAL = 30     # Seconds between VWAP/price checks

# ── Market Quote segment strings (REST API) ───────────────────────────────────
#   Used with dhan.market_feed_ltp({segment: [security_ids]})
#   "IDX_I"   → Index  (Nifty / BankNifty spot)
#   "NSE_FNO" → NSE F&O options
NIFTY_SECURITY_ID  = "13"      # Nifty 50 index security_id on Dhan
NIFTY_MQ_SEGMENT   = "IDX_I"  # segment key for Market Quote REST call
OPTIONS_MQ_SEGMENT = "NSE_FNO"

# ── Paper / Dummy Trading ─────────────────────────────────────────────────────
#   PAPER_TRADE = True  → orders are simulated locally (no real orders sent)
#   PAPER_TRADE = False → live trading via Dhan API
#   Default is True for safety. Set PAPER_TRADE=false in .env to go live.
PAPER_TRADE = os.getenv("PAPER_TRADE", "true").lower() in ("1", "true", "yes")
_paper_order_counter = 0

# ════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ════════════════════════════════════════════════════════════════════════════

import sys
_stream_handler = logging.StreamHandler(stream=sys.stdout)
if hasattr(_stream_handler.stream, "reconfigure"):
    _stream_handler.stream.reconfigure(encoding="utf-8", errors="replace")

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
        # We keep a rolling window of (price, timestamp) samples.
        # VWAP = mean of all samples since strategy start (session VWAP).
        self.vwap_samples: list  = []   # list of straddle prices polled
        self.vwap_pv:      float = 0.0  # sum of prices (for fast mean)
        self.vwap:         Optional[float] = None

        # Position tracking
        self.in_position: bool = False
        self.entry_count: int  = 0
        self.ce_order_id: Optional[str] = None
        self.pe_order_id: Optional[str] = None

        self.lock     = threading.Lock()
        self.shutdown = False

state = StrategyState()

# ════════════════════════════════════════════════════════════════════════════
#  DHAN CLIENT
# ════════════════════════════════════════════════════════════════════════════

dhan_ctx     = DhanContext(CLIENT_ID, ACCESS_TOKEN)
dhan         = dhanhq(dhan_ctx)
option_chain_client = _OptionChain(dhan_ctx)  # REST option chain client

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
        days_ahead = 7
    return today + timedelta(days=days_ahead)


def round_to_strike(spot: float, step: int = 50) -> int:
    """Round spot to nearest ATM strike (multiples of 50)."""
    return int(round(spot / step) * step)


def straddle_price() -> Optional[float]:
    """Return CE + PE LTP."""
    with state.lock:
        if state.ce_ltp is not None and state.pe_ltp is not None:
            return round(state.ce_ltp + state.pe_ltp, 2)
    return None


def update_vwap(price: float, volume: float = 1.0):
    """
    Accumulate session VWAP (simple mean of all straddle price samples).
    Since we don't have real volume for options, each poll tick = 1 unit.
    Call inside state.lock.
    """
    state.vwap_samples.append(price)
    state.vwap_pv += price
    state.vwap = round(state.vwap_pv / len(state.vwap_samples), 2)

# ════════════════════════════════════════════════════════════════════════════
#  REST MARKET QUOTE  (replaces WebSocket feed)
# ════════════════════════════════════════════════════════════════════════════

def _extract_last_price(v) -> Optional[float]:
    """
    Extract last_price from a value that may be a dict (REST response leaf)
    or a plain float/int.  Dhan REST API uses 'last_price' as the key.
    """
    if isinstance(v, dict):
        # /marketfeed/ltp  → {"last_price": 120.25}
        # /marketfeed/quote → {"last_price": 120.25, "open": ..., ...}
        for key in ("last_price", "lastPrice", "ltp", "LTP"):
            if v.get(key) is not None:
                return float(v[key])
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def fetch_ltp(segment: str, security_ids: list) -> dict:
    """
    Fetch LTP for one or more security IDs via REST.
    Returns {security_id_str: ltp_float} or {} on error.

    Routing:
      All segments → dhan.quote_data() (/marketfeed/quote) with ticker_data fallback.
      quote_data is used for all because ticker_data (/marketfeed/ltp) gets
      rate-limited when called in rapid succession.

    The full raw response is logged at DEBUG level for diagnosis.
    """
    try:
        int_ids = [int(sid) for sid in security_ids]
        request = {segment: int_ids}

        # Use quote_data for all segments — ticker_data (/marketfeed/ltp) gets
        # rate-limited when called in quick succession; quote_data is more reliable.
        resp = dhan.quote_data(request)
        if resp.get("status") == "failure":
            log.warning(f"quote_data failed [{segment}], trying ticker_data: {resp.get('remarks')}")
            resp = dhan.ticker_data(request)

        # Always log full raw response at INFO level until stable, then drop to DEBUG
        log.info(f"fetch_ltp raw [{segment}]: {resp}")

        if resp.get("status") == "failure":
            log.error(f"fetch_ltp failure [{segment}]: {resp.get('remarks')}")
            return {}

        raw_data = resp.get("data") or {}
        result = {}

        # Dhan REST response is double-wrapped: resp["data"]["data"][segment][sid]
        # Unwrap inner "data" key if present (quote_data for IDX_I does this)
        if isinstance(raw_data, dict) and "data" in raw_data:
            raw_data = raw_data["data"]

        # Now shape is: {segment: {sid: {last_price: x, ...}}}
        # Walk all segment keys and extract last_price from each sid leaf
        for seg_key, seg_data in raw_data.items():
            if not isinstance(seg_data, dict):
                continue
            for sid, v in seg_data.items():
                lp = _extract_last_price(v)
                if lp is not None:
                    result[str(sid)] = lp

        if not result:
            log.warning(f"fetch_ltp: no prices extracted [{segment}]. Raw: {raw_data}")
        return result

    except Exception as exc:
        log.error(f"fetch_ltp error [{segment} {security_ids}]: {exc}", exc_info=True)
        return {}


def refresh_prices():
    """
    Poll current LTPs for Nifty spot + CE + PE via REST and update state.
    Called once before entry and every CHECK_INTERVAL seconds after.
    """
    # ── Nifty spot ────────────────────────────────────────────────────────
    spot_data = fetch_ltp(NIFTY_MQ_SEGMENT, [NIFTY_SECURITY_ID])
    if spot_data:
        with state.lock:
            state.nifty_spot = spot_data.get(NIFTY_SECURITY_ID)

    # ── Options LTPs (only if security IDs are known) ─────────────────────
    # Small delay to avoid Dhan API rate limiting between consecutive calls
    time.sleep(0.5)
    if state.ce_security_id and state.pe_security_id:
        opt_data = fetch_ltp(OPTIONS_MQ_SEGMENT,
                             [state.ce_security_id, state.pe_security_id])
        if opt_data:
            with state.lock:
                ce_ltp = opt_data.get(state.ce_security_id)
                pe_ltp = opt_data.get(state.pe_security_id)
                if ce_ltp is not None:
                    state.ce_ltp = ce_ltp
                if pe_ltp is not None:
                    state.pe_ltp = pe_ltp
                if state.ce_ltp is not None and state.pe_ltp is not None:
                    update_vwap(state.ce_ltp + state.pe_ltp)
        else:
            # marketfeed/quote not available for NSE_FNO on this account —
            # fall back to option chain polling (slightly heavier but reliable)
            log.info("marketfeed failed for NSE_FNO — falling back to option chain for LTP refresh")
            refresh_option_ltps_via_chain()

# ════════════════════════════════════════════════════════════════════════════
#  OPTION CHAIN LOOKUP
# ════════════════════════════════════════════════════════════════════════════

def fetch_option_security_ids(strike: int, expiry: str):
    """
    Fetch CE and PE security_ids + initial LTPs for a given Nifty strike + expiry.
    expiry format: "YYYY-MM-DD"
    Returns: (ce_id, pe_id, ce_ltp, pe_ltp)

    Uses OptionChain.option_chain() — the response already contains last_price
    for each leg, so we seed state.ce_ltp / pe_ltp directly from here.
    """
    log.info(f"Fetching option chain | Strike={strike}  Expiry={expiry}")
    try:
        resp = option_chain_client.option_chain(
            under_security_id      = 13,
            under_exchange_segment = "IDX_I",
            expiry                 = expiry,
        )
        outer = resp.get("data") or {}
        data  = outer.get("data") or outer

        oc = data.get("oc") or {}
        ce_id = pe_id = None
        ce_ltp = pe_ltp = None

        for strike_key, legs in oc.items():
            try:
                if int(float(strike_key)) == strike:
                    ce_leg = legs.get("ce") or {}
                    pe_leg = legs.get("pe") or {}
                    ce_id  = str(ce_leg.get("security_id") or ce_leg.get("securityId") or "")
                    pe_id  = str(pe_leg.get("security_id") or pe_leg.get("securityId") or "")
                    ce_ltp = ce_leg.get("last_price")
                    pe_ltp = pe_leg.get("last_price")
                    log.debug(f"  CE leg keys: {list(ce_leg.keys())}")
                    break
            except (ValueError, TypeError):
                continue

        if ce_id and pe_id:
            log.info(f"  CE securityId={ce_id} ltp={ce_ltp}   PE securityId={pe_id} ltp={pe_ltp}")
        else:
            log.error(f"  Strike {strike} not found. OC keys sample: {list(oc.keys())[:5]}")

        return (ce_id or None), (pe_id or None), ce_ltp, pe_ltp

    except Exception as exc:
        log.error(f"option_chain API error: {exc}", exc_info=True)
        return None, None, None, None

def refresh_option_ltps_via_chain() -> bool:
    """
    Refresh CE and PE LTPs by re-querying the option chain API.
    This is the fallback when marketfeed/quote fails for NSE_FNO.
    Returns True if both LTPs were updated successfully.
    """
    with state.lock:
        strike      = state.atm_strike
        expiry      = state.expiry_date
        ce_sec_id   = state.ce_security_id
        pe_sec_id   = state.pe_security_id

    if not strike or not expiry:
        return False

    try:
        resp = option_chain_client.option_chain(
            under_security_id      = 13,
            under_exchange_segment = "IDX_I",
            expiry                 = expiry,
        )
        outer = resp.get("data") or {}
        data  = outer.get("data") or outer
        oc    = data.get("oc") or {}
        log.info(f"OC refresh: got {len(oc)} strikes, looking for {strike}. "
                 f"Sample: {list(oc.keys())[:5]}")

        ce_ltp = pe_ltp = None
        for strike_key, legs in oc.items():
            try:
                if int(float(strike_key)) == strike:
                    ce_ltp = legs.get("ce", {}).get("last_price")
                    pe_ltp = legs.get("pe", {}).get("last_price")
                    break
            except (ValueError, TypeError):
                continue

        if ce_ltp is not None and pe_ltp is not None:
            with state.lock:
                state.ce_ltp = float(ce_ltp)
                state.pe_ltp = float(pe_ltp)
                update_vwap(state.ce_ltp + state.pe_ltp)  # called inside lock
            log.info(f"OC refresh OK | CE={ce_ltp}  PE={pe_ltp}  "
                     f"straddle={float(ce_ltp)+float(pe_ltp):.2f}  VWAP={state.vwap}")
            return True
        else:
            log.warning(f"OC refresh: strike {strike} not found in OC. "
                        f"Available strikes sample: {list(oc.keys())[:8]}")
            return False
    except Exception as exc:
        log.error(f"refresh_option_ltps_via_chain error: {exc}", exc_info=True)
        return False


def seed_vwap_from_history():
    """
    Seed VWAP using 1-minute candle data from 09:15 to now for CE + PE.
    This ensures VWAP matches platforms like Sensibull that compute from market open.

    Uses dhan.intraday_minute_data() for each leg, then sums CE+PE close prices
    per minute to get per-minute straddle price, and feeds all into update_vwap().
    """
    with state.lock:
        ce_id  = state.ce_security_id
        pe_id  = state.pe_security_id
        strike = state.atm_strike

    if not ce_id or not pe_id:
        log.warning("seed_vwap_from_history: security IDs not set, skipping.")
        return

    today = date.today().strftime("%Y-%m-%d")
    log.info(f"Seeding VWAP from intraday history since 09:15 | strike={strike} | date={today}")

    try:
        ce_resp = dhan.intraday_minute_data(
            security_id      = ce_id,
            exchange_segment = "NSE_FNO",
            instrument_type  = "OPTIDX",
            from_date        = today,
            to_date          = today,
            interval         = 1,
        )
        time.sleep(0.5)  # avoid rate limit
        pe_resp = dhan.intraday_minute_data(
            security_id      = pe_id,
            exchange_segment = "NSE_FNO",
            instrument_type  = "OPTIDX",
            from_date        = today,
            to_date          = today,
            interval         = 1,
        )
    except Exception as exc:
        log.error(f"seed_vwap_from_history: API error: {exc}", exc_info=True)
        return

    # Parse candle data — Dhan returns lists under "open","high","low","close","timestamp"
    def parse_candles(resp, label):
        d = resp.get("data") or {}
        # unwrap double-data if present
        if isinstance(d, dict) and "data" in d:
            d = d["data"]
        closes = d.get("close") or []
        timestamps = d.get("timestamp") or []
        log.info(f"  {label}: {len(closes)} candles received")
        return closes, timestamps

    ce_closes, ce_ts = parse_candles(ce_resp, "CE")
    pe_closes, pe_ts = parse_candles(pe_resp, "PE")

    if not ce_closes or not pe_closes:
        log.warning("seed_vwap_from_history: no candle data returned — VWAP stays at OC seed.")
        log.debug(f"  CE resp: {ce_resp}")
        log.debug(f"  PE resp: {pe_resp}")
        return

    # Align by index (both should have same candle count for same instrument/date)
    n = min(len(ce_closes), len(pe_closes))
    if n == 0:
        log.warning("seed_vwap_from_history: 0 aligned candles.")
        return

    # Reset VWAP accumulators and rebuild from history
    with state.lock:
        state.vwap_samples = []
        state.vwap_pv      = 0.0
        state.vwap         = None

    now_ts = datetime.now().timestamp()
    market_open_ts = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0).timestamp()

    skipped = 0
    fed = 0
    for i in range(n):
        ts = ce_ts[i] if i < len(ce_ts) else None
        # Filter: only candles from 09:15 onward, skip future candles
        if ts is not None and (ts < market_open_ts or ts > now_ts):
            skipped += 1
            continue
        try:
            straddle = float(ce_closes[i]) + float(pe_closes[i])
            with state.lock:
                update_vwap(straddle)
            fed += 1
        except (TypeError, ValueError):
            continue

    log.info(f"VWAP seeded from history: {fed} candles used, {skipped} skipped | VWAP={state.vwap}")


# ════════════════════════════════════════════════════════════════════════════
#  ORDER MANAGEMENT
# ════════════════════════════════════════════════════════════════════════════

def _paper_order_id() -> str:
    """Generate a sequential dummy order ID for paper trading."""
    global _paper_order_counter
    _paper_order_counter += 1
    return f"PAPER-{_paper_order_counter:04d}"


def place_sell_order(security_id: str, qty: int) -> Optional[str]:
    """Short sell at market (MARGIN intraday).
    In PAPER_TRADE mode the order is simulated — no real order is sent to Dhan.
    """
    if PAPER_TRADE:
        ltp = state.ce_ltp if security_id == state.ce_security_id else state.pe_ltp
        order_id = _paper_order_id()
        log.info(
            f"[PAPER] SELL simulated | sec={security_id} qty={qty} "
            f"price~{ltp} orderId={order_id}"
        )
        return order_id
    # ── LIVE TRADE ────────────────────────────────────────────────────────
    try:
        resp = dhan.place_order(
            security_id      = security_id,
            exchange_segment = dhan.NSE_FNO,
            transaction_type = dhan.SELL,
            quantity         = qty,
            order_type       = dhan.MARKET,
            product_type     = dhan.MARGIN,
            price            = 0,
        )
        order_id = resp.get("data", {}).get("orderId")
        log.info(f"SELL placed | sec={security_id} qty={qty} orderId={order_id}")
        return str(order_id) if order_id else None
    except Exception as exc:
        log.error(f"place_sell_order error: {exc}")
        return None


def place_buy_order(security_id: str, qty: int) -> Optional[str]:
    """Buy back to cover a short leg.
    In PAPER_TRADE mode the order is simulated — no real order is sent to Dhan.
    """
    if PAPER_TRADE:
        ltp = state.ce_ltp if security_id == state.ce_security_id else state.pe_ltp
        order_id = _paper_order_id()
        log.info(
            f"[PAPER] BUY  simulated | sec={security_id} qty={qty} "
            f"price~{ltp} orderId={order_id}"
        )
        return order_id
    # ── LIVE TRADE ────────────────────────────────────────────────────────
    try:
        resp = dhan.place_order(
            security_id      = security_id,
            exchange_segment = dhan.NSE_FNO,
            transaction_type = dhan.BUY,
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
            f"Straddle entered #{state.entry_count} | "
            f"Price={straddle_price()} | VWAP={state.vwap}"
        )
    else:
        log.error("One or both legs failed to place.")


def exit_straddle(reason: str = ""):
    """Buy back CE and PE to close the straddle."""
    qty = LOT_SIZE * QUANTITY
    log.info(f"◀ EXITING straddle | Reason: {reason} | Qty={qty}")
    place_buy_order(state.ce_security_id, qty)
    place_buy_order(state.pe_security_id, qty)
    state.in_position = False
    log.info(f"Straddle exited | Price={straddle_price()} | VWAP={state.vwap}")


def close_all_positions():
    """Emergency flatten — called on Ctrl+C."""
    log.warning("CLOSING ALL POSITIONS")
    if state.in_position:
        exit_straddle(reason="emergency/abort")
    else:
        log.info("No open position to close.")

# ════════════════════════════════════════════════════════════════════════════
#  SIGNAL HANDLER — Ctrl+C
# ════════════════════════════════════════════════════════════════════════════

def handle_sigint(signum, frame):
    log.warning("Keyboard interrupt — closing positions ...")
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
        log.info(f"Waiting for {label} — {int(remaining // 60)}m {int(remaining % 60)}s ...")
        time.sleep(min(60, remaining))

# ════════════════════════════════════════════════════════════════════════════
#  MAIN STRATEGY
# ════════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("  Dhan Nifty ATM Short Straddle — VWAP Strategy")
    log.info(f"  Entry : {ENTRY_TIME}    Exit : {EXIT_TIME}")
    log.info(f"  Lots  : {QUANTITY}  x  Lot size : {LOT_SIZE}  =  Qty : {QUANTITY * LOT_SIZE}")
    log.info(f"  Mode  : {'PAPER TRADE (no real orders sent)' if PAPER_TRADE else 'LIVE TRADE'}")
    log.info("=" * 60)

    entry_dt = parse_time(ENTRY_TIME)
    exit_dt  = parse_time(EXIT_TIME)

    # ── 1. Wait until entry time ─────────────────────────────────────────────
    wait_until(entry_dt, f"entry time ({ENTRY_TIME})")

    # ── 2. Fetch Nifty Spot via REST ─────────────────────────────────────────
    log.info("Fetching Nifty Spot price via Market Quote REST API ...")
    refresh_prices()
    with state.lock:
        spot = state.nifty_spot

    if spot is None:
        log.error("Nifty Spot not received — check credentials / market hours.")
        return

    log.info(f"Nifty Spot at {ENTRY_TIME} : {spot}")

    # ── 3. ATM strike & expiry ───────────────────────────────────────────────
    state.atm_strike  = round_to_strike(spot)
    nearest_tuesday   = get_nearest_tuesday()
    state.expiry_date = nearest_tuesday.strftime("%Y-%m-%d")
    log.info(f"ATM Strike : {state.atm_strike}")
    log.info(f"Expiry     : {state.expiry_date}  (nearest Tuesday)")

    # ── 4. Fetch option security IDs ─────────────────────────────────────────
    ce_id, pe_id, ce_ltp_init, pe_ltp_init = fetch_option_security_ids(state.atm_strike, state.expiry_date)
    if not ce_id or not pe_id:
        log.error("Security IDs unavailable — cannot continue.")
        return

    state.ce_security_id = ce_id
    state.pe_security_id = pe_id
    # Seed initial LTPs from option chain response (avoids extra market_feed_ltp calls)
    if ce_ltp_init is not None:
        state.ce_ltp = float(ce_ltp_init)
    if pe_ltp_init is not None:
        state.pe_ltp = float(pe_ltp_init)
    if state.ce_ltp and state.pe_ltp:
        update_vwap(state.ce_ltp + state.pe_ltp)
        log.info(f"Initial straddle price from OC: {state.ce_ltp + state.pe_ltp:.2f}  VWAP seeded: {state.vwap}")

    # ── 5. Seed VWAP from intraday history since 09:15 ───────────────────────
    seed_vwap_from_history()

    # Also do a couple of live polls to catch up to present
    log.info("Collecting 2 live price ticks to update VWAP to current ...")
    for _ in range(2):
        refresh_prices()
        time.sleep(1)

    sp = straddle_price()
    if sp is None:
        log.error("No straddle price received — verify security IDs.")
        return

    log.info(f"Straddle Price : {sp}")
    log.info(f"VWAP           : {state.vwap}")

    # ── 6. Initial entry decision ────────────────────────────────────────────
    if state.vwap is not None and sp < state.vwap:
        log.info(f"Straddle {sp} < VWAP {state.vwap} → entering straddle")
        enter_straddle()
    else:
        log.info(f"Straddle {sp} >= VWAP {state.vwap} → waiting for drop below VWAP")

    # ── 7. Monitoring loop ───────────────────────────────────────────────────
    log.info(f"Monitoring every {CHECK_INTERVAL}s until {EXIT_TIME} ...")

    while not state.shutdown:
        if datetime.now() >= exit_dt:
            log.info(f"{EXIT_TIME} — hard exit.")
            if state.in_position:
                exit_straddle(reason=f"{EXIT_TIME} EOD exit")
            break

        time.sleep(CHECK_INTERVAL)

        # Refresh prices from REST API
        refresh_prices()

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