"""
Dhan Broker - Nifty ATM Short Straddle Strategy with VWAP
==========================================================
Strategy Logic:
  1. At ENTRY_TIME (default 09:30), fetch Nifty Spot + option LTPs via Option Chain API
  2. Find nearest expiry ATM strike → compute CE + PE LTP (straddle price)
  3. Compute VWAP of the straddle
  4. If straddle price < VWAP → Short the straddle
  5. Every 30 seconds: if straddle price > current VWAP → EXIT
     Re-entry rules:
       a. Wait at least RE_ENTRY_COOLDOWN_MINUTES (5 min) after any exit
       b. Entry condition must still hold: straddle price < VWAP
       c. Max MAX_RE_ENTRIES (3) re-entries allowed  [total = 1 initial + 3]
  6. Every refresh shows: CE LTP | PE LTP | Straddle | VWAP | Combined P&L
  7. Hard exit at EXIT_TIME (default 14:55)
  8. Ctrl+C → close all open positions gracefully

Market data approach:
  All price data (Nifty spot + CE/PE LTPs) is fetched exclusively via the
  Option Chain REST API (OptionChain.option_chain()).  The underlying_last_price
  field in the OC response provides the Nifty spot; CE/PE last_price fields
  provide option LTPs.  This avoids marketfeed/quote rate-limit issues entirely.

Requirements:
    pip install dhanhq python-dotenv
"""

import os
import time
import signal
import logging
import threading
import sys
from datetime import datetime, date, timedelta
from typing import Optional
from dotenv import load_dotenv

# ── DhanHQ SDK (v2 API) ──────────────────────────────────────────────────────
from dhanhq import dhanhq, DhanContext
from dhanhq._option_chain import OptionChain as _OptionChain    # REST option chain

# ── Log file setup ─────────────────────────────────────────────────────────────
# Every print() and log line is written to BOTH the terminal and a timestamped
# .out file under run_log/ so the full session output is always saved to disk.
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = "run_log/" + f"nft_930_{timestamp}.out"

# Ensure output directories exist before opening log files
os.makedirs("run_log", exist_ok=True)
os.makedirs("logs", exist_ok=True)

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

    def reconfigure(self, **kwargs):
        """Satisfy logging.StreamHandler's reconfigure check (no-op on Tee)."""
        for s in self._streams:
            if hasattr(s, "reconfigure"):
                try:
                    s.reconfigure(**kwargs)
                except Exception:
                    pass

    def isatty(self):
        return False

_console_out = sys.stdout
_console_err = sys.stderr
log_file = open(log_filename, "w", encoding="utf-8")

# Redirect stdout and stderr through the Tee so all output goes to terminal + file
sys.stdout = _Tee(_console_out, log_file)
sys.stderr = _Tee(_console_err, log_file)

print("Job started...")

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
ENTRY_TIME = os.getenv("ENTRY_TIME", "9:30")
EXIT_TIME  = os.getenv("EXIT_TIME",  "14:55")

# ── Strategy parameters ───────────────────────────────────────────────────────
LOT_SIZE       = 65     # Nifty lot size (update if NSE revises)
QUANTITY       = 1      # Number of lots per leg
CHECK_INTERVAL = 30     # Seconds between VWAP/price checks

# ── Re-entry rules ────────────────────────────────────────────────────────────
MAX_RE_ENTRIES             = 3    # Max re-entries after the initial entry (1+3 = 4 total)
RE_ENTRY_COOLDOWN_MINUTES  = 5   # Minutes to wait after any exit before re-entering

# ── Paper / Dummy Trading ─────────────────────────────────────────────────────
#   PAPER_TRADE = True  → orders are simulated locally (no real orders sent)
#   PAPER_TRADE = False → live trading via Dhan API
#   Default is True for safety. Set PAPER_TRADE=false in .env to go live.
PAPER_TRADE = os.getenv("PAPER_TRADE", "true").lower() in ("1", "true", "yes")
_paper_order_counter = 0

# ════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ════════════════════════════════════════════════════════════════════════════

_stream_handler = logging.StreamHandler(stream=sys.stdout)
if hasattr(_stream_handler.stream, "reconfigure"):
    _stream_handler.stream.reconfigure(encoding="utf-8", errors="replace")

_formatter = logging.Formatter(
    fmt="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_stream_handler.setFormatter(_formatter)

_file_handler = logging.FileHandler(
    f"logs/NFT930_strdl_strtg_{timestamp}.log", encoding="utf-8"
)
_file_handler.setFormatter(_formatter)

# Configure root logger explicitly (avoids basicConfig no-op when handlers already exist)
_root_logger = logging.getLogger()
_root_logger.setLevel(logging.INFO)
# Remove any pre-existing handlers to avoid duplicate log lines
for _h in _root_logger.handlers[:]:
    _root_logger.removeHandler(_h)
_root_logger.addHandler(_stream_handler)
_root_logger.addHandler(_file_handler)
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
        self.entry_count: int  = 0      # total entries (initial + re-entries)
        self.re_entry_count: int = 0    # re-entries only (max MAX_RE_ENTRIES)
        self.ce_order_id: Optional[str] = None
        self.pe_order_id: Optional[str] = None

        # Re-entry cooldown: timestamp of last exit (None = never exited)
        self.last_exit_time: Optional[datetime] = None

        # P&L tracking across all lots/entries
        # Each entry records (ce_entry_price, pe_entry_price)
        # On exit we compute realized P&L; unrealized is computed live.
        self.entry_ce_price:   Optional[float] = None   # CE price at current/last entry
        self.entry_pe_price:   Optional[float] = None   # PE price at current/last entry
        self.entry_vwap:       Optional[float] = None   # VWAP snapshot at entry time
        self.realized_pnl:     float           = 0.0    # cumulative closed P&L (all lots, ₹)
        self.total_qty:        int             = 0       # qty for current open leg

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
#  PRICE REFRESH  (Option Chain only — no marketfeed)
# ════════════════════════════════════════════════════════════════════════════

def refresh_prices():
    """
    Poll current prices for Nifty spot + CE + PE via the Option Chain API.
    Called once before entry and every CHECK_INTERVAL seconds after.

    The OC response contains:
      - underlying_last_price → Nifty spot
      - oc[strike]["ce"]["last_price"] / ["pe"]["last_price"] → option LTPs
    """
    refresh_option_ltps_via_chain(update_spot=True)



# ════════════════════════════════════════════════════════════════════════════
#  OPTION CHAIN LOOKUP
# ════════════════════════════════════════════════════════════════════════════

def _get_nearest_expiry_from_api(target_date: date) -> Optional[str]:
    """
    Call OptionChain.expiry_list() to get the exact expiry string format Dhan uses,
    then return the one closest to (and >= ) target_date.
    Falls back to None if the API call fails.
    """
    try:
        resp = option_chain_client.expiry_list(
            under_security_id      = 13,
            under_exchange_segment = "IDX_I",
        )
        log.info(f"expiry_list raw response: {resp}")
        outer = resp.get("data") or {}
        data  = outer.get("data") or outer

        # The list may be under "data", "expiryList", "expiry_list", or directly a list
        expiry_list = None
        if isinstance(data, list):
            expiry_list = data
        else:
            for key in ("expiryList", "expiry_list", "expiries", "data"):
                v = data.get(key)
                if isinstance(v, list) and v:
                    expiry_list = v
                    break

        if not expiry_list:
            log.warning(f"expiry_list: could not extract list from response. Keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
            return None

        log.info(f"expiry_list: {expiry_list}")

        # Parse each expiry string and find the nearest one >= target_date
        best_str  = None
        best_date = None
        for s in expiry_list:
            for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%B-%Y", "%d-%m-%Y"):
                try:
                    d = datetime.strptime(str(s), fmt).date()
                    if d >= target_date:
                        if best_date is None or d < best_date:
                            best_date = d
                            best_str  = str(s)
                    break
                except ValueError:
                    continue

        if best_str:
            log.info(f"expiry_list: selected expiry='{best_str}' (date={best_date})")
        else:
            log.warning(f"expiry_list: no expiry >= {target_date} found in {expiry_list}")
        return best_str

    except Exception as exc:
        log.error(f"expiry_list API error: {exc}", exc_info=True)
        return None


def fetch_spot_and_security_ids(expiry: str):
    """
    Single OC API call that returns:
      spot           - Nifty underlying price
      ce_id, pe_id   - security IDs for ATM CE and PE (ATM computed from spot)
      ce_ltp, pe_ltp - initial LTPs

    Using one call avoids the rate-limit failure that occurs when two OC calls
    are made in rapid succession (spot fetch then security-ID fetch).
    """
    log.info(f"OC combined fetch | Expiry='{expiry}'")
    try:
        resp = option_chain_client.option_chain(
            under_security_id      = 13,
            under_exchange_segment = "IDX_I",
            expiry                 = expiry,
        )
        log.info(f"OC combined raw status={resp.get('status')}  data_type={type(resp.get('data')).__name__}")

        if resp.get("status") == "failure":
            log.error(f"OC combined fetch: API returned failure. Full resp: {str(resp)[:600]}")
            return None, None, None, None, None

        outer = resp.get("data") or {}
        data  = outer.get("data") or outer
        oc    = data.get("oc") or {}

        # ── Extract Nifty spot ────────────────────────────────────────────────
        spot = _find_spot_in_dict(resp)
        if spot is None:
            log.error(f"OC combined: spot not found. data keys={list(data.keys())}  full={str(resp)[:400]}")
            return None, None, None, None, None
        log.info(f"OC combined: Nifty spot={spot}  strikes in OC={len(oc)}")

        if not oc:
            log.error(f"OC combined: oc empty. data keys={list(data.keys())}  full={str(resp)[:600]}")
            return spot, None, None, None, None

        # ── Compute ATM and find CE/PE legs ───────────────────────────────────
        atm = round_to_strike(spot)
        ce_id = pe_id = ce_ltp = pe_ltp = None

        for strike_key, legs in oc.items():
            try:
                if int(float(strike_key)) == atm:
                    ce_leg = legs.get("ce") or {}
                    pe_leg = legs.get("pe") or {}
                    ce_id  = str(ce_leg.get("security_id") or ce_leg.get("securityId") or "")
                    pe_id  = str(pe_leg.get("security_id") or pe_leg.get("securityId") or "")
                    ce_ltp = ce_leg.get("last_price")
                    pe_ltp = pe_leg.get("last_price")
                    break
            except (ValueError, TypeError):
                continue

        if ce_id and pe_id:
            log.info(f"OC combined: ATM={atm}  CE id={ce_id} ltp={ce_ltp}  PE id={pe_id} ltp={pe_ltp}")
        else:
            log.error(f"OC combined: ATM strike {atm} not found. Sample OC keys: {list(oc.keys())[:8]}")

        return spot, (ce_id or None), (pe_id or None), ce_ltp, pe_ltp

    except Exception as exc:
        log.error(f"fetch_spot_and_security_ids error: {exc}", exc_info=True)
        return None, None, None, None, None


def fetch_option_security_ids(strike: int, expiry: str):
    """
    Fetch CE and PE security_ids + initial LTPs for a given Nifty strike + expiry.
    expiry must be the exact string returned by expiry_list() (already resolved by caller).
    Returns: (ce_id, pe_id, ce_ltp, pe_ltp)
    """
    log.info(f"Fetching option chain | Strike={strike}  Expiry='{expiry}'")
    try:
        resp = option_chain_client.option_chain(
            under_security_id      = 13,
            under_exchange_segment = "IDX_I",
            expiry                 = expiry,
        )
        outer = resp.get("data") or {}
        data  = outer.get("data") or outer
        oc    = data.get("oc") or {}

        if not oc:
            log.error(
                f"  OC empty for expiry='{expiry}'.\n"
                f"  resp keys={list(resp.keys())}  outer keys={list(outer.keys())}  "
                f"  data keys={list(data.keys())}\n"
                f"  full resp: {str(resp)[:800]}"
            )
            return None, None, None, None

        log.info(f"  OC returned {len(oc)} strikes")
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
                    break
            except (ValueError, TypeError):
                continue

        if ce_id and pe_id:
            log.info(f"  CE securityId={ce_id} ltp={ce_ltp}   PE securityId={pe_id} ltp={pe_ltp}")
            return (ce_id or None), (pe_id or None), ce_ltp, pe_ltp
        else:
            log.error(f"  Strike {strike} not found in OC. Sample keys: {list(oc.keys())[:8]}")
            return None, None, None, None

    except Exception as exc:
        log.error(f"option_chain API error: {exc}", exc_info=True)
        return None, None, None, None
def _find_spot_in_dict(d: dict) -> Optional[float]:
    """
    Recursively search a dict for any known Dhan spot-price field name.
    Dhan OC responses have varied the key across SDK versions; this covers all known variants.
    """
    SPOT_KEYS = (
        "underlying_last_price", "underlyingLastPrice",
        "underlying_ltp",        "underlyingLtp",
        "last_price",            "lastPrice",
        "ltp",                   "LTP",
        "spot",                  "spotPrice",
    )
    if not isinstance(d, dict):
        return None
    for k in SPOT_KEYS:
        v = d.get(k)
        if v is not None:
            try:
                f = float(v)
                if f > 0:
                    return f
            except (TypeError, ValueError):
                pass
    # One level of recursion into nested dicts (e.g. resp["data"]["data"])
    for v in d.values():
        if isinstance(v, dict):
            result = _find_spot_in_dict(v)
            if result is not None:
                return result
    return None


def refresh_option_ltps_via_chain(update_spot: bool = False) -> bool:
    """
    Refresh CE and PE LTPs (and optionally Nifty spot) via the Option Chain API.
    This is now the sole price-refresh mechanism — no marketfeed calls.

    update_spot=True  → also update state.nifty_spot from the OC response.
    Returns True if both option LTPs were updated successfully.
    """
    with state.lock:
        strike = state.atm_strike
        expiry = state.expiry_date

    # If expiry not yet set, resolve it via expiry_list API
    if not expiry:
        api_expiry = _get_nearest_expiry_from_api(get_nearest_tuesday())
        expiry = api_expiry or get_nearest_tuesday().strftime("%Y-%m-%d")

    # If neither spot nor options are needed yet, bail early
    if not update_spot and not strike:
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
    except Exception as exc:
        log.error(f"refresh_option_ltps_via_chain OC call error: {exc}", exc_info=True)
        return False

    try:
        # ── Nifty spot — search the full response for any known spot field ──
        if update_spot:
            spot = _find_spot_in_dict(resp)
            if spot is not None:
                with state.lock:
                    state.nifty_spot = spot
                log.info(f"OC refresh: Nifty spot = {state.nifty_spot}")
            else:
                # Log top-level keys so we can diagnose the field name used by this account
                log.warning(
                    f"OC refresh: spot price not found in OC response. "
                    f"Top-level keys: {list(resp.keys())}  "
                    f"data keys: {list((resp.get('data') or {}).keys())}  "
                    f"inner keys: {list(data.keys())}"
                )

        # ── Option LTPs — only if ATM strike is already known ───────────────
        if not strike:
            log.info(f"OC refresh (spot-only): oc had {len(oc)} strikes (strike not yet set, skipping LTP extract)")
            return False

        #log.info(f"OC refresh: got {len(oc)} strikes, looking for {strike}. "
              #   f"Sample: {list(oc.keys())[:5]}")

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
                update_vwap(state.ce_ltp + state.pe_ltp)
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
    is_reentry = state.entry_count > 0
    label = f"RE-ENTRY #{state.re_entry_count + 1}" if is_reentry else "INITIAL ENTRY"
    log.info(f"▶ {label} — Short Straddle | Strike={state.atm_strike} | Qty={qty}")
    ce_oid = place_sell_order(state.ce_security_id, qty)
    pe_oid = place_sell_order(state.pe_security_id, qty)
    if ce_oid and pe_oid:
        state.ce_order_id  = ce_oid
        state.pe_order_id  = pe_oid
        state.in_position  = True
        state.entry_count += 1
        if is_reentry:
            state.re_entry_count += 1
        # Record entry prices for P&L
        state.entry_ce_price = state.ce_ltp
        state.entry_pe_price = state.pe_ltp
        state.entry_vwap     = state.vwap      # snapshot VWAP at entry moment
        state.total_qty      = qty
        log.info(
            f"Straddle entered #{state.entry_count} "
            f"(re-entries used: {state.re_entry_count}/{MAX_RE_ENTRIES}) | "
            f"CE={state.entry_ce_price}  PE={state.entry_pe_price} | "
            f"Straddle={straddle_price()} | VWAP={state.vwap}"
        )
    else:
        log.error("One or both legs failed to place.")


def exit_straddle(reason: str = ""):
    """Buy back CE and PE to close the straddle."""
    qty = LOT_SIZE * QUANTITY
    log.info(f"◀ EXITING straddle | Reason: {reason} | Qty={qty}")
    place_buy_order(state.ce_security_id, qty)
    place_buy_order(state.pe_security_id, qty)

    # Compute realized P&L for this leg (short straddle: sold high, bought low = profit)
    if state.entry_ce_price is not None and state.entry_pe_price is not None:
        entry_straddle = state.entry_ce_price + state.entry_pe_price
        exit_straddle_price = (state.ce_ltp or 0) + (state.pe_ltp or 0)
        leg_pnl = (entry_straddle - exit_straddle_price) * qty
        state.realized_pnl += leg_pnl
        log.info(
            f"Leg P&L: entry={entry_straddle:.2f}  exit={exit_straddle_price:.2f}  "
            f"leg_₹={leg_pnl:+.2f}  cumulative_realized_₹={state.realized_pnl:+.2f}"
        )

    state.in_position    = False
    state.last_exit_time = datetime.now()
    log.info(
        f"Straddle exited | Straddle={straddle_price()} | VWAP={state.vwap} | "
        f"Total realized P&L=₹{state.realized_pnl:+.2f}"
    )


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

    # ── 2+3+4. Fetch Nifty Spot, ATM strike, expiry, and security IDs — single OC call ──
    log.info("Fetching Nifty Spot + option chain data ...")

    # Resolve expiry first (lightweight call, no rate-limit concern)
    nearest_tuesday = get_nearest_tuesday()
    api_expiry = _get_nearest_expiry_from_api(nearest_tuesday)
    if api_expiry:
        state.expiry_date = api_expiry
        log.info(f"Expiry     : {state.expiry_date}  (from Dhan expiry_list API)")
    else:
        state.expiry_date = nearest_tuesday.strftime("%Y-%m-%d")
        log.warning(f"Expiry     : {state.expiry_date}  (fallback — expiry_list API failed)")

    # Single OC call — returns spot + all strikes in one response
    spot, ce_id, pe_id, ce_ltp_init, pe_ltp_init = fetch_spot_and_security_ids(state.expiry_date)

    if spot is None:
        log.error("Nifty Spot not received — check credentials / market hours.")
        return

    state.nifty_spot = spot
    log.info(f"Nifty Spot at {ENTRY_TIME} : {spot}")

    state.atm_strike = round_to_strike(spot)
    log.info(f"ATM Strike : {state.atm_strike}")

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

        # ── Compute combined P&L (realized + unrealized) ─────────────────────
        if state.in_position and state.entry_ce_price is not None and state.entry_pe_price is not None:
            entry_straddle_val = state.entry_ce_price + state.entry_pe_price
            unrealized_pnl = (entry_straddle_val - sp) * (LOT_SIZE * QUANTITY)
        else:
            unrealized_pnl = 0.0
        combined_pnl = state.realized_pnl + unrealized_pnl

        # ── Cooldown status ───────────────────────────────────────────────────
        cooldown_secs_left = 0
        if state.last_exit_time is not None and not state.in_position:
            elapsed = (datetime.now() - state.last_exit_time).total_seconds()
            cooldown_secs_left = max(0.0, RE_ENTRY_COOLDOWN_MINUTES * 60 - elapsed)

        # ── Status line ───────────────────────────────────────────────────────
        # Build entry-price row (shown only when a position has been taken)
        if state.in_position and state.entry_ce_price is not None and state.entry_pe_price is not None:
            entry_straddle_disp = state.entry_ce_price + state.entry_pe_price
            entry_line = (
                f"  ENTRY  → CE={state.entry_ce_price:>8.2f}  PE={state.entry_pe_price:>8.2f}  "
                f"Straddle={entry_straddle_disp:>8.2f}  VWAP={state.entry_vwap:>8.2f}"
            )
        else:
            entry_line = ""

        # Current prices row
        current_line = (
            f"  CURRENT→ CE={state.ce_ltp:>8.2f}  PE={state.pe_ltp:>8.2f}  "
            f"Straddle={sp:>8.2f}  VWAP={vwap:>8.2f}"
        )

        log.info(
            f"{'─' * 60}\n"
            f"  Pos={'IN ' if state.in_position else 'OUT'}  "
            f"Trades={state.entry_count}(re={state.re_entry_count}/{MAX_RE_ENTRIES})  "
            f"P&L=₹{combined_pnl:+.2f}"
            + (f"  [cooldown {int(cooldown_secs_left)}s]" if cooldown_secs_left > 0 else "")
            + (f"\n{entry_line}" if entry_line else "")
            + f"\n{current_line}"
        )

        # ── Position management ───────────────────────────────────────────────
        if state.in_position:
            if sp > vwap:
                log.info(f"Straddle {sp} > VWAP {vwap} → exiting")
                exit_straddle(reason="price > VWAP")
        else:
            # Re-entry gate: cooldown elapsed?
            if cooldown_secs_left > 0:
                log.info(
                    f"Re-entry blocked — cooldown active "
                    f"({int(cooldown_secs_left)}s remaining)"
                )
                continue

            # Re-entry gate: max re-entries reached?
            if state.entry_count > 0 and state.re_entry_count >= MAX_RE_ENTRIES:
                log.info(
                    f"Re-entry blocked — max re-entries reached "
                    f"({state.re_entry_count}/{MAX_RE_ENTRIES})"
                )
                continue

            # Entry condition: straddle below VWAP
            if sp < vwap:
                action = "entering" if state.entry_count == 0 else "re-entering"
                log.info(f"Straddle {sp} < VWAP {vwap} → {action}")
                enter_straddle()
            else:
                log.info(f"Straddle {sp} >= VWAP {vwap} — waiting for drop below VWAP")

    log.info(
        f"Strategy finished. Total entries: {state.entry_count}  "
        f"(re-entries: {state.re_entry_count}/{MAX_RE_ENTRIES})  "
        f"Final realized P&L=₹{state.realized_pnl:+.2f}"
    )


if __name__ == "__main__":
    main()