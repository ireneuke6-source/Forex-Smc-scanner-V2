"""
Deriv Forex SMC+ICT Scanner — OB + FVG Confluence Model (v3 — per-pair OB tuning)
Pairs: GBPUSD, EURUSD, XAUUSD, USDJPY, GBPJPY
Runs 24/7 on Railway.app

CHANGES FROM v2 (based on first 24h daily diagnostics report):

Diagnostics revealed the bottleneck is NOT the FVG layer (M15_no_fvg
was 0% across ALL pairs) — it is the H1 OB layer killing signals for
XAUUSD, USDJPY, and GBPJPY specifically:

  XAUUSD: 79% dying at H1 OB — gold moves impulsively, OBs mitigated fast
  USDJPY: 99% dying at H1 OB — strong trend, no pullback OBs left behind
  GBPJPY: 100% dying at H1 OB — same as USDJPY

  EURUSD: Working well (10% signal rate) — DO NOT change
  GBPUSD: 100% H4 neutral — market ranging, correct skip behaviour

Fix applied: Per-pair OB config (PER_PAIR_OB) that overrides global OB
settings for the three problematic pairs while leaving EURUSD and GBPUSD
completely unchanged:

  XAUUSD: OB_MIN_BODY 0.4→0.3, OB_LOOKBACK 30→50, OB_MAX_AGE 35→45
  USDJPY: OB_MIN_BODY 0.4→0.3, OB_LOOKBACK 30→50, OB_MAX_AGE 35→45
  GBPJPY: OB_MIN_BODY 0.4→0.3, OB_LOOKBACK 30→50, OB_MAX_AGE 35→45

Rationale:
  - Lower OB_MIN_BODY (0.3) accepts smaller-bodied candles as OBs.
    Fast-moving pairs like JPY crosses and gold leave smaller bodies
    before impulsive moves — requiring 40% body was filtering them out.
  - Wider OB_LOOKBACK (50) gives the scanner more history to find an OB
    in strongly trending pairs where the last qualifying candle may be
    further back than 30 bars.
  - Higher OB_MAX_AGE (45) gives found OBs more time to attract an FVG
    before expiring, consistent with v2's reasoning.

All other logic (FVG proximity, M5 entry, scoring, trade math) unchanged.

Entry Logic (all must align):
  H4  — Trend bias (EMA 21/50) + ADX strength
  H1  — Unmitigated Order Block (per-pair settings)
  M15 — Fair Value Gap within proximity of the H1 OB zone
  M5  — Price enters the confluence zone + rejection wick + RSI + momentum

Trade Plan:
  Entry  = M5 close inside OB+FVG zone
  SL     = Below/above OB wick + ATR buffer
  TP1    = 1:1 (close 50%, move SL to BE)
  TP2    = Dynamic 1:RR based on signal score
  Cooldown = 4 hours per level
"""

import asyncio
import json
import logging
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple

import pandas as pd
import websockets

# =============================================================================
#  LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

WAT = timezone(timedelta(hours=1))


# =============================================================================
#  CONFIG — environment variables + constants
# =============================================================================
TG_TOKEN   = os.environ.get("TG_TOKEN",   "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

SYMBOL_MAP = {
    "GBPUSD": "frxGBPUSD",
    "EURUSD": "frxEURUSD",
    "XAUUSD": "frxXAUUSD",
    "USDJPY": "frxUSDJPY",
    "GBPJPY": "frxGBPJPY",
}

WS_URI         = "wss://ws.derivws.com/websockets/v3?app_id=1089"
SCAN_INTERVAL  = 300       # scan every 5 minutes
COOLDOWN_SECS  = 14400     # 4-hour cooldown per signal key
COOLDOWN_FILE  = "cooldown_v3.json"
STATS_FILE     = "layer_stats_v3.json"
STATS_SUMMARY_INTERVAL = 86400  # post a rejection-stats summary once a day

# Timeframes
H4_TF  = 14400; H4_COUNT  = 100
H1_TF  = 3600;  H1_COUNT  = 100
M15_TF = 900;   M15_COUNT = 120
M5_TF  = 300;   M5_COUNT  = 150

# Order Block settings — global defaults (used for EURUSD and GBPUSD)
OB_LOOKBACK  = 30
OB_MIN_BODY  = 0.4
OB_MAX_AGE   = 35

# Per-pair OB overrides — applied instead of globals for specific pairs.
# Based on 24h diagnostics showing XAUUSD/USDJPY/GBPJPY dying at H1 OB
# 79-100% of the time due to fast-moving impulsive price action that
# either mitigates OBs quickly or leaves smaller-bodied candles behind.
# EURUSD and GBPUSD are NOT listed here — they use global settings above.
PER_PAIR_OB = {
    "XAUUSD": {"lookback": 50, "min_body": 0.3, "max_age": 45},
    "USDJPY": {"lookback": 50, "min_body": 0.3, "max_age": 45},
    "GBPJPY": {"lookback": 50, "min_body": 0.3, "max_age": 45},
}

# FVG settings
FVG_MIN_PCT  = 0.02

# NEW — FVG-to-OB proximity (replaces strict overlap requirement).
# An FVG now qualifies if it sits within this % of price distance from
# the OB zone edges, not just if it literally overlaps the OB body.
FVG_OB_PROXIMITY_PCT = 0.15   # 0.15% of price as a proximity buffer

# Signal filters
ADX_MIN  = 18.0
RSI_OB   = 75.0
RSI_OS   = 25.0
BODY_MIN = 0.35
MIN_WICK = 0.25
ATR_BUF  = 0.5

# Risk/Reward
RR_MIN = 1.5
RR_MAX = 3.0

DECIMALS = {
    "GBPUSD": 5,
    "EURUSD": 5,
    "XAUUSD": 2,
    "USDJPY": 3,
    "GBPJPY": 3,
}


# =============================================================================
#  LAYER REJECTION DIAGNOSTICS
# =============================================================================
LAYER_NAMES = ["H4_neutral_or_weak", "H1_no_ob", "M15_no_fvg", "M5_no_entry", "signal_fired"]

def _load_stats() -> dict:
    try:
        if os.path.exists(STATS_FILE):
            with open(STATS_FILE, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
    except (json.JSONDecodeError, OSError):
        pass
    return {"since": time.time(), "counts": {}}


def _save_stats(state: dict) -> None:
    try:
        tmp = STATS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f)
        os.replace(tmp, STATS_FILE)
    except OSError as e:
        log.error("Failed to save stats file: %s", e)


def record_rejection(display_symbol: str, layer: str) -> None:
    """Tally which layer a scan died at, per symbol, for the daily summary."""
    state = _load_stats()
    counts = state.setdefault("counts", {})
    sym_counts = counts.setdefault(display_symbol, {name: 0 for name in LAYER_NAMES})
    sym_counts[layer] = sym_counts.get(layer, 0) + 1
    _save_stats(state)


def maybe_post_stats_summary() -> None:
    """Once every STATS_SUMMARY_INTERVAL seconds, post a rejection breakdown
    to Telegram and reset the counters."""
    state = _load_stats()
    since = state.get("since", time.time())
    if time.time() - since < STATS_SUMMARY_INTERVAL:
        return

    counts = state.get("counts", {})
    if not counts:
        state["since"] = time.time()
        _save_stats(state)
        return

    lines = ["📊 <b>Daily Scan Diagnostics</b>", "—" * 20]
    for sym, layer_counts in counts.items():
        total = sum(layer_counts.values())
        if total == 0:
            continue
        lines.append(f"<b>{sym}</b> ({total} scans)")
        for layer in LAYER_NAMES:
            c = layer_counts.get(layer, 0)
            pct = round(100 * c / total) if total else 0
            label = layer.replace("_", " ")
            lines.append(f"  {label}: {c} ({pct}%)")
    lines.append("—" * 20)
    lines.append("<i>Use this to see which layer is the current bottleneck.</i>")

    send_telegram("\n".join(lines))

    # Reset for next interval
    _save_stats({"since": time.time(), "counts": {}})


# =============================================================================
#  SYMBOL VERIFICATION
# =============================================================================
async def verify_symbols() -> dict:
    valid_map = {}
    try:
        async with websockets.connect(WS_URI, ping_timeout=15, open_timeout=20) as ws:
            await ws.send(json.dumps({
                "active_symbols": "brief",
                "product_type": "basic",
            }))
            raw  = await asyncio.wait_for(ws.recv(), timeout=20)
            resp = json.loads(raw)

            if "error" in resp:
                log.error("active_symbols error: %s", resp["error"].get("message"))
                log.warning("Skipping verification — using SYMBOL_MAP as-is.")
                return dict(SYMBOL_MAP)

            all_symbols = {s["symbol"]: s.get("display_name", "")
                           for s in resp.get("active_symbols", [])}

            for display_name, deriv_symbol in SYMBOL_MAP.items():
                if deriv_symbol in all_symbols:
                    valid_map[display_name] = deriv_symbol
                    log.info("Verified %-7s -> %s (%s)",
                             display_name, deriv_symbol, all_symbols[deriv_symbol])
                else:
                    candidates = [
                        sym for sym, name in all_symbols.items()
                        if display_name.replace("USD", "").replace("JPY", "")[:3].upper()
                           in sym.upper()
                        or display_name.upper() in name.upper()
                    ]
                    log.warning(
                        "Symbol NOT FOUND: %s (tried '%s'). Possible matches: %s",
                        display_name, deriv_symbol,
                        ", ".join(candidates[:5]) if candidates else "none found"
                    )

            if not valid_map:
                log.error("No symbols verified! Falling back to SYMBOL_MAP as-is — scans may fail.")
                return dict(SYMBOL_MAP)

            return valid_map

    except Exception as e:
        log.error("Symbol verification failed: %s. Using SYMBOL_MAP as-is.", e)
        return dict(SYMBOL_MAP)


# =============================================================================
#  COOLDOWN — file-backed persistence
# =============================================================================
def _load_cooldown() -> dict:
    try:
        if os.path.exists(COOLDOWN_FILE):
            with open(COOLDOWN_FILE, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
    except (json.JSONDecodeError, OSError) as e:
        log.warning("Cooldown file unreadable (%s). Starting fresh.", e)
    return {}


def _save_cooldown(state: dict) -> None:
    try:
        tmp = COOLDOWN_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f)
        os.replace(tmp, COOLDOWN_FILE)
    except OSError as e:
        log.error("Failed to save cooldown file: %s", e)


def is_duplicate(key: str) -> bool:
    state = _load_cooldown()
    ts = state.get(key)
    if ts and (time.time() - ts) < COOLDOWN_SECS:
        remaining = int(COOLDOWN_SECS - (time.time() - ts)) // 60
        log.info("Cooldown active for %s — %d min remaining.", key, remaining)
        return True
    return False


def mark_sent(key: str) -> None:
    state = _load_cooldown()
    state[key] = time.time()
    cutoff = time.time() - 86400
    state = {k: v for k, v in state.items() if v > cutoff}
    _save_cooldown(state)
    log.info("Cooldown set for key: %s", key)


# =============================================================================
#  TELEGRAM
# =============================================================================
def send_telegram(message: str) -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        log.warning("Telegram not configured — TG_TOKEN or TG_CHAT_ID missing.")
        return
    try:
        url  = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id":    TG_CHAT_ID,
            "text":       message,
            "parse_mode": "HTML",
        }).encode()
        req  = urllib.request.Request(url, data=data)
        urllib.request.urlopen(req, timeout=10)
        log.info("Telegram alert sent.")
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        log.error("Telegram HTTP error %d: %s", e.code, body)
    except Exception as e:
        log.error("Telegram send failed: %s", e)


def build_alert(
    display_symbol, signal, entry, sl, tp1, tp2, risk, rr,
    score, rating, h4_bias, ob, fvg, zone, rsi_val, now, decimals,
) -> str:
    icon  = "🟢 <b>BUY (LONG)</b>"  if signal == "BUY" else "🔴 <b>SELL (SHORT)</b>"
    stars = (
        "🔥 PRIME"    if rating == "PRIME"  else
        "⭐⭐ STRONG" if rating == "STRONG" else
        "⭐ GOOD"     if rating == "GOOD"   else "✗ SKIP"
    )
    div = "—" * 20
    fmt = f"%.{decimals}f"
    return (
        f"{icon}  &#8212;  <b>OB + FVG Confluence [V3 TUNED]</b>\n"
        f"<code>{div}</code>\n"
        f"<b>Pair:</b>    {display_symbol}\n"
        f"<b>Rating:</b>  {stars}  ({score}/9)\n"
        f"<b>H4 Bias:</b> {h4_bias}\n"
        f"<b>Time:</b>    {now}\n"
        f"<code>{div}</code>\n"
        f"<b>Entry:</b>   {fmt % entry}\n"
        f"<b>SL:</b>      {fmt % sl}\n"
        f"<b>TP1:</b>     {fmt % tp1}  <i>(close 50%, move SL to BE)</i>\n"
        f"<b>TP2:</b>     {fmt % tp2}  <i>(1:{rr} RR)</i>\n"
        f"<b>Risk/pt:</b> {fmt % risk}\n"
        f"<code>{div}</code>\n"
        f"<b>H1 OB Zone:</b>   {fmt % ob['lo']} – {fmt % ob['hi']}\n"
        f"<b>M15 FVG Zone:</b> {fmt % fvg['lo']} – {fmt % fvg['hi']}\n"
        f"<b>Entry Zone:</b>   {fmt % zone['lo']} – {fmt % zone['hi']}\n"
        f"<b>RSI(14):</b>      {round(rsi_val, 1)}\n"
        f"<code>{div}</code>\n"
        f"<i>H4 trend + H1 OB + M15 FVG (proximity) + M5 entry all confirmed.</i>"
    )


# =============================================================================
#  WEBSOCKET — candle fetcher
# =============================================================================
async def fetch_candles(deriv_symbol: str, granularity: int, count: int) -> Optional[pd.DataFrame]:
    try:
        async with websockets.connect(WS_URI, ping_timeout=15, open_timeout=20) as ws:
            await ws.send(json.dumps({
                "ticks_history":   deriv_symbol,
                "adjust_start_time": 1,
                "count":           count,
                "end":             "latest",
                "style":           "candles",
                "granularity":     granularity,
            }))
            raw  = await asyncio.wait_for(ws.recv(), timeout=20)
            resp = json.loads(raw)

            if "error" in resp:
                log.error("Deriv API error for %s (%ds): %s",
                          deriv_symbol, granularity, resp["error"].get("message", "unknown"))
                return None

            candles = resp.get("candles")
            if not candles:
                log.error("No candle data returned for %s (%ds).", deriv_symbol, granularity)
                return None

            df = pd.DataFrame(candles)
            df.rename(columns={"open": "Open", "high": "High",
                                "low": "Low",  "close": "Close"}, inplace=True)
            for col in ["Open", "High", "Low", "Close"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["Time"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
            df.set_index("Time", inplace=True)
            df.drop(columns=["epoch"], inplace=True)

            if df.isnull().any().any():
                log.warning("%s (%ds): NaN values present after parse.", deriv_symbol, granularity)

            return df

    except asyncio.TimeoutError:
        log.error("%s (%ds): WebSocket recv timed out.", deriv_symbol, granularity)
    except websockets.exceptions.WebSocketException as e:
        log.error("%s (%ds): WebSocket error — %s", deriv_symbol, granularity, e)
    except Exception as e:
        log.error("%s (%ds): Unexpected fetch error — %s", deriv_symbol, granularity, e)
    return None


# =============================================================================
#  INDICATORS
# =============================================================================
def ema(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False).mean()


def atr_series(df: pd.DataFrame, n: int = 14) -> pd.Series:
    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - df["Close"].shift(1)).abs(),
        (df["Low"]  - df["Close"].shift(1)).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=n, adjust=False).mean()


def body_ratio(df: pd.DataFrame) -> pd.Series:
    rng = (df["High"] - df["Low"]).replace(0.0, float("nan"))
    return (df["Close"] - df["Open"]).abs() / rng


def calc_rsi(series: pd.Series, n: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0).ewm(span=n, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(span=n, adjust=False).mean()
    rs    = gain / loss.replace(0.0, float("nan"))
    return 100 - (100 / (1 + rs))


def calc_adx(df: pd.DataFrame, n: int = 14) -> pd.Series:
    up   = df["High"].diff()
    down = -df["Low"].diff()
    pdm  = pd.Series(0.0, index=df.index)
    ndm  = pd.Series(0.0, index=df.index)
    pdm[up > down]   = up[up > down].clip(lower=0)
    ndm[down > up]   = down[down > up].clip(lower=0)
    atr_ = atr_series(df, n)
    safe = atr_.replace(0.0, float("nan"))
    pdi  = 100 * pdm.ewm(span=n, adjust=False).mean() / safe
    ndi  = 100 * ndm.ewm(span=n, adjust=False).mean() / safe
    denom = (pdi + ndi).replace(0.0, float("nan"))
    dx   = 100 * (pdi - ndi).abs() / denom
    return dx.ewm(span=n, adjust=False).mean()


# =============================================================================
#  LAYER 1 — H4 TREND BIAS
# =============================================================================
def get_h4_bias(h4: pd.DataFrame) -> Tuple[str, float]:
    df = h4.copy()
    df["E21"] = ema(df["Close"], 21)
    df["E50"] = ema(df["Close"], 50)
    df["ADX"] = calc_adx(df, 14)

    cur  = df.iloc[-1]
    prev = df.iloc[-3]
    adx_val = float(cur["ADX"])

    bullish = (
        cur["E21"]   > cur["E50"]
        and cur["Close"] > cur["E21"]
        and cur["E21"]   > prev["E21"]
        and cur["E50"]   > prev["E50"]
    )
    bearish = (
        cur["E21"]   < cur["E50"]
        and cur["Close"] < cur["E21"]
        and cur["E21"]   < prev["E21"]
        and cur["E50"]   < prev["E50"]
    )

    if bullish:
        return "BULLISH", adx_val
    if bearish:
        return "BEARISH", adx_val
    return "NEUTRAL", adx_val


# =============================================================================
#  LAYER 2 — H1 ORDER BLOCK (fresh + unmitigated, per-pair settings)
# =============================================================================
def find_ob(h1: pd.DataFrame, bias: str, display_symbol: str) -> Optional[dict]:
    """
    Find the most recent unmitigated H1 Order Block.
    Uses per-pair OB settings from PER_PAIR_OB if defined for the symbol,
    otherwise falls back to global OB_LOOKBACK / OB_MIN_BODY / OB_MAX_AGE.

    Per-pair tuning is applied to XAUUSD, USDJPY, GBPJPY based on live
    diagnostics showing those pairs die at H1 OB 79-100% of scans due to
    impulsive price behaviour leaving smaller/fewer qualifying OBs.
    """
    pair_cfg  = PER_PAIR_OB.get(display_symbol, {})
    lookback  = pair_cfg.get("lookback", OB_LOOKBACK)
    min_body  = pair_cfg.get("min_body", OB_MIN_BODY)
    max_age   = pair_cfg.get("max_age",  OB_MAX_AGE)

    df = h1.copy()
    df["BR"]      = body_ratio(df)
    df["IsBull"]  = df["Close"] > df["Open"]
    df["IsBear"]  = df["Close"] < df["Open"]
    df["BullMSS"] = df["Close"] > df["High"].shift(1).rolling(5).max()
    df["BearMSS"] = df["Close"] < df["Low"].shift(1).rolling(5).min()

    lkb = df.iloc[-lookback:]

    if bias == "BULLISH":
        mss_candles = lkb[lkb["BullMSS"]].index.tolist()
        for mss_idx in reversed(mss_candles):
            pool = lkb.loc[:mss_idx].iloc[:-1]
            pool = pool[pool["IsBear"] & (pool["BR"] >= min_body)]
            if pool.empty:
                continue
            ob_row  = pool.iloc[-1]
            hi      = max(float(ob_row["Open"]), float(ob_row["Close"]))
            lo      = min(float(ob_row["Open"]), float(ob_row["Close"]))
            wick    = float(ob_row["Low"])
            post_ob = df.loc[ob_row.name:]
            if post_ob["Low"].min() < wick:
                continue
            age = len(post_ob)
            if age > max_age:
                continue
            return {
                "hi":   round(hi,   6),
                "lo":   round(lo,   6),
                "wick": round(wick, 6),
                "age":  age,
                "time": ob_row.name,
            }

    elif bias == "BEARISH":
        mss_candles = lkb[lkb["BearMSS"]].index.tolist()
        for mss_idx in reversed(mss_candles):
            pool = lkb.loc[:mss_idx].iloc[:-1]
            pool = pool[pool["IsBull"] & (pool["BR"] >= min_body)]
            if pool.empty:
                continue
            ob_row  = pool.iloc[-1]
            hi      = max(float(ob_row["Open"]), float(ob_row["Close"]))
            lo      = min(float(ob_row["Open"]), float(ob_row["Close"]))
            wick    = float(ob_row["High"])
            post_ob = df.loc[ob_row.name:]
            if post_ob["High"].max() > wick:
                continue
            age = len(post_ob)
            if age > max_age:
                continue
            return {
                "hi":   round(hi,   6),
                "lo":   round(lo,   6),
                "wick": round(wick, 6),
                "age":  age,
                "time": ob_row.name,
            }

    return None


# =============================================================================
#  LAYER 3 — M15 FAIR VALUE GAP near the OB zone (LOOSENED: proximity, not overlap)
# =============================================================================
def find_fvg_near_ob(m15: pd.DataFrame, bias: str, ob: dict) -> Optional[dict]:
    """
    Scan M15 for a Fair Value Gap that is within proximity of the H1 OB
    zone — not necessarily overlapping it. This is the key loosening
    from v1: live logs showed OBs frequently going unmitigated for
    15+ bars without an FVG ever literally overlapping the OB body,
    even when FVGs were forming just outside it. Proximity is more
    representative of how SMC traders actually read confluence by eye.
    """
    df   = m15.copy().reset_index()
    min_size = FVG_MIN_PCT / 100
    ob_mid   = (ob["hi"] + ob["lo"]) / 2
    proximity_buf = ob_mid * (FVG_OB_PROXIMITY_PCT / 100)

    # Expanded zone: OB zone padded by the proximity buffer on both sides
    zone_lo = ob["lo"] - proximity_buf
    zone_hi = ob["hi"] + proximity_buf

    if bias == "BULLISH":
        for i in range(len(df) - 1, 1, -1):
            gap_lo = float(df.iloc[i - 2]["High"])
            gap_hi = float(df.iloc[i]["Low"])
            gap_sz = gap_hi - gap_lo

            if gap_sz <= 0:
                continue
            ref_price = float(df.iloc[i]["Close"])
            if (gap_sz / ref_price) < min_size:
                continue
            # Proximity check: any part of the FVG falls within the padded zone
            if gap_hi >= zone_lo and gap_lo <= zone_hi:
                return {
                    "lo":  round(gap_lo, 6),
                    "hi":  round(gap_hi, 6),
                    "mid": round((gap_lo + gap_hi) / 2, 6),
                    "time": df.iloc[i]["Time"],
                }

    elif bias == "BEARISH":
        for i in range(len(df) - 1, 1, -1):
            gap_hi = float(df.iloc[i - 2]["Low"])
            gap_lo = float(df.iloc[i]["High"])
            gap_sz = gap_hi - gap_lo

            if gap_sz <= 0:
                continue
            ref_price = float(df.iloc[i]["Close"])
            if (gap_sz / ref_price) < min_size:
                continue
            if gap_hi >= zone_lo and gap_lo <= zone_hi:
                return {
                    "lo":  round(gap_lo, 6),
                    "hi":  round(gap_hi, 6),
                    "mid": round((gap_lo + gap_hi) / 2, 6),
                    "time": df.iloc[i]["Time"],
                }

    return None


# =============================================================================
#  LAYER 4 — M5 ENTRY inside confluence zone
# =============================================================================
def check_m5_entry(
    m5: pd.DataFrame,
    bias: str,
    zone_lo: float,
    zone_hi: float,
    adx_val: float,
) -> Tuple[Optional[str], int, float, float]:
    df = m5.copy()
    df["RSI"] = calc_rsi(df["Close"], 14)
    df["BR"]  = body_ratio(df)
    df["ATR"] = atr_series(df, 7)

    last    = df.iloc[-1]
    price   = float(last["Close"])
    hi_c    = float(last["High"])
    lo_c    = float(last["Low"])
    rsi_val = float(last["RSI"])
    br_val  = float(last["BR"])
    atr_val = float(last["ATR"])
    rng     = hi_c - lo_c

    tol = (zone_hi - zone_lo) * 0.5
    in_zone = (zone_lo - tol) <= price <= (zone_hi + tol)
    if not in_zone:
        return None, 0, rsi_val, atr_val

    if rng == 0:
        log.warning("M5 last candle has zero range — skipping entry check.")
        return None, 0, rsi_val, atr_val

    score  = 0
    signal = None

    if bias == "BULLISH":
        lower_wick  = float(last["Close"]) - lo_c
        wick_ratio  = lower_wick / rng
        if wick_ratio < MIN_WICK:
            return None, 0, rsi_val, atr_val
        if rsi_val > RSI_OB:
            return None, 0, rsi_val, atr_val
        signal = "BUY"
        score += 1
        score += 1
        score += 1
        if adx_val  >= ADX_MIN:  score += 1
        if rsi_val  <  60:       score += 1
        if br_val   >= BODY_MIN: score += 1
        if wick_ratio >= 0.40:   score += 1
        if rsi_val  <  50:       score += 1
        if br_val   >= 0.55:     score += 1

    elif bias == "BEARISH":
        upper_wick  = hi_c - float(last["Close"])
        wick_ratio  = upper_wick / rng
        if wick_ratio < MIN_WICK:
            return None, 0, rsi_val, atr_val
        if rsi_val < RSI_OS:
            return None, 0, rsi_val, atr_val
        signal = "SELL"
        score += 1
        score += 1
        score += 1
        if adx_val  >= ADX_MIN:  score += 1
        if rsi_val  >  40:       score += 1
        if br_val   >= BODY_MIN: score += 1
        if wick_ratio >= 0.40:   score += 1
        if rsi_val  >  50:       score += 1
        if br_val   >= 0.55:     score += 1

    return signal, min(score, 9), rsi_val, atr_val


# =============================================================================
#  TRADE PLAN
# =============================================================================
def calc_trade(signal: str, entry: float, sl_level: float,
               score: int, atr_val: float) -> dict:
    rr  = round(RR_MIN + (score / 9) * (RR_MAX - RR_MIN), 2)
    buf = atr_val * ATR_BUF
    if signal == "BUY":
        sl   = round(sl_level - buf, 6)
        risk = round(entry - sl,    6)
        tp1  = round(entry + risk * 1.0, 6)
        tp2  = round(entry + risk * rr,  6)
    else:
        sl   = round(sl_level + buf, 6)
        risk = round(sl - entry,     6)
        tp1  = round(entry - risk * 1.0, 6)
        tp2  = round(entry - risk * rr,  6)
    return {"sl": sl, "tp1": tp1, "tp2": tp2, "risk": risk, "rr": rr}


def signal_rating(score: int) -> str:
    if score >= 7: return "PRIME"
    if score >= 5: return "STRONG"
    if score >= 3: return "GOOD"
    return "SKIP"


# =============================================================================
#  SCAN ONE SYMBOL
# =============================================================================
async def scan_symbol(display_symbol: str, deriv_symbol: str) -> None:
    now_str = datetime.now(WAT).strftime("%Y-%m-%d %H:%M:%S WAT")
    log.info("Scanning %s (%s) ...", display_symbol, deriv_symbol)

    h4, h1, m15, m5 = await asyncio.gather(
        fetch_candles(deriv_symbol, H4_TF,  H4_COUNT),
        fetch_candles(deriv_symbol, H1_TF,  H1_COUNT),
        fetch_candles(deriv_symbol, M15_TF, M15_COUNT),
        fetch_candles(deriv_symbol, M5_TF,  M5_COUNT),
    )

    if any(x is None for x in (h4, h1, m15, m5)):
        log.error("%s — one or more timeframes failed to fetch. Skipping.", display_symbol)
        return

    decimals = DECIMALS.get(display_symbol, 5)

    # Layer 1: H4 trend
    bias, adx_val = get_h4_bias(h4)
    log.info("%s | H4 bias: %-8s | ADX: %.1f", display_symbol, bias, adx_val)

    if bias == "NEUTRAL":
        log.info("%s | Neutral trend — no trade.", display_symbol)
        record_rejection(display_symbol, "H4_neutral_or_weak")
        return
    if adx_val < ADX_MIN:
        log.info("%s | ADX too weak (%.1f < %.1f) — skipping.", display_symbol, adx_val, ADX_MIN)
        record_rejection(display_symbol, "H4_neutral_or_weak")
        return

    # Layer 2: H1 Order Block (uses per-pair settings for XAUUSD/USDJPY/GBPJPY)
    ob = find_ob(h1, bias, display_symbol)
    if not ob:
        log.info("%s | No valid H1 OB found.", display_symbol)
        record_rejection(display_symbol, "H1_no_ob")
        return
    pair_cfg = PER_PAIR_OB.get(display_symbol, {})
    log.info("%s | H1 OB: %.6f – %.6f  (age: %d bars, min_body: %.1f, lookback: %d)",
             display_symbol, ob["lo"], ob["hi"], ob["age"],
             pair_cfg.get("min_body", OB_MIN_BODY),
             pair_cfg.get("lookback", OB_LOOKBACK))

    # Layer 3: M15 FVG near OB (proximity-based, not strict overlap)
    fvg = find_fvg_near_ob(m15, bias, ob)
    if not fvg:
        log.info("%s | No M15 FVG within proximity of H1 OB zone.", display_symbol)
        record_rejection(display_symbol, "M15_no_fvg")
        return
    log.info("%s | M15 FVG: %.6f – %.6f", display_symbol, fvg["lo"], fvg["hi"])

    # Confluence zone — union of OB and FVG range (since they may not strictly overlap now)
    zone_lo = min(ob["lo"], fvg["lo"])
    zone_hi = max(ob["hi"], fvg["hi"])
    log.info("%s | Confluence zone: %.6f – %.6f", display_symbol, zone_lo, zone_hi)

    # Layer 4: M5 entry confirmation
    signal, score, rsi_val, atr_val = check_m5_entry(
        m5, bias, zone_lo, zone_hi, adx_val
    )

    if not signal:
        log.info("%s | No M5 entry confirmation. RSI: %.1f", display_symbol, rsi_val)
        record_rejection(display_symbol, "M5_no_entry")
        return

    r = signal_rating(score)
    if r == "SKIP":
        log.info("%s | Signal score too low (%d/9) — skipping.", display_symbol, score)
        record_rejection(display_symbol, "M5_no_entry")
        return

    entry  = round(float(m5.iloc[-1]["Close"]), 6)
    cd_key = f"{display_symbol}_{signal}_{round(entry, decimals)}"

    if is_duplicate(cd_key):
        return

    trade = calc_trade(signal, entry, ob["wick"], score, atr_val)
    zone_dict = {"lo": round(zone_lo, 6), "hi": round(zone_hi, 6)}

    log.info(
        "%s | SIGNAL: %s | %s (%d/9) | Entry: %.6f | SL: %.6f",
        display_symbol, signal, r, score, entry, trade["sl"],
    )

    msg = build_alert(
        display_symbol=display_symbol, signal=signal,
        entry=entry,        sl=trade["sl"],
        tp1=trade["tp1"],   tp2=trade["tp2"],
        risk=trade["risk"], rr=trade["rr"],
        score=score,        rating=r,
        h4_bias=bias,       ob=ob,
        fvg=fvg,            zone=zone_dict,
        rsi_val=rsi_val,    now=now_str,
        decimals=decimals,
    )
    send_telegram(msg)
    mark_sent(cd_key)
    record_rejection(display_symbol, "signal_fired")


# =============================================================================
#  MAIN LIVE LOOP
# =============================================================================
async def main() -> None:
    start_time = datetime.now(WAT).strftime("%Y-%m-%d %H:%M:%S WAT")
    log.info("=" * 60)
    log.info("Forex SMC+ICT OB+FVG Scanner v3 (per-pair OB tuning)  |  Started %s", start_time)
    log.info("Global OB: lookback=%d min_body=%.1f max_age=%d  FVG_proximity=%.2f%%",
             OB_LOOKBACK, OB_MIN_BODY, OB_MAX_AGE, FVG_OB_PROXIMITY_PCT)
    log.info("Per-pair OB overrides: %s", PER_PAIR_OB)
    log.info("=" * 60)

    active_map = await verify_symbols()
    if not active_map:
        log.error("No valid symbols to scan. Exiting.")
        return

    log.info("Active pairs: %s", ", ".join(active_map.keys()))

    send_telegram(
        f"🚀 <b>Forex SMC+ICT OB+FVG Scanner [V3 TUNED] LIVE</b>\n"
        f"<b>Pairs:</b> {', '.join(active_map.keys())}\n"
        f"<b>Model:</b> H4 Trend → H1 OB (per-pair) → M15 FVG (proximity) → M5 Entry\n"
        f"<b>Started:</b> {start_time}\n"
        f"<i>Per-pair OB tuning applied to XAUUSD, USDJPY, GBPJPY.</i>\n"
        f"<i>EURUSD and GBPUSD settings unchanged from v2.</i>\n"
        f"<i>Daily diagnostics summary will post every 24h.</i>"
    )

    cycle = 0
    while True:
        cycle += 1
        cycle_start = time.time()
        log.info("─── Scan cycle #%d  [%s] ───",
                 cycle, datetime.now(WAT).strftime("%H:%M:%S WAT"))

        for display_symbol, deriv_symbol in active_map.items():
            try:
                await scan_symbol(display_symbol, deriv_symbol)
            except Exception as e:
                log.error("Unhandled error scanning %s: %s", display_symbol, e, exc_info=True)
            await asyncio.sleep(3)

        maybe_post_stats_summary()

        elapsed = time.time() - cycle_start
        wait    = max(0.0, SCAN_INTERVAL - elapsed)
        log.info("Cycle #%d done in %.1fs. Next scan in %.0fs.", cycle, elapsed, wait)
        await asyncio.sleep(wait)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Scanner stopped by user.")
