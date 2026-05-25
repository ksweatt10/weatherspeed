"""Shared in-memory state — dashboard, background threads, and WS client."""
from __future__ import annotations
import threading
import time

_lock  = threading.Lock()
_state = {
    "watch_phase":         "IDLE",   # IDLE|WATCHING|READY|ARMED|FIRING
    "discovered_markets":  {},       # {event_ticker: [market, ...]}
    "last_bid_run":        [],       # results from most recent bid cycle
    "server_ts":           0.0,
    "errors":              [],
    "bids_fired_today":    False,    # prevents WS + timer double-fire
    # ── WebSocket live state ──────────────────────────────────────────────────
    "ws_connected":        False,
    "ws_tickers":          0,        # number of tickers subscribed
    "ws_last_msg_ts":      0.0,      # epoch of last WS message received
    "ws_prices":           {},       # {ticker: {yes_bid_dollars, yes_ask_dollars,
                                     #            open_interest_fp, volume_fp, ts_ms}}
    "first_bids":          {},       # {ticker: ts_ms} — first OI>0 transition per ticker
}


# ── Generic getters ───────────────────────────────────────────────────────────

def get_all() -> dict:
    with _lock:
        return dict(_state)

def set_watch_phase(phase: str) -> None:
    with _lock:
        _state["watch_phase"] = phase
        _state["server_ts"]   = time.time()

def set_discovered_markets(markets: dict) -> None:
    with _lock:
        _state["discovered_markets"] = markets
        _state["server_ts"]          = time.time()

def get_discovered_markets() -> dict:
    with _lock:
        return dict(_state["discovered_markets"])

def set_last_bid_run(results: list) -> None:
    with _lock:
        _state["last_bid_run"] = results
        _state["server_ts"]    = time.time()

# ── Bid-fired flag (atomic, prevents double-fire) ────────────────────────────

def claim_bids_fired() -> bool:
    """Set bids_fired_today atomically. Returns True only for the first caller."""
    with _lock:
        if _state["bids_fired_today"]:
            return False
        _state["bids_fired_today"] = True
        _state["server_ts"]        = time.time()
        return True

def reset_bids_fired() -> None:
    with _lock:
        _state["bids_fired_today"] = False

# ── WebSocket status (updated by ws_client) ───────────────────────────────────

def set_ws_connected(connected: bool, tickers: int = 0) -> None:
    with _lock:
        _state["ws_connected"] = connected
        _state["ws_tickers"]   = tickers
        _state["server_ts"]    = time.time()

def update_ws_ticker(ticker: str, data: dict) -> None:
    """Called by ws_client on every ticker message. Mirrors WS state for dashboard."""
    with _lock:
        _state["ws_prices"][ticker] = data
        _state["ws_last_msg_ts"]    = time.time()

def get_ws_prices() -> dict:
    with _lock:
        return dict(_state["ws_prices"])

def get_ws_status() -> dict:
    with _lock:
        return {
            "connected":   _state["ws_connected"],
            "tickers":     _state["ws_tickers"],
            "last_msg_ts": _state["ws_last_msg_ts"],
        }

# ── First-bid detection (OI 0 → non-zero via WS ticker) ──────────────────────

def record_first_bid(ticker: str, ts_ms: int) -> bool:
    """
    Record the first time OI goes non-zero on a ticker.
    Returns True if this was genuinely new (not already recorded).
    """
    with _lock:
        if ticker in _state["first_bids"]:
            return False
        _state["first_bids"][ticker] = ts_ms
        return True

def get_first_bids() -> dict:
    with _lock:
        return dict(_state["first_bids"])

# ── Errors ────────────────────────────────────────────────────────────────────

def add_error(msg: str) -> None:
    with _lock:
        _state["errors"].append({"ts": time.strftime("%H:%M:%S"), "msg": msg})
        if len(_state["errors"]) > 50:
            _state["errors"] = _state["errors"][-50:]
