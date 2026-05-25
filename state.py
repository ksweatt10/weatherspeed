"""Shared in-memory state for dashboard and background threads."""
from __future__ import annotations
import threading
import time

_lock  = threading.Lock()
_state = {
    "watch_phase":         "IDLE",    # IDLE | WATCHING | READY | ARMED | FIRING
    "discovered_markets":  {},        # {event_ticker: [market, ...]}
    "last_bid_run":        [],        # results from most recent bid cycle
    "server_ts":           0.0,
    "errors":              [],
}


def get_all() -> dict:
    with _lock:
        return dict(_state)

def set_watch_phase(phase: str) -> None:
    with _lock:
        _state["watch_phase"]  = phase
        _state["server_ts"]    = time.time()

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

def add_error(msg: str) -> None:
    with _lock:
        _state["errors"].append({"ts": time.strftime("%H:%M:%S"), "msg": msg})
        if len(_state["errors"]) > 50:
            _state["errors"] = _state["errors"][-50:]
