"""
Runtime-tunable settings — read/write from dashboard without restarting.
"""
from __future__ import annotations
import threading

_lock = threading.Lock()

_defaults: dict = {
    # ── Bidding ──────────────────────────────────────────────────────────────
    "dry_run":              True,    # True = evaluate only, never place real orders
    "auto_bid_enabled":     True,    # actually fire orders at market open
    "bid_only_zero_oi":     True,    # only bid when open_interest == 0
    "dollars_per_bucket":   0.0,     # $ to risk per bucket (0 = use contracts_per_market)
    "contracts_per_market": 1,       # fallback: fixed contracts if dollars_per_bucket=0
    "max_no_price_cents":   70,      # skip NO bids above this price (cents)
    "min_no_price_cents":   50,      # skip NO bids below this price (too risky)
    "batch_size":           30,      # orders per batch POST (Kalshi limit ~30)
    "batch_concurrency":    3,       # max simultaneous batch requests in flight
    "batch_inter_round_ms": 0,       # ms sleep between rounds (0 = RTT-only spacing)

    # ── Timing ───────────────────────────────────────────────────────────────
    # Markets created daily ~09:30–09:31 UTC; open exactly 14:00:00 UTC
    "creation_poll_start_utc_hour":   9,   # start watching for new markets
    "creation_poll_start_utc_minute": 28,
    "creation_poll_interval_secs":    5,   # poll every N seconds until found
    "open_time_utc_hour":             14,  # market open (always exactly 14:00)
    "open_time_utc_minute":           0,

    # ── Research ─────────────────────────────────────────────────────────────
    "track_market_timing":  True,    # record creation/open times to DB
}

_config: dict = dict(_defaults)


def get(key: str, default=None):
    with _lock:
        return _config.get(key, default)


def set(key: str, value) -> None:
    with _lock:
        _config[key] = value


def all_settings() -> dict:
    with _lock:
        return dict(_config)
