"""
Runtime-tunable settings — read/write from dashboard without restarting.
"""
from __future__ import annotations
import threading

_lock = threading.Lock()

_defaults: dict = {
    # ── Bidding ──────────────────────────────────────────────────────────────
    "bot_enabled":          True,    # master kill-switch — False = bot does nothing
    "dry_run":              False,   # False = live orders; True = evaluate only
    "auto_bid_enabled":     True,    # actually fire orders at market open
    "contracts_per_market": 5,       # YES contracts per bucket (cost = N × $0.02 at 2¢)
    "inter_order_ms":       0,       # ms sleep between orders — 0 = rely on natural 13ms RTT
    "yes_price_cents":      2,       # limit price per YES order (2¢ = best fill rate)
    "bid_strategy":         "sequential",  # sequential (optimal ~7.9s) or wave_batch

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
