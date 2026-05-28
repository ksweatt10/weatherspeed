"""
Daily scheduler — three phases each day:

  1. creation-watch  (09:27 UTC) — REST polls until today's markets appear
  2. ws-watcher      (24/7)      — WebSocket: 'activated' → batch bid (PRIMARY)
  3. timer-fallback  (13:59 UTC) — fires at 14:00:05 UTC only if WS missed it

WS watcher runs continuously with auto-reconnect (exponential backoff).
Daily rearm at 09:27 UTC resets the bid-fired flag and starts creation watch.
The WS watcher is only (re)started if its thread has died.
"""
from __future__ import annotations
import threading
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler

import state

# Module-level reference so we can check is_alive() before re-spawning
_ws_thread: threading.Thread | None = None


def _start_creation_watch():
    from market_watcher import run_creation_watch
    threading.Thread(target=run_creation_watch,
                     name="creation-watch", daemon=True).start()


def _start_ws_watcher():
    global _ws_thread
    from market_watcher import run_ws_watcher
    _ws_thread = threading.Thread(target=run_ws_watcher,
                                  name="ws-watcher", daemon=True)
    _ws_thread.start()
    print("[scheduler] WS watcher thread started")


def _ensure_ws_watcher():
    """Start the WS watcher only if it isn't already running."""
    global _ws_thread
    if _ws_thread is None or not _ws_thread.is_alive():
        _start_ws_watcher()
    else:
        print("[scheduler] WS watcher already running — no restart needed")


def _start_open_trigger():
    from market_watcher import run_open_trigger
    threading.Thread(target=run_open_trigger,
                     name="timer-fallback", daemon=True).start()


def _daily_rearm():
    """
    Called at 09:27 UTC each day.
    Resets the bid-fired flag, starts creation watch,
    and ensures WS watcher is alive (restarts only if dead).
    """
    state.reset_bids_fired()
    _start_creation_watch()
    _ensure_ws_watcher()


def _run_order_poll():
    from order_poller import run_poll
    run_poll()


def start() -> None:
    sched = BackgroundScheduler(timezone="UTC")

    # 09:27 UTC: reset bid-fired flag + start creation watch + ensure WS alive
    sched.add_job(_daily_rearm, "cron",
                  hour=9, minute=27, id="daily_rearm")

    # 13:59 UTC: start timer fallback (fires at 14:00:05 if WS missed)
    sched.add_job(_start_open_trigger, "cron",
                  hour=13, minute=59, id="timer_fallback")

    # Every 30 min: sync order status + settlement from Kalshi
    sched.add_job(_run_order_poll, "interval",
                  minutes=30, id="order_poll")

    sched.start()

    # ── Always start WS watcher at boot ──────────────────────────────────────
    # ws_client.run() has full auto-reconnect; it stays live indefinitely.
    _ensure_ws_watcher()

    # ── Boot: sync orders immediately (fixes any bad bid_log rows) ───────────
    from order_poller import start_background_poll
    start_background_poll()

    now = datetime.now(timezone.utc)

    # ── Boot catch-up: timer fallback only ───────────────────────────────────
    # Markets are populated exclusively by the creation watch at 09:27 UTC.
    # No boot discovery — we only want today's fresh markets, not stale ones.

    # Missed the 13:59 timer-fallback job while down — start it now if applicable
    if now.hour == 13 and now.minute >= 59:
        print("[scheduler] Near open — starting timer fallback now")
        _start_open_trigger()

    print(
        "[scheduler] Armed —\n"
        "  WS watcher: 24/7 (started at boot, auto-reconnects)\n"
        "  09:27 UTC  creation watch + bids-fired reset\n"
        "  13:59 UTC  timer fallback (fires 14:00:05 UTC if WS path missed)"
    )
