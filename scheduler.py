"""
Daily scheduler — starts three threads each day:

  1. creation-watch  (09:27 UTC) — REST polls until markets found
  2. ws-watcher      (09:27 UTC) — WebSocket: 'activated' → batch bid (PRIMARY)
  3. timer-fallback  (13:59 UTC) — fires at 14:00:05 UTC only if WS missed it

Uses APScheduler to re-arm all three threads every day at 09:27 UTC.
"""
from __future__ import annotations
import threading

from apscheduler.schedulers.background import BackgroundScheduler

import state


def _start_creation_watch():
    from market_watcher import run_creation_watch
    threading.Thread(target=run_creation_watch,
                     name="creation-watch", daemon=True).start()


def _start_ws_watcher():
    from market_watcher import run_ws_watcher
    threading.Thread(target=run_ws_watcher,
                     name="ws-watcher", daemon=True).start()


def _start_open_trigger():
    from market_watcher import run_open_trigger
    threading.Thread(target=run_open_trigger,
                     name="timer-fallback", daemon=True).start()


def _daily_rearm():
    """
    Called at 09:27 UTC each day.
    Resets the bid-fired flag, then starts creation watch + WS watcher.
    """
    state.reset_bids_fired()
    _start_creation_watch()
    _start_ws_watcher()


def start() -> None:
    sched = BackgroundScheduler(timezone="UTC")

    # 09:27 UTC: reset flag + start creation watch + WS watcher
    sched.add_job(_daily_rearm, "cron",
                  hour=9, minute=27, id="daily_rearm")

    # 13:59 UTC: start timer fallback (fires at 14:00:05 if WS missed)
    sched.add_job(_start_open_trigger, "cron",
                  hour=13, minute=59, id="timer_fallback")

    sched.start()
    print(
        "[scheduler] Armed —\n"
        "  09:27 UTC  creation watch + WS watcher\n"
        "  13:59 UTC  timer fallback (fires 14:00:05 UTC if WS path missed)"
    )

    # Fire immediately if we're already in the window
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)

    if now.hour == 9 and now.minute >= 27:
        print("[scheduler] In creation window — starting watch + WS now")
        _daily_rearm()
    elif now.hour >= 10 and now.hour < 14:
        # Markets found but not open yet — start WS watcher to catch activation
        print("[scheduler] Post-creation window — starting WS watcher for open")
        _start_ws_watcher()
        # If we restarted inside the 13:59 window the APScheduler job may have
        # been missed — start the timer fallback directly as well.
        if now.hour == 13 and now.minute >= 59:
            print("[scheduler] Near open — also starting timer fallback")
            _start_open_trigger()
