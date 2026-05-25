"""
Daily scheduler — starts two threads each day:
  1. Creation watch thread (wakes at 09:28 UTC, polls until markets found)
  2. Open trigger thread   (fires at exactly 14:00:00 UTC)

Uses APScheduler to rearm both threads every day at 09:27 UTC.
"""
from __future__ import annotations
import threading

from apscheduler.schedulers.background import BackgroundScheduler


def _start_creation_watch():
    from market_watcher import run_creation_watch
    t = threading.Thread(target=run_creation_watch,
                         name="creation-watch", daemon=True)
    t.start()


def _start_open_trigger():
    from market_watcher import run_open_trigger
    t = threading.Thread(target=run_open_trigger,
                         name="open-trigger", daemon=True)
    t.start()


def start() -> None:
    sched = BackgroundScheduler(timezone="UTC")

    # Creation watch: start at 09:27 UTC daily so we're polling before 09:30
    sched.add_job(_start_creation_watch, "cron",
                  hour=9, minute=27, id="creation_watch")

    # Open trigger: start at 13:59 UTC daily (sleeps 60s internally to hit exactly 14:00)
    sched.add_job(_start_open_trigger, "cron",
                  hour=13, minute=59, id="open_trigger")

    sched.start()
    print("[scheduler] Armed — creation watch at 09:27 UTC, "
          "open trigger at 13:59 UTC (fires at 14:00:00 UTC)")

    # Also fire immediately if we're in the window right now
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)

    if now.hour == 9 and now.minute >= 27:
        print("[scheduler] In creation window — starting watch now")
        _start_creation_watch()
    elif now.hour == 13 and now.minute >= 59:
        print("[scheduler] Near open — starting trigger now")
        _start_open_trigger()
