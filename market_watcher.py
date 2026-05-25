"""
Market Watcher — two phases every day:

Phase 1 — CREATION WATCH (starts 09:28 UTC):
  Poll the Kalshi events API every few seconds until new markets appear
  (historically created at 09:30:47–09:31:19 UTC).
  Record creation times to DB for research tab.

Phase 2 — OPEN TRIGGER (exactly 14:00:00 UTC):
  At market open, fire the speed bidder immediately.

Both phases run as daemon threads started by scheduler.py.
"""
from __future__ import annotations
import asyncio
import threading
import time
from datetime import datetime, timezone, timedelta

import config
import runtime_config
import state
from db.models import upsert_market_timing, upsert_market_bucket, log_event
from kalshi.speed_client import SpeedClient

# ── Helpers ───────────────────────────────────────────────────────────────────

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _today_et_str() -> str:
    """Return today's date in ET as YYYY-MM-DD."""
    et_offset = timedelta(hours=-4)          # EDT; close enough for logging
    return (_utcnow() + et_offset).date().isoformat()

def _next_utc_hhmm(hour: int, minute: int) -> datetime:
    """Return the next occurrence of HH:MM UTC (today or tomorrow)."""
    now  = _utcnow()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target


# ── Phase 1: creation watch ───────────────────────────────────────────────────

async def _discover_todays_markets() -> dict[str, list[dict]]:
    """
    Poll Kalshi events for each series until all new markets are found.
    Returns {event_ticker: [market_dict, ...]} for markets opening today.
    """
    today_str = _today_et_str()
    # Kalshi open_time is 14:00 UTC on the creation day
    # so "today's markets" = open_time starts with today's UTC date
    utc_date  = _utcnow().date().isoformat()
    found: dict[str, list] = {}

    poll_secs = runtime_config.get("creation_poll_interval_secs", 5)

    async with SpeedClient() as client:
        # First fetch all open events across all series concurrently
        series_list = [s[0] for s in config.WEATHER_SERIES]

        async def _fetch_series(series_ticker: str):
            try:
                events = await client.list_events(series_ticker, status="open")
                return series_ticker, events
            except Exception as e:
                print(f"[watcher] {series_ticker} error: {e}")
                return series_ticker, []

        tasks   = [_fetch_series(s) for s in series_list]
        results = await asyncio.gather(*tasks)

        # Find events opening today (open_time starts with today UTC date)
        today_events: list[tuple[str, str, str, str]] = []  # (event_ticker, series, city, kind)
        series_map   = {s[0]: s for s in config.WEATHER_SERIES}

        for series_ticker, events in results:
            series_info = series_map.get(series_ticker)
            if not series_info:
                continue
            _, city, kind = series_info
            for evt in events:
                et        = evt.get("event_ticker", "")
                open_time = evt.get("strike_date", "")     # approximate; buckets have exact
                today_events.append((et, series_ticker, city, kind))

        if not today_events:
            return {}

        # Fetch all buckets for today's events concurrently
        event_tickers = [e[0] for e in today_events]
        market_map    = await client.get_all_markets_for_events(event_tickers)

        # Store timing data and update state
        for et, series_ticker, city, kind in today_events:
            markets = market_map.get(et, [])
            if not markets:
                continue

            # Get creation / open time from first bucket
            m0           = markets[0]
            created_time = m0.get("created_time", "")
            open_time    = m0.get("open_time", "")
            settlement   = m0.get("occurrence_datetime", "")[:10]

            # Only store if this is a "tomorrow settlement" market
            # (open_time = today at 14:00 UTC)
            if not open_time.startswith(utc_date):
                continue

            if runtime_config.get("track_market_timing", True):
                upsert_market_timing(et, series_ticker, city, kind,
                                     settlement, created_time, open_time)
                for mkt in markets:
                    upsert_market_bucket(
                        et, mkt.get("ticker", ""),
                        mkt.get("no_sub_title") or mkt.get("yes_sub_title", ""),
                        mkt.get("floor_strike"), mkt.get("cap_strike"),
                        mkt.get("created_time", ""), mkt.get("open_time", ""),
                    )

            found[et] = markets

        state.set_discovered_markets(found)
        total_buckets = sum(len(v) for v in found.values())
        print(f"[watcher] Discovered {len(found)} events / "
              f"{total_buckets} markets for {utc_date}")
        log_event(today_str, "MARKETS_DISCOVERED",
                  f"{len(found)} events, {total_buckets} buckets")
        return found


def run_creation_watch() -> None:
    """
    Block until the creation window (09:28 UTC), then poll until markets found.
    Called in its own thread.
    """
    ph_hour = runtime_config.get("creation_poll_start_utc_hour",   9)
    ph_min  = runtime_config.get("creation_poll_start_utc_minute", 28)
    target  = _next_utc_hhmm(ph_hour, ph_min)
    wait    = (target - _utcnow()).total_seconds()
    if wait > 0:
        print(f"[watcher] Sleeping {wait:.0f}s until creation watch at "
              f"{target.strftime('%H:%M UTC')}")
        time.sleep(wait)

    print("[watcher] Starting creation watch — polling every "
          f"{runtime_config.get('creation_poll_interval_secs',5)}s")

    deadline = _utcnow() + timedelta(minutes=15)  # give up after 15 min
    while _utcnow() < deadline:
        found = asyncio.run(_discover_todays_markets())
        if found:
            state.set_watch_phase("READY")
            return
        time.sleep(runtime_config.get("creation_poll_interval_secs", 5))

    print("[watcher] Creation watch timed out — no markets found")
    state.set_watch_phase("NO_MARKETS")


# ── Phase 2: open trigger ─────────────────────────────────────────────────────

def run_open_trigger() -> None:
    """
    Sleep until exactly 14:00:00 UTC, then fire speed_bidder.
    Called in its own thread.
    """
    from speed_bidder import run_bids

    oh   = runtime_config.get("open_time_utc_hour",   14)
    om   = runtime_config.get("open_time_utc_minute",  0)
    target = _next_utc_hhmm(oh, om)
    wait   = (target - _utcnow()).total_seconds()

    print(f"[watcher] Open trigger armed — firing in {wait:.1f}s "
          f"at {target.strftime('%H:%M:%S UTC')}")
    state.set_watch_phase("ARMED")

    if wait > 0:
        time.sleep(wait)

    # Fire immediately at open
    print(f"[watcher] MARKET OPEN — launching bids at "
          f"{_utcnow().strftime('%H:%M:%S.%f UTC')}")
    state.set_watch_phase("FIRING")
    asyncio.run(run_bids())
