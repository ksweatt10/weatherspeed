"""
Market Watcher — three phases every day:

Phase 1 — CREATION WATCH (starts 09:27 UTC):
  Poll the Kalshi events API every few seconds until new markets appear
  (~09:30:47–09:31:19 UTC historically).
  Records creation times to DB for research tab.

Phase 2 — WS WATCHER (starts 09:27 UTC, runs alongside Phase 1):
  Opens WebSocket and subscribes to market_lifecycle_v2 globally.
  'created' events (~09:31) → subscribe ticker channel for those markets.
  'activated' event (14:00:00:000 UTC) → read ws_state, fire ONE batch POST.
  This is the primary bid trigger — zero REST calls at open.

Phase 3 — TIMER FALLBACK (starts 13:59 UTC):
  If WS hasn't fired by 14:00:05 UTC, fall back to REST fetch + batch POST.
  Guards against WS disconnection or missed events.
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
    return (_utcnow() + timedelta(hours=-4)).date().isoformat()

def _next_utc_hhmm(hour: int, minute: int) -> datetime:
    now    = _utcnow()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target


# ── Phase 1: REST creation watch ──────────────────────────────────────────────

async def _discover_todays_markets() -> dict[str, list[dict]]:
    """
    Poll Kalshi events API for each series, return today's markets.
    {event_ticker: [market_dict, ...]}
    """
    today_str = _today_et_str()
    utc_date  = _utcnow().date().isoformat()
    found: dict[str, list] = {}

    poll_secs   = runtime_config.get("creation_poll_interval_secs", 5)
    series_list = [s[0] for s in config.WEATHER_SERIES]
    series_map  = {s[0]: s for s in config.WEATHER_SERIES}

    async with SpeedClient() as client:

        async def _fetch_series(series_ticker: str):
            try:
                events = await client.list_events(series_ticker, status="open")
                return series_ticker, events
            except Exception as e:
                print(f"[watcher] {series_ticker} error: {e}")
                return series_ticker, []

        tasks   = [_fetch_series(s) for s in series_list]
        results = await asyncio.gather(*tasks)

        today_events: list[tuple[str, str, str, str]] = []
        for series_ticker, events in results:
            series_info = series_map.get(series_ticker)
            if not series_info:
                continue
            _, city, kind = series_info
            for evt in events:
                et = evt.get("event_ticker", "")
                today_events.append((et, series_ticker, city, kind))

        if not today_events:
            return {}

        event_tickers = [e[0] for e in today_events]
        market_map    = await client.get_all_markets_for_events(event_tickers)

        for et, series_ticker, city, kind in today_events:
            markets = market_map.get(et, [])
            if not markets:
                continue

            m0           = markets[0]
            created_time = m0.get("created_time", "")
            open_time    = m0.get("open_time", "")
            settlement   = m0.get("occurrence_datetime", "")[:10]

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
    Block until creation window (09:28 UTC), poll until markets found.
    Runs in its own thread.
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
          f"{runtime_config.get('creation_poll_interval_secs', 5)}s")

    deadline = _utcnow() + timedelta(minutes=15)
    while _utcnow() < deadline:
        found = asyncio.run(_discover_todays_markets())
        if found:
            state.set_watch_phase("READY")
            return
        time.sleep(runtime_config.get("creation_poll_interval_secs", 5))

    print("[watcher] Creation watch timed out — no markets found")
    state.set_watch_phase("NO_MARKETS")


# ── Phase 2: WebSocket watcher (primary bid trigger) ──────────────────────────

async def _run_ws_watcher_async() -> None:
    """
    Long-lived async WS watcher.
    Connects at 09:27 UTC, stays live until bids fire at 14:00:00 UTC.
    """
    from kalshi.ws_client import SpeedWSClient
    from speed_bidder import run_bids

    async def _on_activated(ticker: str) -> None:
        """
        Fires on first 'activated' event from Kalshi WS (~14:00:00.000 UTC).
        state.claim_bids_fired() is atomic — only one caller wins.
        """
        if not state.claim_bids_fired():
            print(f"[ws_watcher] activated {ticker} — bids already fired, skip")
            return

        print(
            f"[ws_watcher] ACTIVATED {ticker} — "
            f"firing batch at {_utcnow().strftime('%H:%M:%S.%f UTC')}"
        )
        await run_bids(ws_state=ws_client.ws_state)

    ws_client = SpeedWSClient(on_market_open=_on_activated)

    # Background task: sync ticker subscriptions from REST-discovered markets.
    # Handles the case where REST discovery finishes before/after WS connects.
    async def _sync_tickers_loop() -> None:
        while True:
            await asyncio.sleep(10)
            known = state.get_discovered_markets()
            tickers = [
                m.get("ticker", "")
                for mkts in known.values()
                for m in mkts
                if m.get("ticker")
            ]
            if tickers:
                await ws_client.subscribe_tickers(tickers)

    # Run WS client and ticker-sync loop concurrently
    await asyncio.gather(
        ws_client.run(),
        _sync_tickers_loop(),
    )


def run_ws_watcher() -> None:
    """Thread entry point — runs the async WS watcher in its own event loop."""
    asyncio.run(_run_ws_watcher_async())


# ── Phase 3: Timer fallback ───────────────────────────────────────────────────

def run_open_trigger() -> None:
    """
    FALLBACK ONLY — fires at 14:00:05 UTC if WS hasn't already triggered.

    The 5-second delay gives the WS path every chance to fire first.
    If WS fired (state.claim_bids_fired() returns False), this is a no-op.
    """
    from speed_bidder import run_bids

    oh     = runtime_config.get("open_time_utc_hour",   14)
    om     = runtime_config.get("open_time_utc_minute",  0)
    target = _next_utc_hhmm(oh, om) + timedelta(seconds=5)  # +5s safety margin
    wait   = (target - _utcnow()).total_seconds()

    print(f"[watcher] Timer fallback armed — fires at "
          f"{target.strftime('%H:%M:%S UTC')} ({wait:.1f}s)")
    state.set_watch_phase("ARMED")

    if wait > 0:
        time.sleep(wait)

    if not state.claim_bids_fired():
        print(f"[watcher] FALLBACK TRIGGER — WS path missed, firing REST bids "
              f"at {_utcnow().strftime('%H:%M:%S.%f UTC')}")
        state.set_watch_phase("FIRING")
        asyncio.run(run_bids(ws_state=None))
    else:
        print("[watcher] Timer fallback: WS already fired — nothing to do")
        state.set_watch_phase("IDLE")
