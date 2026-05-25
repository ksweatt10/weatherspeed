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

async def run_research_backfill(days: int = 7) -> dict:
    """
    Pull up to `days` worth of settled market data for all 32 series.
    Stores creation time, open time, and settlement date into market_timing
    and market_buckets tables so the Research tab has historical data.

    Returns a summary dict: {inserted, skipped, series_errors}
    """
    from db.models import upsert_market_timing, upsert_market_bucket
    today_str   = _today_et_str()
    series_list = config.WEATHER_SERIES          # list of (ticker, city, kind)
    series_map  = {s[0]: s for s in series_list}

    inserted = 0
    skipped  = 0
    errors   = []

    async with SpeedClient() as client:
        # Throttle backfill to 5 concurrent series to avoid 429 rate limits.
        # This is a background operation — taking a few extra seconds is fine.
        _sem = asyncio.Semaphore(5)

        event_limit = days + 3   # fetch a few extra so [:days] always has enough

        async def _fetch_settled(series_ticker: str):
            async with _sem:
                try:
                    # settled events cover past days; open covers today/upcoming
                    settled = await client.list_events(
                        series_ticker, status="settled", limit=event_limit)
                    open_ev = await client.list_events(
                        series_ticker, status="open",    limit=3)
                    return series_ticker, settled + open_ev
                except Exception as e:
                    return series_ticker, e

        tasks   = [_fetch_settled(s[0]) for s in series_list]
        results = await asyncio.gather(*tasks)

        for series_ticker, result in results:
            if isinstance(result, Exception):
                errors.append(f"{series_ticker}: {result}")
                continue

            series_info   = series_map.get(series_ticker)
            if not series_info:
                continue
            _, city, kind = series_info

            # Limit to most recent `days` events
            events = result[:days]
            if not events:
                continue

            event_tickers = [e.get("event_ticker", "") for e in events]
            # market_status=None → no status filter → returns finalized/settled markets too
            market_map    = await client.get_all_markets_for_events(
                event_tickers, concurrency=3, market_status=None)

            for evt in events:
                et      = evt.get("event_ticker", "")
                markets = market_map.get(et, [])
                if not markets:
                    skipped += 1
                    continue

                m0           = markets[0]
                created_time = m0.get("created_time", "")
                open_time    = m0.get("open_time",    "")
                settlement   = m0.get("occurrence_datetime", "")[:10] or \
                               m0.get("expiration_time",     "")[:10] or ""

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
                    inserted += 1

    summary = f"backfill done: {inserted} events inserted, {skipped} skipped, {len(errors)} errors"
    print(f"[watcher] {summary}")
    log_event(today_str, "BACKFILL", summary)
    return {"inserted": inserted, "skipped": skipped, "errors": errors}


async def pull_first_trades_for_open_markets(overwrite: bool = False) -> dict:
    """
    For every bucket in the currently-discovered open markets (today + tomorrow),
    paginate GET /markets/trades to find the oldest trade and store its timestamp
    in market_buckets.first_bid_time (and market_timing.first_bid_time).

    Concurrency is capped at 5 simultaneous requests to stay well under rate limits.
    Pagination is only needed when a market has >100 trades; most buckets have
    far fewer, so a single page suffices.

    Args:
        overwrite: if True, re-fetch even buckets that already have a first_bid_time.
                   Defaults to False (skip already-populated rows).

    Returns summary dict: {pulled, no_trades, already_set, errors, tickers}
    """
    from db.models import upsert_first_trade_time, get_bucket_timing

    today_str = _today_et_str()
    discovered = state.get_discovered_markets()
    if not discovered:
        return {"pulled": 0, "no_trades": 0, "already_set": 0,
                "errors": ["no markets discovered — run Refresh Markets first"],
                "tickers": 0}

    # Collect all tickers across discovered events
    tickers: list[str] = [
        m.get("ticker", "")
        for mkts in discovered.values()
        for m in mkts
        if m.get("ticker")
    ]

    if not overwrite:
        # Skip buckets that already have a first_bid_time in the DB
        from db.models import _conn as _db_conn
        with _db_conn() as con:
            placeholders = ",".join("?" * len(tickers))
            rows = con.execute(
                f"SELECT ticker FROM market_buckets "
                f"WHERE ticker IN ({placeholders}) AND first_bid_time IS NOT NULL",
                tickers
            ).fetchall()
        already_done = {r[0] for r in rows}
        tickers = [t for t in tickers if t not in already_done]
        already_set = len(already_done)
    else:
        already_set = 0

    if not tickers:
        return {"pulled": 0, "no_trades": 0, "already_set": already_set,
                "errors": [], "tickers": 0}

    pulled    = 0
    no_trades = 0
    errors:   list[str] = []

    _sem = asyncio.Semaphore(5)   # 5 concurrent pages at a time

    async with SpeedClient() as client:
        async def _fetch_one(ticker: str):
            async with _sem:
                try:
                    trade = await client.get_first_trade_for_ticker(ticker)
                    return ticker, trade, None
                except Exception as e:
                    return ticker, None, str(e)

        tasks   = [_fetch_one(t) for t in tickers]
        results = await asyncio.gather(*tasks)

    for ticker, trade, err in results:
        if err:
            errors.append(f"{ticker}: {err}")
        elif trade is None:
            no_trades += 1
        else:
            iso_ts = trade.get("created_time", "")
            if iso_ts:
                upsert_first_trade_time(ticker, iso_ts)
                pulled += 1
            else:
                no_trades += 1

    summary = (f"first trades: {pulled} pulled, {no_trades} no trades, "
               f"{already_set} already set, {len(errors)} errors")
    print(f"[watcher] {summary}")
    log_event(today_str, "FIRST_TRADES", summary)
    return {"pulled": pulled, "no_trades": no_trades, "already_set": already_set,
            "errors": errors, "tickers": len(tickers)}


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
