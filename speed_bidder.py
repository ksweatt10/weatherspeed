"""
Speed Bidder — fires at market open (14:00:00 UTC).

Processes markets in Z→A city order (config.WEATHER_SERIES is pre-sorted).
Places NO bids concurrently on all qualifying markets.
Records every bid (or dry-run bid) to the DB.
"""
from __future__ import annotations
import asyncio
import time
from datetime import datetime, timezone

import runtime_config
import state
from db.models import insert_bid, log_event
from kalshi.speed_client import SpeedClient


def _today_et() -> str:
    from datetime import timedelta
    return (datetime.now(timezone.utc) + timedelta(hours=-4)).date().isoformat()


async def run_bids() -> None:
    """Main entry point — called at market open."""
    today      = _today_et()
    dry_run    = runtime_config.get("dry_run", True)
    contracts  = runtime_config.get("contracts_per_market", 1)
    max_no     = runtime_config.get("max_no_price_cents", 99)
    min_no     = runtime_config.get("min_no_price_cents", 50)
    only_zero  = runtime_config.get("bid_only_zero_oi", True)
    auto_bid   = runtime_config.get("auto_bid_enabled", True)

    if not auto_bid:
        print("[bidder] auto_bid_enabled=False, skipping")
        state.set_watch_phase("IDLE")
        return

    # Get markets discovered earlier by watcher
    discovered = state.get_discovered_markets()
    if not discovered:
        print("[bidder] No markets in state — re-fetching at open")
        from market_watcher import _discover_todays_markets
        discovered = await _discover_todays_markets()

    if not discovered:
        print("[bidder] Still no markets found — aborting")
        log_event(today, "BID_ERROR", "No markets found at open")
        state.set_watch_phase("IDLE")
        return

    # Flatten all markets into one list (already Z→A from config order)
    all_markets = []
    for event_ticker, markets in discovered.items():
        for m in markets:
            all_markets.append((event_ticker, m))

    total = len(all_markets)
    print(f"[bidder] {'DRY RUN' if dry_run else 'LIVE'} — "
          f"bidding NO on up to {total} markets (Z→A)")

    t_open = time.perf_counter()

    async with SpeedClient() as client:
        # Re-fetch live market data to get current open_interest and no_ask
        # (state data may be slightly stale from creation watch)
        tickers     = [m.get("ticker","") for _, m in all_markets]
        fresh_tasks = [client.get_market(t) for t in tickers]
        fresh       = await asyncio.gather(*fresh_tasks, return_exceptions=True)

        live_markets = []
        et_map       = {m.get("ticker",""): et for et, m in all_markets}
        city_map     = {}
        label_map    = {}
        for _, m in all_markets:
            t = m.get("ticker","")
            city_map[t]  = m.get("city", "")   # fallback to state data
            label_map[t] = (m.get("no_sub_title") or m.get("yes_sub_title", ""))

        for ticker, result in zip(tickers, fresh):
            if isinstance(result, Exception):
                print(f"[bidder] refresh error {ticker}: {result}")
                continue
            live_markets.append(result)

        ms_to_fetch = round((time.perf_counter() - t_open) * 1000)
        print(f"[bidder] Fetched {len(live_markets)} live markets in {ms_to_fetch}ms")

        # Place all bids concurrently
        t_bid = time.perf_counter()
        results = await client.bid_no_all(
            live_markets,
            contracts  = contracts,
            max_no_cents = max_no,
            min_no_cents = min_no,
            only_zero_oi = only_zero,
            dry_run      = dry_run,
        )
        ms_bid = round((time.perf_counter() - t_bid) * 1000)

        # Tally and persist
        placed  = sum(1 for r in results if r.get("placed"))
        firsts  = sum(1 for r in results if r.get("was_first") and r.get("placed"))
        skipped = sum(1 for r in results if not r.get("placed"))

        for r in results:
            ticker = r.get("ticker","")
            et     = et_map.get(ticker, "")
            insert_bid(
                date         = today,
                event_ticker = et,
                ticker       = ticker,
                city         = city_map.get(ticker, ""),
                bucket_label = label_map.get(ticker, ""),
                contracts    = contracts,
                no_price_cents = r.get("no_ask_cents", 0),
                open_interest  = r.get("open_interest", 0),
                was_first      = r.get("was_first", False),
                dry_run        = dry_run,
                order_id       = r.get("order_id"),
                status         = "placed" if r.get("placed") else ("skip:" + (r.get("error") or "")),
                ms_after_open  = r.get("ms_elapsed"),
            )

        summary = (f"placed={placed} firsts={firsts} skipped={skipped} "
                   f"fetch_ms={ms_to_fetch} bid_ms={ms_bid}")
        print(f"[bidder] Done — {summary}")
        log_event(today, "BIDS_PLACED", summary)
        state.set_last_bid_run(results)
        state.set_watch_phase("IDLE")
