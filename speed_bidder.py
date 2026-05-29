"""
Speed Bidder — fires at market open (14:00:00 UTC).

Strategy: buy YES at 1¢ on every bucket across all 33 series.
  - 6 buckets per series, exactly 1 resolves YES → $1.00 payout
  - Cost: 6 × $0.01 = $0.06 per series
  - Net profit: $0.94 per series if all 6 fill
  - 33 series × $0.94 = ~$31/day at 1 contract

No price fetching needed — fixed 1¢ YES limit orders (GTC) on all tickers.
Orders sit in the book all day and fill as NO buyers arrive.
"""
from __future__ import annotations
import asyncio
import time
from datetime import datetime, timezone

import runtime_config
import state
from db.models import insert_bid, log_event, upsert_open_snapshot
from kalshi.speed_client import SpeedClient

# Pre-warmed client — set by prewarm_for_open(), consumed by run_bids()
_warm_client: SpeedClient | None = None


async def prewarm_for_open() -> None:
    """
    Open the SpeedClient session ~10s before market open and fire a dummy
    GET to complete the TCP + TLS handshake.  Saves ~30ms on the very first
    order at 14:00:00 by reusing an already-established connection.
    Called by scheduler at 13:59:50 UTC.
    """
    global _warm_client
    client = SpeedClient()
    await client.__aenter__()
    try:
        await client._get("/portfolio/orders", params={"limit": "1"})
        print("[bidder] Pre-warm OK — TCP+TLS ready for 14:00:00")
    except Exception as exc:
        print(f"[bidder] Pre-warm warning (non-fatal): {exc}")
    _warm_client = client


def _today_et() -> str:
    from datetime import timedelta
    return (datetime.now(timezone.utc) + timedelta(hours=-4)).date().isoformat()


async def _snapshot_markets(client, discovered: dict, snapshot_at: str) -> int:
    """
    Fetch live REST prices for all discovered markets concurrently with bid placement.
    Stores open_yes_ask, open_no_ask, open_oi, open_snapshot_at into market_buckets.
    Fires at the same moment as YES bids via asyncio.gather — zero added latency.
    """
    event_tickers = list(discovered.keys())
    try:
        market_map = await client.get_all_markets_for_events(
            event_tickers, concurrency=10)
    except Exception as exc:
        print(f"[bidder] snapshot fetch error: {exc}")
        return 0

    count = 0
    for et, markets in market_map.items():
        for m in markets:
            ticker  = m.get("ticker", "")
            yes_ask = float(m.get("yes_ask_dollars")  or 0)
            no_ask  = float(m.get("no_ask_dollars")   or 0)
            oi      = float(m.get("open_interest_fp") or 0)
            if ticker:
                upsert_open_snapshot(ticker, yes_ask, no_ask, oi, snapshot_at)
                count += 1

    print(f"[bidder] open snapshot saved {count} prices at {snapshot_at}")
    return count


async def run_bids() -> None:
    """
    Main entry point — called at market open.
    Places YES limit orders at 1¢ on all discovered market tickers.
    No WS state or live price fetching needed.
    """
    today = _today_et()

    _oh = runtime_config.get("open_time_utc_hour",  14)
    _om = runtime_config.get("open_time_utc_minute", 0)
    _d  = datetime.now(timezone.utc).date()
    t_open = datetime(_d.year, _d.month, _d.day, _oh, _om, 0,
                      tzinfo=timezone.utc).timestamp()

    dry_run         = runtime_config.get("dry_run",              True)
    contracts       = runtime_config.get("contracts_per_market", 1)
    auto_bid        = runtime_config.get("auto_bid_enabled",     True)
    inter_order_ms  = runtime_config.get("inter_order_ms",       40)
    yes_price_cents = runtime_config.get("yes_price_cents",       1)
    bid_strategy    = runtime_config.get("bid_strategy",  "wave_batch")

    if not auto_bid:
        print("[bidder] auto_bid_enabled=False — skipping")
        state.set_watch_phase("IDLE")
        return

    # ── Get discovered markets ────────────────────────────────────────────────
    discovered = state.get_discovered_markets()
    if not discovered:
        print("[bidder] No markets in state — re-fetching via REST")
        from market_watcher import _discover_todays_markets
        discovered = await _discover_todays_markets()

    if not discovered:
        print("[bidder] Still no markets — aborting")
        log_event(today, "BID_ERROR", "No markets found at open")
        state.set_watch_phase("IDLE")
        return

    all_markets = [
        (et, m)
        for et, markets in discovered.items()
        for m in markets
    ]
    total = len(all_markets)

    print(f"[bidder] {'DRY RUN' if dry_run else 'LIVE'} [{bid_strategy}] — "
          f"YES at {yes_price_cents}¢ on {total} buckets across {len(discovered)} series "
          f"({contracts} contracts each)")

    # ── Fire batch YES bids + open snapshot concurrently ─────────────────────
    t_bid       = time.perf_counter()
    snapshot_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    global _warm_client
    client = _warm_client
    _warm_client = None
    if client is not None:
        print("[bidder] Using pre-warmed connection")
    else:
        print("[bidder] No pre-warm — opening fresh connection")
        client = SpeedClient()
        await client.__aenter__()

    try:
        if bid_strategy == "sequential":
            bid_coro = client.individual_yes_bids(
                all_markets,
                contracts       = contracts,
                yes_price_cents = yes_price_cents,
                dry_run         = dry_run,
                inter_order_ms  = inter_order_ms,
                t_open          = t_open,
            )
        else:  # "wave_batch" — sequential batches of 30, tail retry on 429 (~300ms)
            bid_coro = client.batch_yes_bids(
                all_markets,
                contracts         = contracts,
                yes_price_cents   = yes_price_cents,
                dry_run           = dry_run,
                batch_size        = 30,
                batch_concurrency = 1,
                inter_round_ms    = 0,
                t_open            = t_open,
            )
        results, _snap_count = await asyncio.gather(
            bid_coro,
            _snapshot_markets(client, discovered, snapshot_at),
        )
    finally:
        await client.__aexit__(None, None, None)

    ms_bid = round((time.perf_counter() - t_bid) * 1000)

    # ── Persist all results ───────────────────────────────────────────────────
    et_map    = {m.get("ticker", ""): et for et, m in all_markets}
    city_map  = {m.get("ticker", ""): m.get("city", "")
                 for _, m in all_markets}
    label_map = {m.get("ticker", ""): (
                     m.get("no_sub_title") or m.get("yes_sub_title", "")
                 )
                 for _, m in all_markets}

    placed  = sum(1 for r in results if r.get("placed"))
    skipped = sum(1 for r in results if not r.get("placed"))

    for r in results:
        ticker = r.get("ticker", "")
        insert_bid(
            date           = today,
            event_ticker   = et_map.get(ticker, ""),
            ticker         = ticker,
            city           = city_map.get(ticker, ""),
            bucket_label   = label_map.get(ticker, ""),
            contracts      = contracts,
            no_price_cents = yes_price_cents,
            open_interest  = r.get("open_interest", 0),
            was_first      = r.get("was_first", False),
            dry_run        = dry_run,
            order_id       = r.get("order_id"),
            status         = ("placed" if r.get("placed")
                              else "skip:" + (r.get("error") or "")),
            ms_after_open  = r.get("ms_elapsed"),
            bid_engine_ms  = r.get("engine_ms"),
            side           = "yes",
        )

    summary = (f"placed={placed} skipped={skipped} bid_ms={ms_bid} "
               f"series={len(discovered)} buckets={total}")
    print(f"[bidder] Done — {summary}")
    log_event(today, "BIDS_PLACED", summary)
    state.set_last_bid_run(results)
    state.set_watch_phase("IDLE")
