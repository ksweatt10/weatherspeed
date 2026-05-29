#!/usr/bin/env python3
"""
BTC Above/Below Speed Bidder — one-shot script.

Discovers all buckets for a KXBTCD event, pre-warms at T-10s,
then fires 2¢ YES orders on every bucket at exact market open time.

Usage:
    python btc_bidder.py                              # auto-discover next open KXBTCD event
    python btc_bidder.py KXBTCD-26MAY3017             # target specific event
    python btc_bidder.py KXBTCD-26MAY3017 --dry-run   # evaluate only, no live orders
    python btc_bidder.py KXBTCD-26MAY3017 --contracts 3 --price 2
"""
from __future__ import annotations
import argparse
import asyncio
import sys
import time
from datetime import datetime, timezone, timedelta

from db.models import init_db, insert_bid, log_event
from kalshi.speed_client import SpeedClient

_BTC_SERIES = "KXBTCD"
_DEFAULT_PRICE_CENTS = 2
_DEFAULT_CONTRACTS   = 5
_PREWARM_LEAD_SECS   = 10    # pre-warm this many seconds before open


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _today_et() -> str:
    return (_utcnow() + timedelta(hours=-4)).date().isoformat()


def _parse_open_time(iso: str) -> datetime:
    """Parse ISO timestamp from Kalshi API → timezone-aware UTC datetime."""
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


async def discover_btc_event(client: SpeedClient,
                              event_ticker: str | None) -> tuple[str, list[dict]]:
    """
    If event_ticker is given, fetch its markets directly.
    Otherwise, discover the next open KXBTCD event automatically.

    Returns (event_ticker, [market_dict, ...])
    """
    if event_ticker:
        # Fetch markets for specified event regardless of status
        markets = await client.get_markets_for_event(event_ticker, status=None)
        if not markets:
            raise RuntimeError(f"No markets found for event {event_ticker}")
        return event_ticker, markets

    # Auto-discover: look for initialized or open KXBTCD events
    for status in (None, "open"):
        events = await client.list_events(_BTC_SERIES, status=status, limit=10)
        if events:
            break
    if not events:
        raise RuntimeError(f"No {_BTC_SERIES} events found")

    # Pick the first upcoming event (soonest open_time in the future)
    now_iso = _utcnow().isoformat()
    upcoming = []
    for evt in events:
        et = evt.get("event_ticker", "")
        # Fetch its markets to get open_time
        try:
            mkts = await client.get_markets_for_event(et, status=None)
        except Exception:
            continue
        if not mkts:
            continue
        ot = mkts[0].get("open_time", "")
        if ot and ot >= now_iso:
            upcoming.append((ot, et, mkts))

    if not upcoming:
        raise RuntimeError(f"No upcoming {_BTC_SERIES} events found")

    upcoming.sort(key=lambda x: x[0])
    ot, et, mkts = upcoming[0]
    return et, mkts


async def _run(event_ticker: str | None,
               dry_run: bool,
               contracts: int,
               yes_price_cents: int) -> None:

    init_db()
    today = _today_et()

    # ── 1. Discover markets ───────────────────────────────────────────────────
    print(f"[btc_bidder] Discovering markets (event={event_ticker or 'auto'}) ...")
    async with SpeedClient() as client:
        et, markets = await discover_btc_event(client, event_ticker)

    if not markets:
        print("[btc_bidder] ERROR: no markets found — aborting")
        sys.exit(1)

    open_time_str = markets[0].get("open_time", "")
    if not open_time_str:
        print("[btc_bidder] ERROR: no open_time on market — aborting")
        sys.exit(1)

    open_dt   = _parse_open_time(open_time_str)
    close_str = markets[0].get("close_time", "")
    n         = len(markets)

    print(f"[btc_bidder] Event   : {et}")
    print(f"[btc_bidder] Buckets : {n}")
    print(f"[btc_bidder] Open    : {open_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}  "
          f"({(open_dt + timedelta(hours=-4)).strftime('%H:%M ET')})")
    print(f"[btc_bidder] Close   : {close_str}")
    print(f"[btc_bidder] Strategy: {'DRY RUN' if dry_run else 'LIVE'} "
          f"YES@{yes_price_cents}¢ × {contracts} contracts × {n} buckets "
          f"= ${yes_price_cents * contracts * n / 100:.2f} cost")

    # ── 2. Sleep until pre-warm window ───────────────────────────────────────
    prewarm_dt = open_dt - timedelta(seconds=_PREWARM_LEAD_SECS)
    wait_prewarm = (prewarm_dt - _utcnow()).total_seconds()

    if wait_prewarm > 60:
        print(f"[btc_bidder] Sleeping {wait_prewarm:.0f}s until pre-warm at "
              f"{prewarm_dt.strftime('%H:%M:%S UTC')} ...")
        time.sleep(wait_prewarm - 1)   # wake up 1s early for precision loop

    # Precision spin until pre-warm time
    while _utcnow() < prewarm_dt:
        time.sleep(0.05)

    # ── 3. Pre-warm — open connection + dummy GET to establish TCP/TLS ────────
    print(f"[btc_bidder] Pre-warming connection at "
          f"{_utcnow().strftime('%H:%M:%S.%f UTC')[:-3]} ...")
    warm_client = SpeedClient()
    await warm_client.__aenter__()
    try:
        await warm_client._get("/portfolio/orders", params={"limit": "1"})
        print("[btc_bidder] Pre-warm OK — TCP+TLS ready")
    except Exception as exc:
        print(f"[btc_bidder] Pre-warm warning (non-fatal): {exc}")

    # ── 4. Precision sleep until exact open ───────────────────────────────────
    while _utcnow() < open_dt:
        time.sleep(0.001)   # 1ms spin loop for < 10s remaining

    fire_ts = _utcnow()
    print(f"[btc_bidder] FIRING at {fire_ts.strftime('%H:%M:%S.%f UTC')[:-3]}")

    # Wall-clock reference for ms_elapsed timing
    t_open_wall = open_dt.timestamp()

    # ── 5. Fire all bids ──────────────────────────────────────────────────────
    # Wrap markets as (event_ticker, market_dict) tuples so batch_yes_bids
    # can read them the same way as the weather bidder
    market_tuples = [(et, m) for m in markets]

    try:
        # 80 buckets → 3 batches of 26-27 orders, all fired concurrently via HTTP/2
        results = await warm_client.batch_yes_bids(
            markets           = market_tuples,
            contracts         = contracts,
            yes_price_cents   = yes_price_cents,
            dry_run           = dry_run,
            batch_size        = 30,
            batch_concurrency = 3,   # all batches in parallel
            inter_round_ms    = 0,
            t_open            = t_open_wall,
        )
    finally:
        await warm_client.__aexit__(None, None, None)

    # ── 6. Persist results to bid_log ─────────────────────────────────────────
    label_map = {
        m.get("ticker", ""): (m.get("no_sub_title") or m.get("yes_sub_title", ""))
        for m in markets
    }

    placed  = sum(1 for r in results if r.get("placed"))
    skipped = sum(1 for r in results if not r.get("placed"))

    for r in results:
        ticker = r.get("ticker", "")
        insert_bid(
            date           = today,
            event_ticker   = et,
            ticker         = ticker,
            city           = "BTC",
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

    summary = (f"event={et} placed={placed} skipped={skipped} "
               f"buckets={n} price={yes_price_cents}c contracts={contracts} "
               f"{'DRY' if dry_run else 'LIVE'}")
    print(f"[btc_bidder] Done — {summary}")
    log_event(today, "BTC_BIDS_PLACED", summary)


def main() -> None:
    parser = argparse.ArgumentParser(description="BTC above/below speed bidder")
    parser.add_argument("event_ticker", nargs="?", default=None,
                        help="e.g. KXBTCD-26MAY3017  (default: auto-discover)")
    parser.add_argument("--dry-run",   action="store_true",
                        help="Evaluate only — no live orders placed")
    parser.add_argument("--contracts", type=int, default=_DEFAULT_CONTRACTS,
                        help=f"Contracts per bucket (default: {_DEFAULT_CONTRACTS})")
    parser.add_argument("--price",     type=int, default=_DEFAULT_PRICE_CENTS,
                        help=f"YES limit price in cents (default: {_DEFAULT_PRICE_CENTS})")
    args = parser.parse_args()

    asyncio.run(_run(
        event_ticker    = args.event_ticker,
        dry_run         = args.dry_run,
        contracts       = args.contracts,
        yes_price_cents = args.price,
    ))


if __name__ == "__main__":
    main()
