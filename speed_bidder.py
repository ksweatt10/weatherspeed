"""
Speed Bidder — fires at market open (14:00:00 UTC).

Primary path (WS trigger):
  ws_state is passed in from the WebSocket client — no REST fetch needed.
  no_ask derived from yes_bid: no_ask_cents = 100 - yes_bid_cents
  All qualifying orders sent in ONE batch POST.

Fallback path (timer trigger):
  ws_state=None → re-fetch all markets via REST, then batch POST.

Both paths record every bid (or dry-run bid) to the DB.
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


def _no_ask_from_ws(ws_data: dict) -> int:
    """
    Derive NO ask cents from WS ticker state.
    In a binary market: NO ask = 1.00 - YES bid
    """
    yes_bid = float(ws_data.get("yes_bid_dollars", "0") or "0")
    return max(0, 100 - round(yes_bid * 100))


async def run_bids(ws_state: dict | None = None) -> None:
    """
    Main entry point — called at market open.

    ws_state: live ticker state from SpeedWSClient.ws_state
              {ticker: {yes_bid_dollars, yes_ask_dollars, open_interest_fp, ...}}
              Pass None to fall back to REST market fetch.
    """
    today     = _today_et()
    dry_run   = runtime_config.get("dry_run",               True)
    contracts = runtime_config.get("contracts_per_market",  1)
    max_no    = runtime_config.get("max_no_price_cents",     70)
    min_no    = runtime_config.get("min_no_price_cents",     50)
    only_zero = runtime_config.get("bid_only_zero_oi",       True)
    auto_bid  = runtime_config.get("auto_bid_enabled",       True)

    if not auto_bid:
        print("[bidder] auto_bid_enabled=False — skipping")
        state.set_watch_phase("IDLE")
        return

    # ── Get discovered markets ────────────────────────────────────────────────
    discovered = state.get_discovered_markets()
    if not discovered:
        print("[bidder] No markets in state — re-fetching")
        from market_watcher import _discover_todays_markets
        discovered = await _discover_todays_markets()

    if not discovered:
        print("[bidder] Still no markets — aborting")
        log_event(today, "BID_ERROR", "No markets found at open")
        state.set_watch_phase("IDLE")
        return

    # Flatten event→markets into a single ordered list (already Z→A from config)
    all_markets = [
        (et, m)
        for et, markets in discovered.items()
        for m in markets
    ]
    total = len(all_markets)

    # ── Build live_markets list ───────────────────────────────────────────────
    t_fetch_start = time.perf_counter()

    async with SpeedClient() as client:

        if ws_state is not None:
            # ── WS path: zero REST calls, instant state read ──────────────────
            live_markets = []
            for et, m in all_markets:
                ticker  = m.get("ticker", "")
                ws_data = ws_state.get(ticker, {})
                if ws_data:
                    no_cents = _no_ask_from_ws(ws_data)
                    oi       = float(ws_data.get("open_interest_fp", "0") or "0")
                else:
                    # WS state missing for this ticker — use pre-cached data
                    no_ask_d = m.get("no_ask_dollars")
                    no_cents = round(float(no_ask_d) * 100) if no_ask_d else 0
                    oi       = float(m.get("open_interest_fp") or 0)

                live_markets.append({
                    "ticker":        ticker,
                    "no_ask_cents":  no_cents,
                    "open_interest": oi,
                })

            ms_to_fetch = round((time.perf_counter() - t_fetch_start) * 1000)
            ws_coverage = sum(1 for m in live_markets
                              if ws_state.get(m["ticker"]))
            print(f"[bidder] WS state read — {ws_coverage}/{total} tickers live "
                  f"in {ms_to_fetch}ms")

        else:
            # ── REST fallback: concurrent GET per ticker ──────────────────────
            print(f"[bidder] WS state unavailable — falling back to REST fetch")
            tickers     = [m.get("ticker", "") for _, m in all_markets]
            fresh_tasks = [client.get_market(t) for t in tickers]
            fresh       = await asyncio.gather(*fresh_tasks, return_exceptions=True)

            live_markets = []
            for (et, m), result in zip(all_markets, fresh):
                ticker = m.get("ticker", "")
                if isinstance(result, Exception):
                    print(f"[bidder] REST fetch error {ticker}: {result}")
                    no_cents = 0
                    oi       = 0.0
                else:
                    no_ask_d = result.get("no_ask_dollars")
                    no_cents = round(float(no_ask_d) * 100) if no_ask_d else 0
                    oi       = float(result.get("open_interest_fp") or 0)
                live_markets.append({
                    "ticker":        ticker,
                    "no_ask_cents":  no_cents,
                    "open_interest": oi,
                })

            ms_to_fetch = round((time.perf_counter() - t_fetch_start) * 1000)
            print(f"[bidder] REST fetch {total} markets in {ms_to_fetch}ms")

        # ── Fire ONE batch POST ───────────────────────────────────────────────
        print(f"[bidder] {'DRY RUN' if dry_run else 'LIVE'} — "
              f"batch NO bid {total} markets (Z→A)  "
              f"max={max_no}¢ min={min_no}¢ first_only={only_zero}")

        t_bid = time.perf_counter()
        results = await client.batch_no_bids(
            live_markets,
            contracts    = contracts,
            max_no_cents = max_no,
            min_no_cents = min_no,
            only_zero_oi = only_zero,
            dry_run      = dry_run,
        )
        ms_bid = round((time.perf_counter() - t_bid) * 1000)

        # ── Persist all results ───────────────────────────────────────────────
        et_map    = {m.get("ticker", ""): et for et, m in all_markets}
        city_map  = {m.get("ticker", ""): m.get("city", "")
                     for _, m in all_markets}
        label_map = {m.get("ticker", ""): (
                         m.get("no_sub_title") or m.get("yes_sub_title", "")
                     )
                     for _, m in all_markets}

        placed  = sum(1 for r in results if r.get("placed"))
        firsts  = sum(1 for r in results if r.get("was_first") and r.get("placed"))
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
                no_price_cents = r.get("no_ask_cents", 0),
                open_interest  = r.get("open_interest", 0),
                was_first      = r.get("was_first", False),
                dry_run        = dry_run,
                order_id       = r.get("order_id"),
                status         = ("placed" if r.get("placed")
                                  else "skip:" + (r.get("error") or "")),
                ms_after_open  = r.get("ms_elapsed"),
            )

        summary = (
            f"placed={placed} firsts={firsts} skipped={skipped} "
            f"fetch_ms={ms_to_fetch} bid_ms={ms_bid} "
            f"path={'ws' if ws_state is not None else 'rest'}"
        )
        print(f"[bidder] Done — {summary}")
        log_event(today, "BIDS_PLACED", summary)
        state.set_last_bid_run(results)
        state.set_watch_phase("IDLE")
