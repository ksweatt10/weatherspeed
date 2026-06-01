#!/usr/bin/env python3
"""
BTC Above/Below Order Status — pull a live snapshot of your BTC positions.

Shows queue position, fill status, and settlement details for every bucket
in a KXBTCD event.  Fetches live data from Kalshi + reads local bid_log.

Usage:
    python btc_status.py                          # today's BTC orders
    python btc_status.py KXBTCD-26MAY3017         # specific event
    python btc_status.py --date 2026-05-29        # specific date
    python btc_status.py --save                   # also write status back to DB
"""
from __future__ import annotations
import argparse
import asyncio
from datetime import datetime, timezone, timedelta

from db.models import init_db, _conn, upsert_bid_from_order, mark_bid_settled


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _today_et() -> str:
    return (_utcnow() + timedelta(hours=-4)).date().isoformat()


def _get_btc_rows(date: str, event_ticker: str | None) -> list[dict]:
    """Pull BTC bid_log rows for a given date (and optionally event)."""
    with _conn() as con:
        if event_ticker:
            rows = con.execute("""
                SELECT * FROM bid_log
                WHERE city = 'BTC'
                  AND date = ?
                  AND event_ticker = ?
                  AND dry_run = 0
                ORDER BY ticker
            """, (date, event_ticker)).fetchall()
        else:
            rows = con.execute("""
                SELECT * FROM bid_log
                WHERE city = 'BTC'
                  AND date = ?
                  AND dry_run = 0
                ORDER BY event_ticker, ticker
            """, (date,)).fetchall()
    return [dict(r) for r in rows]


async def _fetch_order(client, order_id: str, sem: asyncio.Semaphore) -> dict | None:
    """Fetch a single order from Kalshi."""
    async with sem:
        try:
            data = await client._get(f"/portfolio/orders/{order_id}")
            return data.get("order", data)
        except Exception as e:
            return {"error": str(e), "order_id": order_id}


async def _fetch_queue(client, order_id: str, sem: asyncio.Semaphore) -> float | None:
    """Fetch queue_position_fp for a resting order."""
    async with sem:
        try:
            data = await client._get(
                f"/portfolio/orders/{order_id}/queue_position")
            val = data.get("queue_position_fp")
            return float(val) if val is not None else None
        except Exception:
            return None


async def _fetch_market_result(client, ticker: str,
                                sem: asyncio.Semaphore) -> dict:
    """Fetch settlement result for a market ticker."""
    async with sem:
        try:
            data = await client._get(f"/markets/{ticker}")
            mkt = data.get("market", data)
            return {
                "ticker":            ticker,
                "result":            mkt.get("result", ""),
                "expiration_value":  mkt.get("expiration_value", ""),
                "close_time":        mkt.get("close_time", ""),
                "status":            mkt.get("status", ""),
            }
        except Exception as e:
            return {"ticker": ticker, "error": str(e)}


async def _run(date: str, event_ticker: str | None, save: bool) -> None:
    from kalshi.speed_client import SpeedClient

    init_db()
    rows = _get_btc_rows(date, event_ticker)

    if not rows:
        et_hint = event_ticker or "any"
        print(f"No BTC orders found in bid_log for date={date} event={et_hint}")
        print("(Run btc_bidder.py first, or check --date)")
        return

    # Collect unique event tickers and order IDs
    events   = sorted({r["event_ticker"] for r in rows})
    order_map = {r["order_id"]: r for r in rows
                 if r.get("order_id") and r["order_id"] != "DRY_RUN"}

    print(f"\n{'═'*62}")
    print(f"  BTC Above/Below Status  —  {date}  —  {_utcnow().strftime('%H:%M:%S UTC')}")
    print(f"{'═'*62}")
    print(f"  Events   : {', '.join(events)}")
    print(f"  Buckets  : {len(rows)}  |  With order_id: {len(order_map)}")

    if not order_map:
        print("\n  No live order IDs found. Orders may not have been placed yet.")
        return

    # ── Fetch live order status from Kalshi ───────────────────────────────────
    print(f"\n  Fetching live order status ({len(order_map)} orders) ...")
    sem10 = asyncio.Semaphore(10)
    async with SpeedClient() as client:
        # Fetch all orders concurrently
        order_tasks = [
            _fetch_order(client, oid, sem10)
            for oid in order_map
        ]
        live_orders = await asyncio.gather(*order_tasks)

        # Sort into buckets by status
        resting_ids  = []
        filled_ids   = []
        canceled_ids = []
        errored_ids  = []

        live_by_id: dict[str, dict] = {}
        for o in live_orders:
            if not o:
                continue
            oid    = o.get("order_id") or o.get("id", "")
            err    = o.get("error")
            status = o.get("status", "")
            live_by_id[oid] = o
            if err:
                errored_ids.append(oid)
            elif status == "resting":
                resting_ids.append(oid)
            elif status in ("executed", "filled"):
                filled_ids.append(oid)
            elif status in ("canceled", "cancelled"):
                canceled_ids.append(oid)

        # Fetch queue positions for all resting orders concurrently
        queue_map: dict[str, float | None] = {}
        if resting_ids:
            sem5 = asyncio.Semaphore(5)
            q_tasks = [_fetch_queue(client, oid, sem5) for oid in resting_ids]
            q_vals  = await asyncio.gather(*q_tasks)
            queue_map = dict(zip(resting_ids, q_vals))

        # Fetch settlement results for filled markets
        filled_tickers = list({
            order_map[oid]["ticker"]
            for oid in filled_ids
            if oid in order_map
        })
        settle_map: dict[str, dict] = {}
        if filled_tickers:
            sem5b = asyncio.Semaphore(5)
            s_tasks = [_fetch_market_result(client, t, sem5b) for t in filled_tickers]
            s_vals  = await asyncio.gather(*s_tasks)
            settle_map = {s["ticker"]: s for s in s_vals if s}

    # ── Print summary ─────────────────────────────────────────────────────────
    n_resting  = len(resting_ids)
    n_filled   = len(filled_ids)
    n_canceled = len(canceled_ids)
    n_error    = len(errored_ids)

    qvals       = [v for v in queue_map.values() if v is not None]
    avg_q       = sum(qvals) / len(qvals) if qvals else None
    min_q       = min(qvals) if qvals else None
    max_q       = max(qvals) if qvals else None

    print(f"\n  ┌─ STATUS ──────────────────────────────────────┐")
    print(f"  │  Resting   : {n_resting:>4}  {'(avg queue: ' + f'{avg_q:.1f}' + ')' if avg_q is not None else ''}")
    print(f"  │  Filled    : {n_filled:>4}")
    print(f"  │  Canceled  : {n_canceled:>4}")
    if n_error:
        print(f"  │  Errors    : {n_error:>4}  (API fetch errors)")
    print(f"  └───────────────────────────────────────────────┘")

    # ── Resting orders + queue positions ──────────────────────────────────────
    if resting_ids:
        print(f"\n  RESTING  ({n_resting} orders — queue position = your place in line)\n")
        # Sort by queue position ascending (best = lowest)
        resting_sorted = sorted(
            resting_ids,
            key=lambda oid: queue_map.get(oid) or float("inf")
        )
        print(f"  {'Bucket Label':<28} {'Queue':>7}  {'Order ID'}")
        print(f"  {'─'*28} {'─'*7}  {'─'*36}")
        for oid in resting_sorted:
            row   = order_map.get(oid, {})
            label = row.get("bucket_label") or row.get("ticker", "")[-12:]
            qpos  = queue_map.get(oid)
            qstr  = f"{qpos:>7.1f}" if qpos is not None else "      ?"
            print(f"  {label:<28} {qstr}  {oid}")

        if qvals:
            print(f"\n  Queue range: {min_q:.0f} – {max_q:.0f}  "
                  f"(median: {sorted(qvals)[len(qvals)//2]:.0f})")

    # ── Filled orders ─────────────────────────────────────────────────────────
    if filled_ids:
        yes_wins   = 0
        no_losses  = 0
        unresolved = 0
        pnl_cents  = 0
        contracts_per = rows[0]["contracts"] if rows else 5

        print(f"\n  FILLED  ({n_filled} orders)\n")
        print(f"  {'Bucket Label':<28} {'Fill':<6} {'Result':<8}  {'Exp Val':<10}  {'P&L'}")
        print(f"  {'─'*28} {'─'*6} {'─'*8}  {'─'*10}  {'─'*8}")

        for oid in sorted(filled_ids):
            row     = order_map.get(oid, {})
            ticker  = row.get("ticker", "")
            label   = row.get("bucket_label") or ticker[-12:]
            settle  = settle_map.get(ticker, {})
            result  = settle.get("result", "")
            exp_val = settle.get("expiration_value", "")
            live_o  = live_by_id.get(oid, {})
            fills   = int(float(live_o.get("fill_count_fp") or 0))

            if result == "yes":
                p = fills * 99
                pnl_str = f"+${p/100:.2f}"
                pnl_cents += p
                yes_wins += 1
            elif result == "no":
                p = -(fills * 1)
                pnl_str = f"-${abs(p)/100:.2f}"
                pnl_cents += p
                no_losses += 1
            else:
                pnl_str = "pending"
                unresolved += 1

            result_str = result.upper() if result else "open"
            print(f"  {label:<28} {fills:<6} {result_str:<8}  "
                  f"{str(exp_val):<10}  {pnl_str}")

        print(f"\n  ── Settlement summary ──────────────────────────────")
        print(f"  YES wins   : {yes_wins}")
        print(f"  NO losses  : {no_losses}")
        print(f"  Unresolved : {unresolved}")
        if yes_wins + no_losses > 0:
            cost_cents = contracts_per * yes_price_from_rows(rows) * (yes_wins + no_losses)
            print(f"  Net P&L    : ${pnl_cents/100:+.2f}")

    # ── Canceled orders ───────────────────────────────────────────────────────
    if canceled_ids:
        print(f"\n  CANCELED  ({n_canceled} orders)")
        for oid in canceled_ids:
            row = order_map.get(oid, {})
            print(f"    {row.get('ticker', oid)}")

    # ── Save fresh status to DB ───────────────────────────────────────────────
    if save:
        print(f"\n  Saving to DB ...")
        saved = 0
        for oid, live_o in live_by_id.items():
            if live_o.get("error"):
                continue
            # Normalize Kalshi "executed" → "filled" before upsert
            if live_o.get("status") == "executed":
                live_o = dict(live_o)
                live_o["status"] = "filled"
            # Add queue_position_fp if we have it
            if oid in queue_map:
                live_o = dict(live_o)
                live_o["queue_position_fp"] = queue_map[oid]
            upsert_bid_from_order(live_o)
            saved += 1

        # Save settlements
        for ticker, settle in settle_map.items():
            result = settle.get("result", "")
            if result not in ("yes", "no"):
                continue
            # Find contracts for this ticker
            matching = [r for r in rows if r.get("ticker") == ticker]
            if not matching:
                continue
            contracts = matching[0].get("contracts", 5)
            pnl = contracts * 99 if result == "yes" else -(contracts * 1)
            mark_bid_settled(
                ticker           = ticker,
                market_result    = result,
                expiration_value = str(settle.get("expiration_value", "")),
                settled_at       = settle.get("close_time", _utcnow().isoformat()),
                pnl_cents        = pnl,
            )
        print(f"  Saved {saved} order statuses to bid_log.")

    print(f"\n{'═'*62}\n")


def yes_price_from_rows(rows: list[dict]) -> int:
    """Read the bid price (stored in no_price_cents column) from the first row."""
    for r in rows:
        p = r.get("no_price_cents")
        if p is not None:
            return int(p)
    return 2


def main() -> None:
    parser = argparse.ArgumentParser(
        description="BTC above/below order status checker")
    parser.add_argument("event_ticker", nargs="?", default=None,
                        help="e.g. KXBTCD-26MAY3017 (default: any BTC today)")
    parser.add_argument("--date", default=None,
                        help="ET date YYYY-MM-DD (default: today)")
    parser.add_argument("--save", action="store_true",
                        help="Write fresh status back to bid_log DB")
    args = parser.parse_args()

    date = args.date or _today_et()
    asyncio.run(_run(date, args.event_ticker, args.save))


if __name__ == "__main__":
    main()
