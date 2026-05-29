"""
Order Poller — syncs order lifecycle from Kalshi into bid_log.

Runs every 30 minutes via scheduler. Does three things:
  1. Fetches all resting/filled weather orders from Kalshi → upserts bid_log
     (this also fixes any bad status rows from batch-429 failures)
  2. For settled markets, fetches result + expiration_value + computes PnL
  3. Logs a summary

bid_log lifecycle columns tracked:
  order_status   — "resting" | "filled" | "canceled" | "expired"
  fill_count     — contracts actually filled (0 → contracts)
  fill_price_cents — avg fill price in cents (1 for YES@1¢ GTC)
  market_result  — "yes" | "no" once market settles
  expiration_value — actual settlement value (e.g. "73" for 73°F)
  settled_at     — UTC ISO timestamp of settlement
  pnl_cents      — fill_count × 99 if YES won, -fill_count × 1 if NO won
"""
from __future__ import annotations
import asyncio
import threading
import time
from datetime import datetime, timezone, timedelta

from db.models import (
    upsert_bid_from_order, mark_bid_settled, get_open_bid_order_ids,
    log_event
)
from kalshi.speed_client import SpeedClient

_WEATHER_PREFIXES = ("KXHIGH", "KXLOW")


def _today_et() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=-4)).date().isoformat()


def _is_weather(ticker: str) -> bool:
    return any(ticker.startswith(p) for p in _WEATHER_PREFIXES)


async def _fetch_queue_position(client: SpeedClient, order_id: str,
                                sem: asyncio.Semaphore) -> float | None:
    """Fetch queue_position_fp for a single resting order."""
    async with sem:
        try:
            data = await client._get(f"/portfolio/orders/{order_id}/queue_position")
            val  = data.get("queue_position_fp")
            return float(val) if val is not None else None
        except Exception:
            return None


async def _sync_orders(client: SpeedClient) -> int:
    """
    Pull all weather orders (resting + filled) from Kalshi.
    For resting orders, also fetches queue_position_fp concurrently.
    Upserts each into bid_log. Returns count upserted.
    """
    resting_orders: list[dict] = []
    filled_orders:  list[dict] = []

    for status_filter in ("resting", "executed"):  # Kalshi uses "executed" not "filled"
        cursor = ""
        while True:
            params: dict = {"status": status_filter, "limit": 200}
            if cursor:
                params["cursor"] = cursor
            data   = await client._get("/portfolio/orders", params)
            orders = data.get("orders", [])
            for o in orders:
                if _is_weather(o.get("ticker", "")):
                    if status_filter == "resting":
                        resting_orders.append(o)
                    else:
                        # Normalize Kalshi's "executed" → "filled" for DB/UI consistency
                        o["status"] = "filled"
                        filled_orders.append(o)
            cursor = data.get("cursor", "")
            if not cursor or not orders:
                break

    # Fetch queue positions for all resting orders concurrently (max 10 in flight)
    if resting_orders:
        sem = asyncio.Semaphore(10)
        queue_tasks = [
            _fetch_queue_position(client, o["order_id"], sem)
            for o in resting_orders
        ]
        queue_positions = await asyncio.gather(*queue_tasks)
        for o, qp in zip(resting_orders, queue_positions):
            o["queue_position_fp"] = qp

    # Upsert all orders
    for o in resting_orders + filled_orders:
        upsert_bid_from_order(o)

    return len(resting_orders) + len(filled_orders)


async def _sync_settlements(client: SpeedClient) -> int:
    """
    For every bid_log row that has order_id but no market_result yet,
    check if the market has settled and record result + PnL.
    """
    rows = get_open_bid_order_ids()   # [{ticker, order_id, fill_count, contracts}]
    if not rows:
        return 0

    settled_count = 0
    for row in rows:
        ticker = row["ticker"]
        try:
            mkt = await client.get_market(ticker)
        except Exception:
            continue

        result = mkt.get("result", "")        # "yes" | "no" | ""
        exp_val = mkt.get("expiration_value", "") or ""
        close_time = mkt.get("close_time", "")

        if result not in ("yes", "no"):
            continue  # not settled yet

        fill_count = int(row.get("fill_count") or 0)
        if result == "yes":
            # YES won: payout $1/contract − cost $0.01/contract = net +99¢/contract
            pnl_cents = fill_count * 99
        else:
            # NO won: lose the $0.01 cost per filled contract
            pnl_cents = -(fill_count * 1)

        mark_bid_settled(
            ticker        = ticker,
            market_result = result,
            expiration_value = str(exp_val),
            settled_at    = close_time or datetime.now(timezone.utc).isoformat(),
            pnl_cents     = pnl_cents,
        )
        settled_count += 1

    return settled_count


async def _run_once() -> None:
    today = _today_et()
    async with SpeedClient() as client:
        upserted = await _sync_orders(client)
        settled  = await _sync_settlements(client)

    msg = f"orders_synced={upserted} newly_settled={settled}"
    print(f"[poller] {msg}")
    if settled > 0:
        log_event(today, "SETTLEMENT_SYNC", msg)


def run_poll() -> None:
    """Entry point called by scheduler (runs in a daemon thread)."""
    try:
        asyncio.run(_run_once())
    except Exception as exc:
        print(f"[poller] error: {exc}")


def start_background_poll() -> None:
    """Fire one poll immediately in a background thread (called at boot)."""
    threading.Thread(target=run_poll, name="order-poller-boot", daemon=True).start()
