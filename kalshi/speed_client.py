"""
Speed-optimized Kalshi client using aiohttp for concurrent async requests.

Goal: place NO bids on 30–50 markets in < 500ms total.

Key optimisations:
  - Single aiohttp.ClientSession with connection pooling (reused across calls)
  - RSA key loaded once at startup, not re-read from disk on every request
  - All market fetches fired concurrently (asyncio.gather)
  - All bid orders fired concurrently
  - Auth header generation is the hot path — minimise allocations
"""
from __future__ import annotations
import asyncio
import base64
import time
from typing import Any

import aiohttp
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

import config

# ── Key loaded once at import time ────────────────────────────────────────────
with open(config.KALSHI_PRIVATE_KEY_PATH, "rb") as _f:
    _PRIVATE_KEY = serialization.load_pem_private_key(_f.read(), password=None)

_BASE   = config.KALSHI_REST_BASE
_PREFIX = "/trade-api/v2"


def _auth_headers(method: str, path: str) -> dict:
    """Generate RSA-signed auth headers. Hot path — kept minimal."""
    ts_ms = str(int(time.time() * 1000))
    sig   = _PRIVATE_KEY.sign(
        (ts_ms + method.upper() + path).encode(),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=hashes.SHA256().digest_size),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       config.KALSHI_API_KEY,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "Content-Type":            "application/json",
    }


class SpeedClient:
    """
    Async Kalshi client.  Use as an async context manager:

        async with SpeedClient() as client:
            markets = await client.get_all_markets_for_events(event_tickers)
            results = await client.batch_yes_bids(all_markets, contracts=N)
    """

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=100,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(total=5, connect=2)
        self._session = aiohttp.ClientSession(
            base_url="https://external-api.kalshi.com",
            connector=connector,
            timeout=timeout,
        )
        return self

    async def __aexit__(self, *_):
        if self._session:
            await self._session.close()

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _get(self, path: str, params: dict | None = None,
                   _retry: int = 3) -> dict:
        full_path = f"{_PREFIX}{path}"
        for attempt in range(_retry):
            headers = _auth_headers("GET", full_path)
            async with self._session.get(full_path, headers=headers,
                                         params=params) as r:
                if r.status == 429 and attempt < _retry - 1:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                r.raise_for_status()
                return await r.json()
        raise RuntimeError(f"_get {path} failed after {_retry} attempts")

    async def _post(self, path: str, body: dict, _retry: int = 3) -> dict:
        full_path = f"{_PREFIX}{path}"
        for attempt in range(_retry):
            headers = _auth_headers("POST", full_path)
            async with self._session.post(full_path, headers=headers,
                                          json=body) as r:
                if r.status == 429 and attempt < _retry - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))   # 0.5s, 1.0s
                    continue
                r.raise_for_status()
                return await r.json()
        raise RuntimeError(f"_post {path} failed after {_retry} attempts")

    async def _delete(self, path: str) -> dict:
        full_path = f"{_PREFIX}{path}"
        headers   = _auth_headers("DELETE", full_path)
        async with self._session.delete(full_path, headers=headers) as r:
            r.raise_for_status()
            # 204 No Content on success — return empty dict
            if r.content_length == 0 or r.status == 204:
                return {}
            return await r.json()

    # ── Market discovery ──────────────────────────────────────────────────────

    async def list_events(self, series_ticker: str,
                          status: str = "open",
                          limit: int = 5) -> list[dict]:
        """Return events for a series (e.g. all open KXHIGHTATL events)."""
        params: dict = {"series_ticker": series_ticker, "limit": limit}
        if status:
            params["status"] = status
        data = await self._get("/events", params=params)
        return data.get("events", [])

    async def get_markets_for_event(self, event_ticker: str,
                                     status: str | None = "open") -> list[dict]:
        """
        Return all bucket markets for a single event.
        status="open"     — live markets only (default, used at market open)
        status=None       — all markets regardless of status (used for backfill)
        """
        params: dict = {"event_ticker": event_ticker, "limit": 20}
        if status:
            params["status"] = status
        data = await self._get("/markets", params=params)
        return data.get("markets", [])

    async def get_market(self, ticker: str) -> dict:
        data = await self._get(f"/markets/{ticker}")
        return data.get("market", data)

    async def get_all_markets_for_events(self,
                                          event_tickers: list[str],
                                          concurrency: int = 10,
                                          market_status: str | None = "open",
                                          ) -> dict[str, list[dict]]:
        """
        Fetch all bucket markets for all events concurrently.
        Returns {event_ticker: [market, ...]}

        market_status="open"  — live markets only (default, hot path)
        market_status=None    — all statuses (backfill / historical research)
        concurrency=10 for hot path; pass lower value for backfill.
        """
        sem = asyncio.Semaphore(concurrency)

        async def _fetch(et: str):
            async with sem:
                return await self.get_markets_for_event(et, status=market_status)

        tasks = [_fetch(et) for et in event_tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        out = {}
        for et, res in zip(event_tickers, results):
            if isinstance(res, Exception):
                print(f"[speed] fetch error for {et}: {res}")
                out[et] = []
            else:
                out[et] = res
        return out

    # ── Order management ─────────────────────────────────────────────────────

    async def cancel_order(self, order_id: str) -> dict:
        """Cancel a single resting order by ID."""
        return await self._delete(f"/portfolio/orders/{order_id}")

    async def cancel_orders(self, order_ids: list[str],
                             concurrency: int = 3) -> list[dict]:
        """Cancel multiple orders with limited concurrency to avoid 429s."""
        sem = asyncio.Semaphore(concurrency)

        async def _cancel_one(oid: str) -> dict:
            async with sem:
                try:
                    await self.cancel_order(oid)
                    return {"order_id": oid, "ok": True}
                except Exception as e:
                    return {"order_id": oid, "ok": False, "error": str(e)}

        return list(await asyncio.gather(*[_cancel_one(oid) for oid in order_ids]))

    # ── Trades ───────────────────────────────────────────────────────────────

    async def get_first_trade_for_ticker(self, ticker: str) -> dict | None:
        """
        Paginate GET /markets/trades to find the oldest (first-ever) trade
        for a specific market ticker.

        Kalshi returns trades newest-first; we follow cursor pages until the
        cursor is empty, then take trades[-1] from the final page.

        Returns the trade dict (keys: created_time, price, size, ticker)
        or None if the market has never traded.

        NOTE: min_ts param causes 400 — use ticker + cursor pagination only.
        """
        cursor: str = ""
        last_batch: list[dict] = []
        while True:
            params: dict = {"ticker": ticker, "limit": 100}
            if cursor:
                params["cursor"] = cursor
            r        = await self._get("/markets/trades", params=params)
            batch    = r.get("trades", [])
            if batch:
                last_batch = batch
            cursor = r.get("cursor", "")
            if not cursor or not batch:
                break
        return last_batch[-1] if last_batch else None

    # ── Balance ───────────────────────────────────────────────────────────────

    async def get_balance(self) -> float:
        data = await self._get("/portfolio/balance")
        return data["balance"] / 100

    # ── Bidding ───────────────────────────────────────────────────────────────

    async def batch_yes_bids(self, markets: list[tuple], contracts: int = 1,
                              yes_price_cents: int = 1,
                              dry_run: bool = True,
                              batch_size: int = 30,
                              batch_concurrency: int = 3,
                              inter_round_ms: int = 0,
                              t_open: float | None = None) -> list[dict]:
        """
        Place YES limit orders at yes_price_cents on all markets.

        markets: list of (event_ticker, market_dict) tuples from discovered_markets.
        Fixed price — no filtering, no price reading.  GTC orders sit in the
        book all day and fill as NO buyers arrive.

        Returns list of result dicts compatible with batch_no_bids output.
        """
        import uuid
        t0 = time.perf_counter()
        _t_open_wall: float = t_open if t_open is not None else time.time()

        results:  list[dict] = []
        to_place: list[dict] = []

        for et_or_tuple in markets:
            # Accept either (event_ticker, market_dict) tuple or plain market_dict
            if isinstance(et_or_tuple, tuple):
                _, m = et_or_tuple
            else:
                m = et_or_tuple

            ticker = m.get("ticker", "")
            oi     = float(m.get("open_interest_fp") or 0)

            r = {
                "ticker":        ticker,
                "yes_price_cents": yes_price_cents,
                "open_interest": oi,
                "contracts":     contracts,
                "placed":        False,
                "dry_run":       dry_run,
                "order_id":      None,
                "error":         None,
                "was_first":     oi == 0,
                "ms_elapsed":    None,
                "engine_ms":     None,
            }
            results.append(r)

            if dry_run:
                r["placed"]   = True
                r["order_id"] = "DRY_RUN"
            to_place.append(r)

        # Stamp dry-run timing and return early
        if dry_run:
            engine_ms = round((time.perf_counter() - t0) * 1000)
            ms_after  = round((time.time() - _t_open_wall) * 1000)
            for r in to_place:
                r["ms_elapsed"] = ms_after
                r["engine_ms"]  = engine_ms
            return results

        if not to_place:
            return results

        # ── Build chunk payloads ──────────────────────────────────────────────
        cid_map: dict[str, dict] = {}
        order_list = []
        for r in to_place:
            cid = f"spd-{uuid.uuid4().hex[:8]}"
            cid_map[cid] = r
            order_list.append({
                "ticker":          r["ticker"],
                "side":            "yes",
                "action":          "buy",
                "count":           r["contracts"],
                "yes_price":       yes_price_cents,
                "client_order_id": cid,
                "time_in_force":   "good_till_canceled",
            })

        chunks  = [order_list[i: i + batch_size]
                   for i in range(0, len(order_list), batch_size)]
        n_chunks = len(chunks)
        print(f"[speed] firing {len(to_place)} YES@{yes_price_cents}¢ orders "
              f"across {n_chunks} chunks "
              f"(size≤{batch_size}, concurrency={batch_concurrency})")

        async def _post_chunk(chunk: list[dict], chunk_idx: int):
            t1   = time.perf_counter()
            resp = await self._post("/portfolio/orders/batched",
                                    {"orders": chunk})
            t_wall_resp  = time.time()
            ms_engine    = round((time.perf_counter() - t1) * 1000)
            ms_from_open = round((t_wall_resp - _t_open_wall) * 1000)
            print(f"[speed] chunk {chunk_idx+1}/{n_chunks} "
                  f"({len(chunk)} orders) → {ms_engine}ms rtt / "
                  f"{ms_from_open}ms after open")
            return resp, ms_engine, ms_from_open

        responses: list = []
        for round_start in range(0, n_chunks, batch_concurrency):
            round_chunks = chunks[round_start: round_start + batch_concurrency]
            round_tasks  = [
                _post_chunk(c, round_start + i)
                for i, c in enumerate(round_chunks)
            ]
            round_results = await asyncio.gather(*round_tasks,
                                                 return_exceptions=True)
            responses.extend(round_results)

            if inter_round_ms > 0 and round_start + batch_concurrency < n_chunks:
                await asyncio.sleep(inter_round_ms / 1000)

        ms_total      = round((time.perf_counter() - t0) * 1000)
        ms_total_open = round((time.time() - _t_open_wall) * 1000)

        for resp_or_exc in responses:
            if isinstance(resp_or_exc, Exception):
                print(f"[speed] chunk error: {resp_or_exc}")
                for r in to_place:
                    if r["ms_elapsed"] is None:
                        r["error"]      = str(resp_or_exc)
                        r["ms_elapsed"] = ms_total_open
                        r["engine_ms"]  = ms_total
                continue

            resp, ms_engine, ms_after_open = resp_or_exc
            for item in resp.get("orders", []):
                r = cid_map.get(item.get("client_order_id", ""))
                if not r:
                    continue
                if item.get("error"):
                    r["error"]      = str(item["error"])
                    r["ms_elapsed"] = ms_after_open
                    r["engine_ms"]  = ms_engine
                else:
                    ord_obj        = item.get("order", {})
                    r["placed"]    = True
                    r["order_id"]  = ord_obj.get("order_id") or ord_obj.get("id")
                    r["ms_elapsed"] = ms_after_open
                    r["engine_ms"]  = ms_engine

        for r in to_place:
            if r["ms_elapsed"] is None:
                r["error"]      = "no response"
                r["ms_elapsed"] = ms_total_open
                r["engine_ms"]  = ms_total

        placed = sum(1 for r in to_place if r["placed"])
        print(f"[speed] batch complete — {placed}/{len(to_place)} placed "
              f"in {ms_total}ms total")
        return results

    async def individual_yes_bids(self, markets: list[tuple], contracts: int = 1,
                                   yes_price_cents: int = 1,
                                   dry_run: bool = True,
                                   inter_order_ms: int = 40,
                                   t_open: float | None = None) -> list[dict]:
        """
        Place YES limit orders one at a time via POST /portfolio/orders.

        Slower than batch (~8s for 198 orders at 40ms/order) but immune to the
        stricter per-endpoint rate limit on the batched endpoint.  Used as the
        primary placement path since the batch endpoint 429s at market open.

        markets: list of (event_ticker, market_dict) tuples.
        Returns same result-dict list as batch_yes_bids for drop-in compatibility.
        """
        import uuid
        t0 = time.perf_counter()
        _t_open_wall: float = t_open if t_open is not None else time.time()

        results:  list[dict] = []

        for et_or_tuple in markets:
            if isinstance(et_or_tuple, tuple):
                _, m = et_or_tuple
            else:
                m = et_or_tuple

            ticker = m.get("ticker", "")
            oi     = float(m.get("open_interest_fp") or 0)

            r = {
                "ticker":          ticker,
                "yes_price_cents": yes_price_cents,
                "open_interest":   oi,
                "contracts":       contracts,
                "placed":          False,
                "dry_run":         dry_run,
                "order_id":        None,
                "error":           None,
                "was_first":       oi == 0,
                "ms_elapsed":      None,
                "engine_ms":       None,
            }
            results.append(r)

            if dry_run:
                r["placed"]   = True
                r["order_id"] = "DRY_RUN"
                r["ms_elapsed"] = 0
                r["engine_ms"]  = 0
                continue

            cid  = f"ws-{uuid.uuid4().hex[:8]}"
            body = {
                "ticker":          ticker,
                "side":            "yes",
                "action":          "buy",
                "count":           contracts,
                "yes_price":       yes_price_cents,
                "client_order_id": cid,
                "time_in_force":   "good_till_canceled",
            }

            for attempt in range(3):
                try:
                    resp = await self._post("/portfolio/orders", body)
                    t_wall_resp  = time.time()
                    ms_from_open = round((t_wall_resp - _t_open_wall) * 1000)
                    ms_engine    = round((time.perf_counter() - t0) * 1000)
                    ord_obj      = resp.get("order", resp)
                    r["placed"]    = True
                    r["order_id"]  = ord_obj.get("order_id") or ord_obj.get("id")
                    r["ms_elapsed"] = ms_from_open
                    r["engine_ms"]  = ms_engine
                    break
                except Exception as exc:
                    if "429" in str(exc) and attempt < 2:
                        await asyncio.sleep(1.0 * (attempt + 1))
                        continue
                    r["error"]      = str(exc)
                    r["ms_elapsed"] = round((time.time() - _t_open_wall) * 1000)
                    r["engine_ms"]  = round((time.perf_counter() - t0) * 1000)
                    break

            if inter_order_ms > 0:
                await asyncio.sleep(inter_order_ms / 1000)

        ms_total = round((time.perf_counter() - t0) * 1000)
        placed   = sum(1 for r in results if r.get("placed"))
        print(f"[speed] individual complete — {placed}/{len(results)} placed "
              f"in {ms_total}ms total")
        return results

