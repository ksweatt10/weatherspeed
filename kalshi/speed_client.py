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
            markets = await client.get_markets_for_event(event_ticker)
            results = await client.bid_no_all(markets, contracts=1)
    """

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=100,              # max simultaneous connections
            ttl_dns_cache=300,      # cache DNS for 5 min
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

    async def cancel_orders(self, order_ids: list[str]) -> list[dict]:
        """Cancel multiple orders concurrently. Returns list of results."""
        tasks = [self.cancel_order(oid) for oid in order_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        out = []
        for oid, r in zip(order_ids, results):
            if isinstance(r, Exception):
                out.append({"order_id": oid, "ok": False, "error": str(r)})
            else:
                out.append({"order_id": oid, "ok": True})
        return out

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

    async def _place_no_bid(self, ticker: str, contracts: int,
                             price_cents: int) -> dict:
        """Place a single NO limit order. Returns order dict."""
        body = {
            "ticker":            ticker,
            "side":              "no",
            "action":            "buy",
            "type":              "limit",
            "count":             contracts,
            "no_price_dollars":  f"{price_cents / 100:.2f}",
        }
        data = await self._post("/portfolio/orders", body)
        return data.get("order", data)

    async def batch_no_bids(self, markets: list[dict], contracts: int = 1,
                             max_no_cents: int = 70,
                             min_no_cents: int = 50,
                             only_zero_oi: bool = True,
                             dry_run: bool = True,
                             batch_size: int = 30,
                             batch_concurrency: int = 3,
                             inter_round_ms: int = 0,
                             t_open: float | None = None) -> list[dict]:
        """
        Place NO bids on all qualifying markets using chunked batch POSTs.

        Why chunked:
          ~192 orders possible (32 series × 6 buckets).
          Kalshi's batch endpoint limit is ~30 orders per request.
          Token bucket: 10 tokens/order — a single 192-order payload would
          exhaust the bucket instantly.

          Strategy (from Kalshi API docs):
            batch_size=30  → chunk qualifying orders into groups of 30
            batch_concurrency=3 → fire up to 3 chunks simultaneously
            192 orders → 7 chunks → ceil(7/3)=3 HTTP rounds → ~3× RTT total

        markets dicts must have: ticker (str), no_ask_cents (int), open_interest (float)
        Returns same result format as bid_no_all() for DB compatibility.
        """
        import uuid
        t0 = time.perf_counter()
        # Wall-clock reference for ms_after_open.  Caller passes the scheduled
        # open epoch (14:00:00 UTC) so both WS and fallback paths share the
        # same reference.  Defaults to now() if not supplied (manual trigger).
        _t_open_wall: float = t_open if t_open is not None else time.time()

        results:  list[dict] = []
        to_place: list[dict] = []   # result dicts that passed qualification

        for m in markets:
            ticker    = m.get("ticker", "")
            no_cents  = int(m.get("no_ask_cents", 0))
            oi        = float(m.get("open_interest") or 0)
            # Per-market contract count (set by speed_bidder from dollars_per_bucket);
            # falls back to the global `contracts` parameter.
            mkt_contracts = int(m.get("contracts", contracts))

            r = {
                "ticker":        ticker,
                "no_ask_cents":  no_cents,
                "open_interest": oi,
                "contracts":     mkt_contracts,
                "placed":        False,
                "dry_run":       dry_run,
                "order_id":      None,
                "error":         None,
                "was_first":     oi == 0,
                "ms_elapsed":    None,   # wall ms from 14:00:00 UTC → order confirmed
                "engine_ms":     None,   # how long batch_no_bids ran internally
            }
            results.append(r)

            if only_zero_oi and oi > 0:
                r["error"] = f"oi={oi:.0f}>0 skip"
            elif no_cents < min_no_cents:
                r["error"] = f"{no_cents}¢ < min {min_no_cents}¢"
            elif no_cents > max_no_cents:
                r["error"] = f"{no_cents}¢ > max {max_no_cents}¢"
            elif dry_run:
                r["placed"]    = True
                r["order_id"]  = "DRY_RUN"
                to_place.append(r)
            else:
                to_place.append(r)

        # Stamp dry-run timing and return early
        if dry_run:
            engine_ms = round((time.perf_counter() - t0) * 1000)
            ms_after  = round((time.time() - _t_open_wall) * 1000)
            for r in to_place:
                r["ms_elapsed"] = ms_after   # wall ms from open → qualification done
                r["engine_ms"]  = engine_ms  # how long the qual loop took
            return results

        if not to_place:
            return results

        # ── Assign client_order_ids and build chunk payloads ──────────────────
        cid_map: dict[str, dict] = {}
        order_list = []
        for r in to_place:
            cid = f"spd-{uuid.uuid4().hex[:8]}"
            cid_map[cid] = r
            order_list.append({
                "ticker":          r["ticker"],
                "side":            "no",
                "action":          "buy",
                "count":           r["contracts"],      # per-market count
                "no_price":        r["no_ask_cents"],   # integer cents (V1)
                "client_order_id": cid,
                "time_in_force":   "good_till_canceled",
            })

        # Split into chunks of batch_size
        chunks = [
            order_list[i : i + batch_size]
            for i in range(0, len(order_list), batch_size)
        ]
        n_chunks = len(chunks)
        print(f"[speed] firing {len(to_place)} orders across "
              f"{n_chunks} chunks (size≤{batch_size}, concurrency={batch_concurrency})")

        # ── Send chunks in rounds with controlled concurrency ─────────────────
        # Split chunks into rounds of batch_concurrency each.
        # Within a round, chunks fire concurrently.
        # Between rounds, optionally sleep inter_round_ms (token-bucket refill).
        #
        # Example: 7 chunks, concurrency=3, inter_round_ms=0
        #   Round 1: chunks 0,1,2 concurrent
        #   Round 2: chunks 3,4,5 concurrent
        #   Round 3: chunk  6

        async def _post_chunk(chunk: list[dict], chunk_idx: int):
            t1   = time.perf_counter()
            resp = await self._post("/portfolio/orders/batched",
                                    {"orders": chunk})
            t_wall_resp = time.time()          # wall clock when Kalshi responded
            ms_engine   = round((time.perf_counter() - t1) * 1000)  # HTTP RTT
            ms_from_open = round((t_wall_resp - _t_open_wall) * 1000)
            print(f"[speed] chunk {chunk_idx+1}/{n_chunks} "
                  f"({len(chunk)} orders) → {ms_engine}ms rtt / "
                  f"{ms_from_open}ms after open")
            return resp, ms_engine, ms_from_open

        responses: list = []
        for round_start in range(0, n_chunks, batch_concurrency):
            round_chunks = chunks[round_start : round_start + batch_concurrency]
            round_tasks  = [
                _post_chunk(c, round_start + i)
                for i, c in enumerate(round_chunks)
            ]
            round_results = await asyncio.gather(*round_tasks,
                                                 return_exceptions=True)
            responses.extend(round_results)

            # Inter-round pause for token-bucket refill (skipped on last round)
            if inter_round_ms > 0 and round_start + batch_concurrency < n_chunks:
                await asyncio.sleep(inter_round_ms / 1000)

        ms_total      = round((time.perf_counter() - t0) * 1000)
        ms_total_open = round((time.time() - _t_open_wall) * 1000)

        # ── Parse all responses back to result dicts ──────────────────────────
        for resp_or_exc in responses:
            if isinstance(resp_or_exc, Exception):
                # Whole chunk failed — mark its orders as errored
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
                    ord_obj          = item.get("order", {})
                    r["placed"]      = True
                    r["order_id"]    = ord_obj.get("order_id") or ord_obj.get("id")
                    r["ms_elapsed"]  = ms_after_open  # wall ms from open → confirmed
                    r["engine_ms"]   = ms_engine      # HTTP RTT for this chunk

        # Catch any orders that got no response at all
        for r in to_place:
            if r["ms_elapsed"] is None:
                r["error"]      = "no response"
                r["ms_elapsed"] = ms_total_open
                r["engine_ms"]  = ms_total

        placed = sum(1 for r in to_place if r["placed"])
        print(f"[speed] batch complete — {placed}/{len(to_place)} placed "
              f"in {ms_total}ms total")
        return results

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

    async def bid_no_all(self, markets: list[dict], contracts: int = 1,
                          max_no_cents: int = 99,
                          min_no_cents: int = 50,
                          only_zero_oi: bool = True,
                          dry_run: bool = True) -> list[dict]:
        """
        Place NO bids concurrently on all qualifying markets.

        Returns list of result dicts:
          {ticker, no_ask_cents, open_interest, placed, dry_run,
           order_id, error, ms_elapsed}
        """
        t0 = time.perf_counter()

        async def _bid_one(m: dict) -> dict:
            ticker   = m.get("ticker", "")
            no_ask   = m.get("no_ask_dollars")
            oi       = float(m.get("open_interest_fp") or 0)
            no_cents = round(float(no_ask) * 100) if no_ask else 0

            result = {
                "ticker":        ticker,
                "no_ask_cents":  no_cents,
                "open_interest": oi,
                "placed":        False,
                "dry_run":       dry_run,
                "order_id":      None,
                "error":         None,
                "was_first":     oi == 0,
                "ms_elapsed":    0,
            }

            # Qualification checks
            if only_zero_oi and oi > 0:
                result["error"] = f"oi={oi} > 0, skip"
                return result
            if no_cents < min_no_cents:
                result["error"] = f"no_ask={no_cents}¢ < min {min_no_cents}¢"
                return result
            if no_cents > max_no_cents:
                result["error"] = f"no_ask={no_cents}¢ > max {max_no_cents}¢"
                return result

            if dry_run:
                result["placed"] = True
                result["order_id"] = "DRY_RUN"
                result["ms_elapsed"] = round((time.perf_counter() - t0) * 1000)
                return result

            try:
                t1    = time.perf_counter()
                order = await self._place_no_bid(ticker, contracts, no_cents)
                result["placed"]     = True
                result["order_id"]   = order.get("order_id") or order.get("id")
                result["ms_elapsed"] = round((time.perf_counter() - t1) * 1000)
            except Exception as e:
                result["error"] = str(e)

            return result

        # Fire all bids concurrently
        tasks   = [_bid_one(m) for m in markets]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return list(results)
