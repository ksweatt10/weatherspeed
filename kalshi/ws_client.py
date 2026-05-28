"""
Speed WebSocket client for Kalshi.

Timeline each day:
  09:27 UTC  — connect, subscribe to market_lifecycle_v2 globally
  ~09:31 UTC — 'created' events fire → subscribe ticker for those markets
  14:00:00   — 'activated' event fires → on_market_open callback
               caller reads ws_state (already live) → fires ONE batch POST

Also:
  - Mirrors all ticker data to state.py for dashboard display
  - Detects OI 0→non-zero transition → records first bid timestamp
  - Updates state.ws_connected on connect/disconnect

no_ask_cents = 100 - yes_bid_cents  (binary market: YES + NO = $1.00)
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from typing import Awaitable, Callable

import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

import config
import state as _state_module

log = logging.getLogger("ws_client")

WS_URL  = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"

# RSA key loaded once at import
with open(config.KALSHI_PRIVATE_KEY_PATH, "rb") as _f:
    _WS_KEY = serialization.load_pem_private_key(_f.read(), password=None)

# Series ticker prefixes — filter lifecycle events to our markets only
_SERIES_PREFIXES = tuple(s[0] for s in config.WEATHER_SERIES)


def _ws_headers() -> dict:
    """Auth headers for WS handshake — same RSA-PSS signing as REST."""
    ts_ms = str(int(time.time() * 1000))
    sig = _WS_KEY.sign(
        (ts_ms + "GET" + WS_PATH).encode(),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=hashes.SHA256().digest_size,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       config.KALSHI_API_KEY,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
    }


class SpeedWSClient:
    """
    Long-lived WebSocket client.

    Usage:
        async def on_open(ticker: str): ...
        client = SpeedWSClient(on_market_open=on_open)
        await client.run()

    ws_state[ticker] is the authoritative live dict for speed_bidder:
        {yes_bid_dollars, yes_ask_dollars, open_interest_fp, volume_fp, ts_ms}

    state.py is updated in parallel so the dashboard can read live data.
    """

    def __init__(
        self,
        on_market_open: Callable[[str], Awaitable[None]] | None = None,
    ):
        self.on_market_open  = on_market_open
        self.ws_state: dict[str, dict] = {}   # authoritative fast-path state

        self._ws                           = None
        self._cmd_id                       = 0
        self._subscribed_tickers: set[str] = set()
        self._activated_fired              = False
        self._running                      = False

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _next_id(self) -> int:
        self._cmd_id += 1
        return self._cmd_id

    async def _send(self, msg: dict) -> None:
        if self._ws is not None:
            try:
                await self._ws.send(json.dumps(msg))
            except Exception as e:
                log.warning(f"[ws] send error: {e}")

    def _is_our_market(self, ticker: str) -> bool:
        return any(ticker.startswith(p) for p in _SERIES_PREFIXES)

    # ── Subscriptions ─────────────────────────────────────────────────────────

    async def _sub_lifecycle_global(self) -> None:
        await self._send({
            "id":  self._next_id(),
            "cmd": "subscribe",
            "params": {"channels": ["market_lifecycle_v2"]},
        })
        log.info("[ws] subscribed market_lifecycle_v2 (global)")

    async def subscribe_tickers(self, tickers: list[str]) -> None:
        """Add tickers to the live ticker subscription. Safe to call repeatedly."""
        new = [t for t in tickers if t and t not in self._subscribed_tickers]
        if not new:
            return
        await self._send({
            "id":  self._next_id(),
            "cmd": "subscribe",
            "params": {
                "channels":              ["ticker"],
                "market_tickers":        new,
                "send_initial_snapshot": True,
            },
        })
        self._subscribed_tickers.update(new)
        _state_module.set_ws_connected(True, len(self._subscribed_tickers))
        log.info(
            f"[ws] subscribed ticker +{len(new)} "
            f"(total {len(self._subscribed_tickers)})"
        )

    # ── Message dispatch ──────────────────────────────────────────────────────

    async def _dispatch(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except Exception:
            return

        t = data.get("type")
        if   t == "ticker":
            self._on_ticker(data.get("msg", {}))
        elif t == "market_lifecycle_v2":
            await self._on_lifecycle(data.get("msg", {}))
        elif t == "error":
            log.warning(f"[ws] server error: {data}")
        # subscribed / ok / pong — silently ignore

    def _on_ticker(self, msg: dict) -> None:
        ticker = msg.get("market_ticker", "")
        if not ticker:
            return

        # Check for OI 0 → non-zero transition (= first bid on this market)
        prev   = self.ws_state.get(ticker, {})
        prev_oi = float(prev.get("open_interest_fp", "0") or "0")
        new_oi  = float(msg.get("open_interest_fp",  "0") or "0")

        if prev_oi == 0.0 and new_oi > 0.0:
            ts_ms = msg.get("ts_ms") or int(time.time() * 1000)
            if _state_module.record_first_bid(ticker, ts_ms):
                log.info(f"[ws] FIRST BID  {ticker}  oi={new_oi}  ts={ts_ms}")
                # Persist to DB asynchronously (fire and forget)
                asyncio.get_event_loop().create_task(
                    _persist_first_bid(ticker, ts_ms)
                )

        # Update both fast-path and dashboard state
        data = {
            "yes_bid_dollars":  msg.get("yes_bid_dollars",  "0"),
            "yes_ask_dollars":  msg.get("yes_ask_dollars",  "0"),
            "no_bid_dollars":   msg.get("no_bid_dollars",   "0"),
            "no_ask_dollars":   msg.get("no_ask_dollars",   "0"),
            "open_interest_fp": msg.get("open_interest_fp", "0"),
            "volume_fp":        msg.get("volume_fp",        "0"),
            "ts_ms":            msg.get("ts_ms",            0),
        }
        self.ws_state[ticker] = data
        _state_module.update_ws_ticker(ticker, data)

    async def _on_lifecycle(self, msg: dict) -> None:
        event_type = msg.get("event_type", "")
        ticker     = msg.get("market_ticker", "")

        if not self._is_our_market(ticker):
            return

        if event_type == "created":
            open_ts = msg.get("open_ts", "?")
            log.info(f"[ws] CREATED   {ticker}  open_ts={open_ts}")
            await self.subscribe_tickers([ticker])

        elif event_type == "activated":
            log.info(f"[ws] ACTIVATED {ticker} — market is OPEN")
            if not self._activated_fired and self.on_market_open:
                self._activated_fired = True
                asyncio.get_event_loop().create_task(
                    self.on_market_open(ticker)
                )

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Connect, subscribe, handle messages. Auto-reconnects with backoff."""
        self._running = True
        backoff = 1

        while self._running:
            try:
                log.info("[ws] connecting...")
                async with websockets.connect(
                    WS_URL,
                    additional_headers=_ws_headers(),
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self._ws              = ws
                    backoff               = 1
                    self._activated_fired = False   # reset each new connection so daily rearm works
                    _state_module.set_ws_connected(True, len(self._subscribed_tickers))
                    log.info("[ws] connected")

                    await self._sub_lifecycle_global()

                    # Re-subscribe to tickers (handles reconnect mid-session)
                    if self._subscribed_tickers:
                        prev = list(self._subscribed_tickers)
                        self._subscribed_tickers.clear()
                        await self.subscribe_tickers(prev)

                    async for raw in ws:
                        if not self._running:
                            break
                        await self._dispatch(raw)

            except websockets.ConnectionClosed as e:
                log.warning(f"[ws] closed ({e.code}): '{e.reason}' — retry in {backoff}s")
            except OSError as e:
                log.warning(f"[ws] network error: {e} — retry in {backoff}s")
            except Exception as e:
                log.error(f"[ws] unexpected: {e} — retry in {backoff}s")
            finally:
                self._ws = None
                _state_module.set_ws_connected(False, len(self._subscribed_tickers))

            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def stop(self) -> None:
        self._running = False


async def _persist_first_bid(ticker: str, ts_ms: int) -> None:
    """Write first-bid timestamp to DB (runs as a background task)."""
    try:
        from db.models import upsert_first_bid_time
        upsert_first_bid_time(ticker, ts_ms)
    except Exception as e:
        log.warning(f"[ws] failed to persist first bid for {ticker}: {e}")
