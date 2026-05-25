"""
Speed WebSocket client for Kalshi.

Timeline each day:
  09:27 UTC  — connect, subscribe to market_lifecycle_v2 globally
  ~09:31 UTC — 'created' events fire → subscribe ticker for those markets
  14:00:00   — 'activated' events fire → call on_market_open immediately
               caller reads ws_state (already live) → fires ONE batch POST

no_ask is derived:  no_ask_cents = 100 - yes_bid_cents
(binary market: YES + NO always = $1.00)
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

log = logging.getLogger("ws_client")

WS_URL  = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"

# RSA key loaded once at import — same pattern as speed_client.py
with open(config.KALSHI_PRIVATE_KEY_PATH, "rb") as _f:
    _WS_KEY = serialization.load_pem_private_key(_f.read(), password=None)

# Series ticker prefixes to filter lifecycle events to our markets only
_SERIES_PREFIXES = tuple(s[0] for s in config.WEATHER_SERIES)


def _ws_headers() -> dict:
    """Auth headers for WS handshake (same RSA-PSS as REST, path = WS_PATH)."""
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

    ws_state[ticker] is updated live by the ticker channel:
        {yes_bid_dollars, yes_ask_dollars, open_interest_fp, volume_fp, ts_ms}

    no_ask_cents = 100 - round(float(ws_state[t]["yes_bid_dollars"]) * 100)
    """

    def __init__(
        self,
        on_market_open: Callable[[str], Awaitable[None]] | None = None,
    ):
        self.on_market_open = on_market_open
        self.ws_state: dict[str, dict] = {}   # live per-ticker state

        self._ws                          = None
        self._cmd_id                      = 0
        self._subscribed_tickers: set[str] = set()
        self._activated_fired             = False  # fire once per day
        self._running                     = False

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
        """Subscribe to ALL market lifecycle events (global — no ticker filter)."""
        await self._send({
            "id":  self._next_id(),
            "cmd": "subscribe",
            "params": {"channels": ["market_lifecycle_v2"]},
        })
        log.info("[ws] subscribed market_lifecycle_v2 (global)")

    async def subscribe_tickers(self, tickers: list[str]) -> None:
        """
        Add markets to the ticker channel subscription.
        Safe to call multiple times — skips already-subscribed tickers.
        """
        new = [t for t in tickers if t and t not in self._subscribed_tickers]
        if not new:
            return
        await self._send({
            "id":  self._next_id(),
            "cmd": "subscribe",
            "params": {
                "channels":              ["ticker"],
                "market_tickers":        new,
                "send_initial_snapshot": True,   # get current state immediately
            },
        })
        self._subscribed_tickers.update(new)
        log.info(
            f"[ws] subscribed ticker +{len(new)} markets "
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
        self.ws_state[ticker] = {
            "yes_bid_dollars":  msg.get("yes_bid_dollars",  "0"),
            "yes_ask_dollars":  msg.get("yes_ask_dollars",  "0"),
            "open_interest_fp": msg.get("open_interest_fp", "0"),
            "volume_fp":        msg.get("volume_fp",        "0"),
            "ts_ms":            msg.get("ts_ms",            0),
        }

    async def _on_lifecycle(self, msg: dict) -> None:
        event_type = msg.get("event_type", "")
        ticker     = msg.get("market_ticker", "")

        if not self._is_our_market(ticker):
            return

        if event_type == "created":
            open_ts = msg.get("open_ts", "?")
            log.info(f"[ws] CREATED   {ticker}  open_ts={open_ts}")
            # Subscribe to ticker immediately — 4.5 hrs of live data before open
            await self.subscribe_tickers([ticker])

        elif event_type == "activated":
            log.info(f"[ws] ACTIVATED {ticker} — MARKET IS OPEN")
            # Fire once only — first activation triggers the batch
            if not self._activated_fired and self.on_market_open:
                self._activated_fired = True
                # Don't await here — let the WS loop keep running for fills
                asyncio.get_event_loop().create_task(
                    self.on_market_open(ticker)
                )

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Connect, subscribe, handle messages indefinitely.
        Reconnects automatically on disconnect (exponential backoff, max 30s).
        Call stop() to exit cleanly.
        """
        self._running = True
        backoff = 1

        while self._running:
            try:
                log.info(f"[ws] connecting...")
                async with websockets.connect(
                    WS_URL,
                    additional_headers=_ws_headers(),
                    ping_interval=20,   # keep-alive ping every 20s
                    ping_timeout=10,    # disconnect if no pong in 10s
                ) as ws:
                    self._ws = ws
                    backoff  = 1        # reset on successful connect
                    log.info("[ws] connected")

                    # Always subscribe lifecycle globally first
                    await self._sub_lifecycle_global()

                    # Re-subscribe to tickers we knew about before (handles reconnect)
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
                log.error(f"[ws] unexpected error: {e} — retry in {backoff}s")
            finally:
                self._ws = None

            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def stop(self) -> None:
        self._running = False
