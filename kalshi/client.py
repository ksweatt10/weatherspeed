"""
Kalshi REST client — sync, RSA-signed. Auth pattern ported from BTC bot.
"""
from __future__ import annotations
import base64
import time

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

import config


def _make_auth_headers(method: str, path: str) -> dict:
    with open(config.KALSHI_PRIVATE_KEY_PATH, "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)

    ts_ms = str(int(time.time() * 1000))
    msg = (ts_ms + method.upper() + path).encode("utf-8")
    sig = private_key.sign(
        msg,
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
        "Content-Type":            "application/json",
    }


class KalshiClient:
    def __init__(self):
        self.base = config.KALSHI_REST_BASE

    def _get(self, path: str, params: dict = None) -> dict:
        full_path = f"/trade-api/v2{path}"
        r = requests.get(
            f"{self.base}{path}",
            headers=_make_auth_headers("GET", full_path),
            params=params,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, body: dict) -> dict:
        full_path = f"/trade-api/v2{path}"
        r = requests.post(
            f"{self.base}{path}",
            headers=_make_auth_headers("POST", full_path),
            json=body,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def _delete(self, path: str) -> dict:
        full_path = f"/trade-api/v2{path}"
        r = requests.delete(
            f"{self.base}{path}",
            headers=_make_auth_headers("DELETE", full_path),
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def get_balance(self) -> float:
        data = self._get("/portfolio/balance")
        return data["balance"] / 100

    def list_markets(self, series_ticker: str = None, status: str = "open",
                     limit: int = 100) -> list[dict]:
        params = {"limit": limit}
        if status:                    # None → omit filter, returns all statuses
            params["status"] = status
        if series_ticker:
            params["series_ticker"] = series_ticker
        data = self._get("/markets", params=params)
        return data.get("markets", [])

    def get_market(self, ticker: str) -> dict:
        data = self._get(f"/markets/{ticker}")
        return data.get("market", data)

    def place_order(self, ticker: str, side: str, count: int,
                    limit_price_cents: int) -> dict:
        price_key = "yes_price_dollars" if side.lower() == "yes" else "no_price_dollars"
        body = {
            "ticker": ticker,
            "side":   side.lower(),
            "action": "buy",
            "type":   "limit",
            "count":  count,
            price_key: f"{limit_price_cents / 100:.2f}",
        }
        data = self._post("/portfolio/orders", body)
        return data.get("order", data)

    def cancel_order(self, order_id: str) -> None:
        self._delete(f"/portfolio/orders/{order_id}")

    def get_order(self, order_id: str) -> dict:
        data = self._get(f"/portfolio/orders/{order_id}")
        return data.get("order", data)

    def get_best_bid(self, ticker: str) -> float | None:
        """Return the best YES bid in cents, or None if no bids."""
        data = self._get(f"/markets/{ticker}/orderbook", params={"depth": 3})
        ob = data.get("orderbook_fp") or data.get("orderbook") or {}
        yes_bids = ob.get("yes_dollars") or ob.get("yes") or []
        if not yes_bids:
            return None
        v = yes_bids[0][0]
        f = float(v)
        return round(f * 100) if f < 1.0 else round(f)

    def sell_position(self, ticker: str, side: str, count: int,
                      min_price_cents: int) -> dict:
        """
        Limit sell of an existing position at min_price_cents floor.
        side: 'yes' or 'no'
        """
        price_key = "yes_price_dollars" if side.lower() == "yes" else "no_price_dollars"
        floor_dollars = max(0.01, min_price_cents / 100.0)
        body = {
            "ticker": ticker,
            "side":   side.lower(),
            "action": "sell",
            "type":   "limit",
            "count":  count,
            price_key: f"{floor_dollars:.4f}",
        }
        data = self._post("/portfolio/orders", body)
        return data.get("order", data)
