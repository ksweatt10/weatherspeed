"""
Weather Speed Bot — Flask dashboard (port 8002).
"""
from __future__ import annotations
from flask import Flask, jsonify, render_template, request
from datetime import datetime, timezone, timedelta

import config
import runtime_config
import state
from db.models import (init_db, get_bid_history, get_market_timing_history,
                        get_session_log, get_first_trades_for_research)

app = Flask(__name__)

# ── Balance cache (refresh at most once per 60s) ──────────────────────────────
import time as _time
_balance_cache: dict = {"value": None, "ts": 0.0}


def _et_date() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=-4)).date().isoformat()


# ── API ───────────────────────────────────────────────────────────────────────

@app.get("/api/state")
def api_state():
    s          = state.get_all()
    ws_prices  = s.get("ws_prices", {})
    discovered = s.get("discovered_markets", {})

    market_summary = []
    for et, markets in discovered.items():
        for m in markets:
            ticker  = m.get("ticker", "")
            ws_data = ws_prices.get(ticker, {})

            # Prefer live WS prices; fall back to stale REST cache
            if ws_data:
                yes_bid  = float(ws_data.get("yes_bid_dollars",  "0") or "0")
                yes_ask  = float(ws_data.get("yes_ask_dollars",  "0") or "0")
                no_ask_c = max(0, 100 - round(yes_bid * 100))
                yes_ask_c = round(yes_ask * 100)
                oi       = float(ws_data.get("open_interest_fp", "0") or "0")
                volume   = float(ws_data.get("volume_fp",        "0") or "0")
                live     = True
            else:
                no_ask_c  = round(float(m.get("no_ask_dollars")  or 0) * 100)
                yes_ask_c = round(float(m.get("yes_ask_dollars") or 0) * 100)
                oi        = float(m.get("open_interest_fp") or 0)
                volume    = float(m.get("volume_fp") or 0)
                live      = False

            first_bid_ts = s.get("first_bids", {}).get(ticker)

            market_summary.append({
                "event_ticker":  et,
                "ticker":        ticker,
                "bucket":        m.get("no_sub_title") or m.get("yes_sub_title", ""),
                "no_ask_cents":  no_ask_c,
                "yes_ask_cents": yes_ask_c,
                "open_interest": oi,
                "volume":        volume,
                "open_time":     m.get("open_time", ""),
                "created_time":  m.get("created_time", ""),
                "ws_live":       live,
                "first_bid_ts":  first_bid_ts,
            })

    ws_status = {
        "connected": s.get("ws_connected", False),
        "tickers":   s.get("ws_tickers", 0),
        "last_msg":  s.get("ws_last_msg_ts", 0),
    }

    return jsonify({
        "watch_phase":    s.get("watch_phase", "IDLE"),
        "server_ts":      s.get("server_ts", 0),
        "markets":        market_summary,
        "last_bid_count": len(s.get("last_bid_run", [])),
        "errors":         s.get("errors", []),
        "dry_run":        runtime_config.get("dry_run", True),
        "ws":             ws_status,
        "bids_fired_today": s.get("bids_fired_today", False),
    })


@app.get("/api/bids")
def api_bids():
    date  = request.args.get("date", _et_date())
    bids  = get_bid_history(days=30)
    today = [b for b in bids if b.get("date") == date]
    return jsonify({"bids": today, "date": date, "all": bids[:500]})


@app.get("/api/research")
def api_research():
    timing       = get_market_timing_history(days=60)
    log          = get_session_log(limit=100)
    first_trades = get_first_trades_for_research()
    return jsonify({"timing": timing, "log": log, "first_trades": first_trades})


@app.get("/api/balance")
def api_balance():
    """Return Kalshi wallet balance, cached for 60 s."""
    import asyncio, threading
    now = _time.monotonic()
    if now - _balance_cache["ts"] < 60 and _balance_cache["value"] is not None:
        return jsonify({"balance": _balance_cache["value"], "cached": True})

    result = {"balance": None, "error": None, "cached": False}

    def _fetch():
        async def _do():
            from kalshi.speed_client import SpeedClient
            async with SpeedClient() as client:
                return await client.get_balance()
        try:
            val = asyncio.run(_do())
            _balance_cache["value"] = val
            _balance_cache["ts"]    = _time.monotonic()
            result["balance"] = val
        except Exception as e:
            result["error"] = str(e)

    t = threading.Thread(target=_fetch, daemon=True)
    t.start()
    t.join(timeout=5)
    return jsonify(result)


@app.get("/api/settings")
def api_settings():
    return jsonify(runtime_config.all_settings())


@app.post("/api/settings")
def api_settings_post():
    data = request.get_json() or {}
    bool_keys = {"dry_run", "auto_bid_enabled", "bid_only_zero_oi",
                 "track_market_timing"}
    int_keys  = {"contracts_per_market", "max_no_price_cents",
                 "min_no_price_cents", "creation_poll_interval_secs",
                 "creation_poll_start_utc_hour",
                 "creation_poll_start_utc_minute",
                 "open_time_utc_hour", "open_time_utc_minute",
                 "batch_size", "batch_concurrency", "batch_inter_round_ms"}
    for k, v in data.items():
        if k in bool_keys:
            runtime_config.set(k, bool(v))
        elif k in int_keys:
            runtime_config.set(k, int(v))
        else:
            runtime_config.set(k, v)
    return jsonify({"ok": True, "settings": runtime_config.all_settings()})


@app.get("/api/manual-trigger")
def api_manual_trigger():
    """Manually fire the bid cycle using live WS state if available."""
    import asyncio, threading
    from speed_bidder import run_bids
    ws_prices = state.get_ws_prices()

    def _run():
        asyncio.run(run_bids(ws_state=ws_prices if ws_prices else None))

    threading.Thread(target=_run, name="manual-bid", daemon=True).start()
    path = "WS" if ws_prices else "REST"
    return jsonify({"ok": True, "msg": f"Bid cycle triggered ({path} path)"})


@app.get("/api/refresh-markets")
def api_refresh_markets():
    """Manually re-discover today's markets."""
    import asyncio, threading
    from market_watcher import _discover_todays_markets

    def _run():
        asyncio.run(_discover_todays_markets())

    threading.Thread(target=_run, name="manual-discover", daemon=True).start()
    return jsonify({"ok": True, "msg": "Market discovery triggered"})


@app.get("/api/pull-first-trades-backfill")
def api_pull_first_trades_backfill():
    """
    DB-driven first-trade backfill: hits every bucket in market_buckets whose
    open_time is within the last N days and first_trade_contracts is NULL.
    Catches days where the discovery window has already closed (e.g. today after
    14:00 UTC). Pass ?days=N to control the lookback (default 3).
    """
    import asyncio, threading
    from market_watcher import pull_first_trades_db_backfill
    days    = int(request.args.get("days", 3))
    results = {}

    def _run():
        results.update(asyncio.run(pull_first_trades_db_backfill(days_back=days)))

    t = threading.Thread(target=_run, name="ft-backfill", daemon=True)
    t.start()
    t.join(timeout=180)
    return jsonify({"ok": True, **results})


@app.get("/api/pull-first-trades")
def api_pull_first_trades():
    """
    For all currently-discovered open-market buckets, paginate the Kalshi
    trades API to find each bucket's oldest trade and store the timestamp
    in market_buckets.first_bid_time.

    Pass ?overwrite=1 to re-fetch buckets that already have a value.
    Blocks for up to 120s synchronously so the response includes results.
    """
    import asyncio, threading
    from market_watcher import pull_first_trades_for_open_markets
    overwrite = request.args.get("overwrite", "0") == "1"
    results = {}

    def _run():
        results.update(asyncio.run(pull_first_trades_for_open_markets(overwrite=overwrite)))

    t = threading.Thread(target=_run, name="first-trades", daemon=True)
    t.start()
    t.join(timeout=120)
    return jsonify({"ok": True, **results})


@app.get("/api/backfill-research")
def api_backfill_research():
    """Pull up to 7 days of historical market data for all 32 series."""
    import asyncio, threading
    from market_watcher import run_research_backfill
    results = {}

    def _run():
        results.update(asyncio.run(run_research_backfill(days=7)))

    t = threading.Thread(target=_run, name="backfill", daemon=True)
    t.start()
    t.join(timeout=60)   # wait up to 60s synchronously so we can return results
    return jsonify({"ok": True, **results})


@app.get("/api/test-batch")
def api_test_batch():
    """
    Dry-run connectivity test: verify Kalshi auth + time the full batch flow.
    Does NOT place real orders. Uses dry_run=True always.
    Returns: balance, timing for market fetch, batch chunking plan, WS status.
    """
    import asyncio, time as _time

    async def _test():
        from kalshi.speed_client import SpeedClient
        t0 = _time.perf_counter()
        result = {}

        async with SpeedClient() as client:
            # 1. Balance check (verifies auth works)
            try:
                bal = await client.get_balance()
                result["balance_dollars"] = bal
            except Exception as e:
                result["balance_error"] = str(e)

            # 2. Fetch today's discovered markets (or re-fetch)
            discovered = state.get_discovered_markets()
            if not discovered:
                from market_watcher import _discover_todays_markets
                discovered = await _discover_todays_markets()

            all_markets = [
                {"ticker": m.get("ticker",""), "no_ask_cents": 0, "open_interest": 0}
                for mkts in discovered.values()
                for m in mkts
            ]
            result["markets_found"] = len(all_markets)
            result["fetch_ms"]      = round((_time.perf_counter() - t0) * 1000)

            # 3. Dry-run the full batch path (no real orders)
            batch_size = runtime_config.get("batch_size", 30)
            batch_conc = runtime_config.get("batch_concurrency", 3)
            n_chunks   = max(1, -(-len(all_markets) // batch_size))  # ceiling div
            result["batch_plan"] = {
                "total_markets":   len(all_markets),
                "batch_size":      batch_size,
                "batch_concurrency": batch_conc,
                "chunks":          n_chunks,
                "estimated_rounds": -(-n_chunks // batch_conc),
            }

            # 4. WS status
            ws = state.get_ws_status()
            result["ws"] = ws

        result["total_ms"] = round((_time.perf_counter() - t0) * 1000)
        return result

    try:
        data = asyncio.run(_test())
        return jsonify({"ok": True, **data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    return render_template("index.html")


# ── Startup ───────────────────────────────────────────────────────────────────

def create_app():
    init_db()
    import scheduler
    scheduler.start()
    # Auto-backfill research data on startup (non-blocking).
    # 60s delay lets the service settle and avoids rate-limit collisions
    # with any other startup API calls or rapid service restarts.
    import threading, asyncio, time as _time
    from market_watcher import run_research_backfill
    def _backfill():
        _time.sleep(60)
        asyncio.run(run_research_backfill(days=7))
    threading.Thread(target=_backfill, name="startup-backfill", daemon=True).start()
    return app


if __name__ == "__main__":
    import waitress
    application = create_app()
    print(f"[dashboard] Serving on http://0.0.0.0:{config.PORT}")
    waitress.serve(application, host="0.0.0.0", port=config.PORT)
