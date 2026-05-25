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
                        get_session_log)

app = Flask(__name__)


def _et_date() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=-4)).date().isoformat()


# ── API ───────────────────────────────────────────────────────────────────────

@app.get("/api/state")
def api_state():
    s = state.get_all()
    discovered = s.get("discovered_markets", {})
    # Summarise discovered markets for dashboard
    market_summary = []
    for et, markets in discovered.items():
        for m in markets:
            market_summary.append({
                "event_ticker":  et,
                "ticker":        m.get("ticker",""),
                "bucket":        m.get("no_sub_title") or m.get("yes_sub_title",""),
                "no_ask_cents":  round(float(m.get("no_ask_dollars") or 0) * 100),
                "yes_ask_cents": round(float(m.get("yes_ask_dollars") or 0) * 100),
                "open_interest": float(m.get("open_interest_fp") or 0),
                "volume":        float(m.get("volume_fp") or 0),
                "open_time":     m.get("open_time",""),
                "created_time":  m.get("created_time",""),
            })
    return jsonify({
        "watch_phase":    s.get("watch_phase","IDLE"),
        "server_ts":      s.get("server_ts", 0),
        "markets":        market_summary,
        "last_bid_count": len(s.get("last_bid_run",[])),
        "errors":         s.get("errors",[]),
        "dry_run":        runtime_config.get("dry_run", True),
    })


@app.get("/api/bids")
def api_bids():
    date  = request.args.get("date", _et_date())
    bids  = get_bid_history(days=30)
    today = [b for b in bids if b.get("date") == date]
    return jsonify({"bids": today, "date": date, "all": bids[:500]})


@app.get("/api/research")
def api_research():
    """Historical market creation + open time data."""
    timing  = get_market_timing_history(days=60)
    log     = get_session_log(limit=100)
    return jsonify({"timing": timing, "log": log})


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
                 "batch_size", "batch_concurrency"}
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
    """Manually fire the bid cycle right now (for testing)."""
    import asyncio, threading
    from speed_bidder import run_bids
    def _run():
        asyncio.run(run_bids())
    threading.Thread(target=_run, name="manual-bid", daemon=True).start()
    return jsonify({"ok": True, "msg": "Bid cycle triggered"})


@app.get("/api/refresh-markets")
def api_refresh_markets():
    """Manually re-discover today's markets."""
    import asyncio, threading
    from market_watcher import _discover_todays_markets
    def _run():
        asyncio.run(_discover_todays_markets())
    threading.Thread(target=_run, name="manual-discover", daemon=True).start()
    return jsonify({"ok": True, "msg": "Market discovery triggered"})


# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    return render_template("index.html")


# ── Startup ───────────────────────────────────────────────────────────────────

def create_app():
    init_db()
    import scheduler
    scheduler.start()
    return app


if __name__ == "__main__":
    import waitress
    application = create_app()
    print(f"[dashboard] Serving on http://0.0.0.0:{config.PORT}")
    waitress.serve(application, host="0.0.0.0", port=config.PORT)
