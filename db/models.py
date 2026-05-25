"""
SQLite models for Weather Speed Bot.

Tables:
  market_timing   — when each event was created & opened (+ first bid)
  market_buckets  — individual bucket metadata
  bid_log         — every NO bid we placed (or dry_run)
  session_log     — event log
"""
from __future__ import annotations
import sqlite3
import threading
import time
import os

_DB_PATH = os.getenv("DB_PATH", "weatherspeed.db")
_local   = threading.local()


def _conn() -> sqlite3.Connection:
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
    return _local.conn


def init_db() -> None:
    with _conn() as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS market_timing (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event_ticker    TEXT NOT NULL,
            series_ticker   TEXT NOT NULL,
            city            TEXT NOT NULL,
            kind            TEXT NOT NULL,
            settlement_date TEXT NOT NULL,
            created_time    TEXT,
            open_time       TEXT,
            first_bid_time  TEXT,
            recorded_at     TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ux_market_timing_event
            ON market_timing(event_ticker);

        CREATE TABLE IF NOT EXISTS market_buckets (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event_ticker    TEXT NOT NULL,
            ticker          TEXT NOT NULL UNIQUE,
            bucket_label    TEXT,
            floor_strike    REAL,
            cap_strike      REAL,
            created_time    TEXT,
            open_time       TEXT,
            first_bid_time  TEXT,
            recorded_at     TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        );

        CREATE TABLE IF NOT EXISTS bid_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT NOT NULL,
            event_ticker    TEXT NOT NULL,
            ticker          TEXT NOT NULL,
            city            TEXT,
            bucket_label    TEXT,
            side            TEXT DEFAULT 'no',
            contracts       INTEGER,
            no_price_cents  INTEGER,
            open_interest   REAL,
            was_first       INTEGER DEFAULT 0,
            dry_run         INTEGER DEFAULT 1,
            order_id        TEXT,
            status          TEXT,
            placed_at       TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            ms_after_open   INTEGER
        );

        CREATE TABLE IF NOT EXISTS session_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT NOT NULL,
            event       TEXT NOT NULL,
            detail      TEXT,
            ts          TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        );
        """)
        # Migration: add columns if upgrading from older schema
        _safe_add_column(con, "market_timing",  "first_bid_time",          "TEXT")
        _safe_add_column(con, "market_buckets", "first_bid_time",          "TEXT")
        _safe_add_column(con, "market_buckets", "first_trade_et",          "TEXT")
        _safe_add_column(con, "market_buckets", "first_trade_contracts",   "REAL")
        _safe_add_column(con, "market_buckets", "first_trade_yes_price",   "REAL")
        _safe_add_column(con, "market_buckets", "first_trade_no_price",    "REAL")
        _safe_add_column(con, "market_buckets", "first_trade_taker_side",  "TEXT")


def _safe_add_column(con, table: str, col: str, dtype: str) -> None:
    """Add column if it doesn't exist (handles schema migration)."""
    try:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {dtype}")
    except sqlite3.OperationalError:
        pass   # column already exists


# ── market_timing ─────────────────────────────────────────────────────────────

def upsert_market_timing(event_ticker: str, series_ticker: str, city: str,
                          kind: str, settlement_date: str,
                          created_time: str, open_time: str) -> None:
    with _conn() as con:
        con.execute("""
            INSERT INTO market_timing
                (event_ticker, series_ticker, city, kind, settlement_date,
                 created_time, open_time)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(event_ticker) DO UPDATE SET
                created_time = excluded.created_time,
                open_time    = excluded.open_time
        """, (event_ticker, series_ticker, city, kind, settlement_date,
              created_time, open_time))


def _utc_to_et(utc_iso: str) -> str:
    """Convert UTC ISO string to 'YYYY-MM-DD HH:MM:SS.mmm ET' (EDT = UTC-4)."""
    from datetime import datetime, timezone, timedelta
    _EDT = timezone(timedelta(hours=-4))
    dt = datetime.fromisoformat(utc_iso.replace("Z", "+00:00"))
    et = dt.astimezone(_EDT)
    return et.strftime("%Y-%m-%d %H:%M:%S.") + f"{et.microsecond//1000:03d}" + " ET"


def upsert_first_trade_data(ticker: str, utc_iso: str,
                             contracts: float, yes_price: float,
                             no_price: float, taker_side: str) -> None:
    """
    Write all first-trade fields for a bucket (from Kalshi GET /markets/trades).
    Always overwrites — caller controls whether to skip already-populated rows.
    ET time is computed from utc_iso automatically.
    """
    et_str = _utc_to_et(utc_iso)
    with _conn() as con:
        con.execute("""
            UPDATE market_buckets SET
                first_bid_time          = ?,
                first_trade_et          = ?,
                first_trade_contracts   = ?,
                first_trade_yes_price   = ?,
                first_trade_no_price    = ?,
                first_trade_taker_side  = ?
            WHERE ticker = ?
        """, (utc_iso, et_str, contracts, yes_price, no_price, taker_side, ticker))
        # Roll up first_bid_time to market_timing (event-level; only if not set)
        con.execute("""
            UPDATE market_timing SET first_bid_time = ?
            WHERE event_ticker = (
                SELECT event_ticker FROM market_buckets WHERE ticker = ?
            ) AND first_bid_time IS NULL
        """, (utc_iso, ticker))


def upsert_first_trade_time(ticker: str, iso_ts: str) -> None:
    """
    Legacy: record only the first-trade UTC time (no price/contract data).
    Kept for backward compat; prefer upsert_first_trade_data for new calls.
    Only writes if first_bid_time is currently NULL.
    """
    with _conn() as con:
        con.execute("""
            UPDATE market_buckets SET first_bid_time = ?
            WHERE ticker = ? AND first_bid_time IS NULL
        """, (iso_ts, ticker))
        con.execute("""
            UPDATE market_timing SET first_bid_time = ?
            WHERE event_ticker = (
                SELECT event_ticker FROM market_buckets WHERE ticker = ?
            ) AND first_bid_time IS NULL
        """, (iso_ts, ticker))


def upsert_first_bid_time(ticker: str, ts_ms: int) -> None:
    """Record first-bid timestamp on a bucket (from WS OI 0→non-zero)."""
    iso = _ms_to_iso(ts_ms)
    # Update market_buckets by ticker
    with _conn() as con:
        con.execute("""
            UPDATE market_buckets SET first_bid_time = ?
            WHERE ticker = ? AND first_bid_time IS NULL
        """, (iso, ticker))
        # Also update market_timing for the event this bucket belongs to
        con.execute("""
            UPDATE market_timing SET first_bid_time = ?
            WHERE event_ticker = (
                SELECT event_ticker FROM market_buckets WHERE ticker = ?
            ) AND first_bid_time IS NULL
        """, (iso, ticker))


def _ms_to_iso(ts_ms: int) -> str:
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3] + "Z"


def upsert_market_bucket(event_ticker: str, ticker: str, bucket_label: str,
                          floor_strike, cap_strike,
                          created_time: str, open_time: str) -> None:
    with _conn() as con:
        con.execute("""
            INSERT INTO market_buckets
                (event_ticker, ticker, bucket_label, floor_strike, cap_strike,
                 created_time, open_time)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(ticker) DO NOTHING
        """, (event_ticker, ticker, bucket_label, floor_strike, cap_strike,
              created_time, open_time))


def get_market_timing_history(days: int = 30) -> list[dict]:
    with _conn() as con:
        rows = con.execute("""
            SELECT mt.*,
                   (SELECT MIN(mb.first_bid_time)
                    FROM market_buckets mb
                    WHERE mb.event_ticker = mt.event_ticker
                      AND mb.first_bid_time IS NOT NULL
                   ) AS first_bucket_bid_time
            FROM market_timing mt
            ORDER BY settlement_date DESC
            LIMIT ?
        """, (days * 40,)).fetchall()
    return [dict(r) for r in rows]


def get_bucket_timing(event_ticker: str) -> list[dict]:
    """Return all buckets for an event with their first_bid_time."""
    with _conn() as con:
        rows = con.execute("""
            SELECT * FROM market_buckets
            WHERE event_ticker = ?
            ORDER BY floor_strike
        """, (event_ticker,)).fetchall()
    return [dict(r) for r in rows]


def get_first_trades_for_research() -> list[dict]:
    """
    Return all buckets that have first-trade data, joined with market_timing
    for city/kind/settlement context.  Used by the Research tab.
    ms_after_open is computed from open_time vs first_bid_time.
    """
    from datetime import datetime, timezone
    with _conn() as con:
        rows = con.execute("""
            SELECT
                mb.ticker,
                mb.event_ticker,
                mb.bucket_label,
                mb.floor_strike,
                mb.cap_strike,
                mb.open_time,
                mb.first_bid_time         AS first_trade_utc,
                mb.first_trade_et,
                mb.first_trade_contracts,
                mb.first_trade_yes_price,
                mb.first_trade_no_price,
                mb.first_trade_taker_side,
                mt.city,
                mt.kind,
                mt.settlement_date,
                mt.series_ticker
            FROM market_buckets mb
            LEFT JOIN market_timing mt ON mt.event_ticker = mb.event_ticker
            WHERE mb.first_bid_time IS NOT NULL
            ORDER BY mt.settlement_date DESC, mt.city, mt.kind, mb.floor_strike
        """).fetchall()

    result = []
    for r in rows:
        d = dict(r)
        utc = d.get("first_trade_utc", "")
        ot  = d.get("open_time", "")
        if utc and ot:
            try:
                ft  = datetime.fromisoformat(utc.replace("Z", "+00:00"))
                ot_ = datetime.fromisoformat(ot.replace("Z",  "+00:00"))
                d["ms_after_open"] = int((ft - ot_).total_seconds() * 1000)
            except Exception:
                d["ms_after_open"] = None
        else:
            d["ms_after_open"] = None
        result.append(d)
    return result


# ── bid_log ───────────────────────────────────────────────────────────────────

def insert_bid(date: str, event_ticker: str, ticker: str, city: str,
               bucket_label: str, contracts: int, no_price_cents: int,
               open_interest: float, was_first: bool, dry_run: bool,
               order_id: str | None, status: str,
               ms_after_open: int | None) -> int:
    with _conn() as con:
        cur = con.execute("""
            INSERT INTO bid_log
                (date, event_ticker, ticker, city, bucket_label,
                 contracts, no_price_cents, open_interest,
                 was_first, dry_run, order_id, status, ms_after_open)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (date, event_ticker, ticker, city, bucket_label,
              contracts, no_price_cents, open_interest,
              1 if was_first else 0, 1 if dry_run else 0,
              order_id, status, ms_after_open))
        return cur.lastrowid


def get_bid_history(days: int = 14) -> list[dict]:
    with _conn() as con:
        rows = con.execute("""
            SELECT * FROM bid_log
            ORDER BY placed_at DESC
            LIMIT ?
        """, (days * 200,)).fetchall()
    return [dict(r) for r in rows]


# ── session_log ───────────────────────────────────────────────────────────────

def log_event(date: str, event: str, detail: str = "") -> None:
    with _conn() as con:
        con.execute(
            "INSERT INTO session_log (date, event, detail) VALUES (?,?,?)",
            (date, event, detail)
        )


def get_session_log(date: str | None = None, limit: int = 200) -> list[dict]:
    with _conn() as con:
        if date:
            rows = con.execute(
                "SELECT * FROM session_log WHERE date=? ORDER BY ts DESC LIMIT ?",
                (date, limit)
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT * FROM session_log ORDER BY ts DESC LIMIT ?",
                (limit,)
            ).fetchall()
    return [dict(r) for r in rows]
