"""
Weather Speed Bot — configuration.
All secrets loaded from .env (VPS only, never committed).
"""
from __future__ import annotations
import os
from dotenv import load_dotenv

load_dotenv()

# ── Kalshi ─────────────────────────────────────────────────────────────────────
KALSHI_API_KEY          = os.getenv("KALSHI_API_KEY", "")
KALSHI_PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "kalshi_private_key")
KALSHI_REST_BASE        = "https://external-api.kalshi.com/trade-api/v2"

# ── App ────────────────────────────────────────────────────────────────────────
PORT = int(os.getenv("PORT", 8002))

# ── Weather series tracked ─────────────────────────────────────────────────────
# All Kalshi weather series we monitor — highs and lows for all cities.
# Sorted Z→A by city name for bid ordering.
WEATHER_SERIES = [
    # (series_ticker, city_display_name, kind)
    # All tickers verified live against Kalshi API — 33 active series.
    # Naming is inconsistent on Kalshi's side: some HIGH use KXHIGHT-prefix,
    # some use KXHIGH-prefix; LOW always uses KXLOWT-prefix.

    # Atlanta
    ("KXHIGHTATL", "Atlanta",        "high"),
    ("KXLOWTATL",  "Atlanta",        "low"),

    # Austin
    ("KXHIGHAUS",  "Austin",         "high"),
    ("KXLOWTAUS",  "Austin",         "low"),

    # Boston
    ("KXHIGHTBOS", "Boston",         "high"),
    ("KXLOWTBOS",  "Boston",         "low"),

    # Chicago
    ("KXHIGHCHI",  "Chicago",        "high"),
    ("KXLOWTCHI",  "Chicago",        "low"),

    # Dallas
    ("KXHIGHTDAL", "Dallas",         "high"),
    ("KXLOWTDAL",  "Dallas",         "low"),

    # Denver
    ("KXHIGHDEN",  "Denver",         "high"),
    ("KXLOWTDEN",  "Denver",         "low"),

    # Houston
    ("KXHIGHTHOU", "Houston",        "high"),
    ("KXLOWTHOU",  "Houston",        "low"),

    # Las Vegas
    ("KXHIGHTLV",  "Las Vegas",      "high"),
    ("KXLOWTLV",   "Las Vegas",      "low"),

    # Los Angeles
    ("KXHIGHLAX",  "Los Angeles",    "high"),
    ("KXLOWTLAX",  "Los Angeles",    "low"),

    # Miami
    ("KXLOWTMIA",  "Miami",          "low"),   # no high-temp series on Kalshi

    # Minneapolis
    ("KXHIGHTMIN", "Minneapolis",    "high"),
    ("KXLOWTMIN",  "Minneapolis",    "low"),

    # New Orleans
    ("KXHIGHTNOLA","New Orleans",    "high"),
    ("KXLOWTNOLA", "New Orleans",    "low"),

    # New York City
    ("KXLOWTNYC",  "New York City",  "low"),   # no high-temp series on Kalshi

    # Oklahoma City
    ("KXHIGHTOKC", "Oklahoma City",  "high"),
    ("KXLOWTOKC",  "Oklahoma City",  "low"),

    # Philadelphia
    ("KXLOWTPHIL", "Philadelphia",   "low"),   # no high-temp series on Kalshi

    # Phoenix
    ("KXHIGHTPHX", "Phoenix",        "high"),
    ("KXLOWTPHX",  "Phoenix",        "low"),

    # San Francisco
    ("KXHIGHTSFO", "San Francisco",  "high"),
    ("KXLOWTSFO",  "San Francisco",  "low"),

    # Seattle
    ("KXHIGHTSEA", "Seattle",        "high"),
    ("KXLOWTSEA",  "Seattle",        "low"),
]

# Z→A sort by city display name (user requirement: bid in Z→A order)
WEATHER_SERIES = sorted(WEATHER_SERIES, key=lambda x: x[1], reverse=True)
