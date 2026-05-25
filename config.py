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
    ("KXHIGHWDC",  "Washington DC",  "high"),
    ("KXLOWTWDC",  "Washington DC",  "low"),
    ("KXHIGHSEA",  "Seattle",        "high"),
    ("KXLOWTSEA",  "Seattle",        "low"),
    ("KXHIGHSFO",  "San Francisco",  "high"),
    ("KXLOWTSFO",  "San Francisco",  "low"),
    ("KXHIGHSAT",  "San Antonio",    "high"),
    ("KXLOWTSAT",  "San Antonio",    "low"),
    ("KXHIGHPHL",  "Philadelphia",   "high"),
    ("KXHIGHPHX",  "Phoenix",        "high"),
    ("KXLOWTPHX",  "Phoenix",        "low"),
    ("KXHIGHMSP",  "Minneapolis",    "high"),
    ("KXHIGHMSQ",  "Miami",          "high"),
    ("KXLOWTMSQ",  "Miami",          "low"),
    ("KXHIGHLAX",  "Los Angeles",    "high"),
    ("KXHIGHLAS",  "Las Vegas",      "high"),
    ("KXHIGHIAH",  "Houston",        "high"),
    ("KXLOWTIAH",  "Houston",        "low"),
    ("KXHIGHDAL",  "Dallas",         "high"),
    ("KXLOWTDAL",  "Dallas",         "low"),
    ("KXHIGHDEN",  "Denver",         "high"),
    ("KXLOWTDEN",  "Denver",         "low"),
    ("KXHIGHCHI",  "Chicago",        "high"),
    ("KXLOWTCHI",  "Chicago",        "low"),
    ("KXHIGHAUS",  "Austin",         "high"),
    ("KXLOWTAUS",  "Austin",         "low"),
    ("KXHIGHTATL", "Atlanta",        "high"),
    ("KXHIGHBOS",  "Boston",         "high"),
    ("KXHIGHNYC",  "New York City",  "high"),
    ("KXLOWTNYC",  "New York City",  "low"),
    ("KXHIGHMSY",  "New Orleans",    "high"),
    ("KXHIGHOKC",  "Oklahoma City",  "high"),
]

# Z→A sort by city display name (user requirement: bid in Z→A order)
WEATHER_SERIES = sorted(WEATHER_SERIES, key=lambda x: x[1], reverse=True)
