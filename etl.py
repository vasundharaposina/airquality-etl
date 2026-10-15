"""Weather and air-quality ETL pipeline.

Extract  : hourly weather + air-quality data for several Indian cities from the
           free Open-Meteo APIs (no API key needed).
Transform: clean and validate with Pandas (types, missing values, duplicates,
           out-of-range readings).
Load     : SQLite database with a raw table (original JSON), a cleaned table
           (one row per city per hour) and a run-log table.

Run:  python etl.py
Re-running is safe: rows are upserted on (city, observed_at), so no duplicates.
"""
import json
import logging
import sqlite3
import time
from pathlib import Path

import pandas as pd
import requests

DB_PATH = Path("data/pipeline.db")
LOG_PATH = Path("data/pipeline.log")
TIMEZONE = "Asia/Kolkata"

CITIES = {
    "Bengaluru": (12.9716, 77.5946),
    "Hyderabad": (17.3850, 78.4867),
    "Delhi": (28.6139, 77.2090),
    "Mumbai": (19.0760, 72.8777),
    "Chennai": (13.0827, 80.2707),
}

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
AIR_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
WEATHER_VARS = "temperature_2m,relative_humidity_2m,precipitation"
AIR_VARS = "pm10,pm2_5,us_aqi"

# Valid ranges used by the data-quality check.
LIMITS = {
    "temperature_c": (-10, 55),
    "humidity_pct": (0, 100),
    "precipitation_mm": (0, 500),
    "pm10": (0, 2000),
    "pm2_5": (0, 1000),
    "us_aqi": (0, 500),
}

log = logging.getLogger("etl")


# ---------------------------------------------------------------- Extract
def fetch_json(url, params, retries=3, backoff=2):
    """GET a JSON payload, retrying on network or HTTP errors."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            log.warning("Attempt %d/%d failed for %s: %s", attempt, retries, url, exc)
            if attempt == retries:
                raise
            time.sleep(backoff ** attempt)


def extract(city, lat, lon):
    base = {"latitude": lat, "longitude": lon, "timezone": TIMEZONE,
            "past_days": 1, "forecast_days": 1}
    weather = fetch_json(WEATHER_URL, {**base, "hourly": WEATHER_VARS})
    air = fetch_json(AIR_URL, {**base, "hourly": AIR_VARS})
    return weather, air


# -------------------------------------------------------------- Transform
def transform(city, weather, air):
    """Join weather and air-quality hourly data into one tidy DataFrame."""
    w = pd.DataFrame(weather["hourly"]).rename(columns={
        "temperature_2m": "temperature_c",
        "relative_humidity_2m": "humidity_pct",
        "precipitation": "precipitation_mm",
    })
    a = pd.DataFrame(air["hourly"])
    df = w.merge(a, on="time", how="outer").rename(columns={"time": "observed_at"})

    df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")
    for col in LIMITS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["observed_at"])
    # Keep only hours that have already happened (the API also returns forecasts).
    now = pd.Timestamp.now(tz=TIMEZONE).tz_localize(None)
    df = df[df["observed_at"] <= now]
    # Drop rows with no measurements at all, and duplicate hours.
    df = df.dropna(subset=list(LIMITS), how="all")
    df = df.drop_duplicates(subset="observed_at", keep="last")

    df.insert(0, "city", city)
    df["observed_at"] = df["observed_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df.reset_index(drop=True)


def validate(df):
    """Reject rows with out-of-range values. Returns (clean_df, rejected_count)."""
    ok = pd.Series(True, index=df.index)
    for col, (lo, hi) in LIMITS.items():
        # Missing values are allowed; present values must be inside the range.
        ok &= df[col].isna() | df[col].between(lo, hi)
    return df[ok].reset_index(drop=True), int((~ok).sum())


# ------------------------------------------------------------------- Load
def init_db(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS raw_payloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at TEXT NOT NULL,
            city TEXT NOT NULL,
            source TEXT NOT NULL,
            payload TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS readings (
            city TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            temperature_c REAL,
            humidity_pct REAL,
            precipitation_mm REAL,
            pm10 REAL,
            pm2_5 REAL,
            us_aqi REAL,
            PRIMARY KEY (city, observed_at)
        );
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at TEXT NOT NULL,
            city TEXT NOT NULL,
            rows_extracted INTEGER,
            rows_loaded INTEGER,
            rows_rejected INTEGER,
            status TEXT NOT NULL,
            message TEXT
        );
    """)


def load_raw(conn, city, weather, air, fetched_at):
    conn.executemany(
        "INSERT INTO raw_payloads (fetched_at, city, source, payload) VALUES (?, ?, ?, ?)",
        [(fetched_at, city, "weather", json.dumps(weather)),
         (fetched_at, city, "air_quality", json.dumps(air))],
    )


def load_clean(conn, df):
    """Upsert cleaned rows so re-running the pipeline never creates duplicates."""
    cols = ["city", "observed_at", *LIMITS]
    rows = [tuple(None if pd.isna(v) else v for v in r)
            for r in df[cols].itertuples(index=False, name=None)]
    conn.executemany(
        f"INSERT OR REPLACE INTO readings ({', '.join(cols)}) "
        f"VALUES ({', '.join('?' * len(cols))})",
        rows,
    )
    return len(rows)


# -------------------------------------------------------------------- Run
def run():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
    )
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    run_at = pd.Timestamp.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
    failures = 0

    for city, (lat, lon) in CITIES.items():
        try:
            weather, air = extract(city, lat, lon)
            load_raw(conn, city, weather, air, run_at)
            df = transform(city, weather, air)
            clean, rejected = validate(df)
            loaded = load_clean(conn, clean)
            conn.execute(
                "INSERT INTO pipeline_runs (run_at, city, rows_extracted, rows_loaded, "
                "rows_rejected, status) VALUES (?, ?, ?, ?, ?, 'success')",
                (run_at, city, len(df), loaded, rejected),
            )
            log.info("%s: extracted=%d loaded=%d rejected=%d", city, len(df), loaded, rejected)
        except Exception as exc:  # keep going so one bad city doesn't stop the run
            failures += 1
            log.error("%s failed: %s", city, exc)
            conn.execute(
                "INSERT INTO pipeline_runs (run_at, city, status, message) "
                "VALUES (?, ?, 'failed', ?)",
                (run_at, city, str(exc)),
            )
        conn.commit()

    conn.close()
    log.info("Run finished with %d failed city(ies).", failures)
    return failures


if __name__ == "__main__":
    raise SystemExit(1 if run() else 0)
