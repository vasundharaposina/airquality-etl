"""Streamlit dashboard on top of the SQLite database built by etl.py.

Run:  streamlit run dashboard.py
"""
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

DB_PATH = Path("data/pipeline.db")

st.set_page_config(page_title="Weather & Air Quality", layout="wide")
st.title("Weather and Air-Quality Dashboard")

if not DB_PATH.exists():
    st.warning("No database found yet. Run `python etl.py` first.")
    st.stop()


@st.cache_data(ttl=60)
def query(sql):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(sql, conn)


readings = query("SELECT * FROM readings ORDER BY observed_at")
runs = query("SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT 50")

if readings.empty:
    st.warning("The readings table is empty. Run `python etl.py` first.")
    st.stop()

readings["observed_at"] = pd.to_datetime(readings["observed_at"])

cities = sorted(readings["city"].unique())
selected = st.sidebar.multiselect("Cities", cities, default=cities)
data = readings[readings["city"].isin(selected)]

st.subheader("Latest reading per city")
latest = data.sort_values("observed_at").groupby("city").tail(1).set_index("city")
cols = st.columns(max(len(latest), 1))
for col, (city, row) in zip(cols, latest.iterrows()):
    aqi = "n/a" if pd.isna(row["us_aqi"]) else f"{row['us_aqi']:.0f}"
    temp = "n/a" if pd.isna(row["temperature_c"]) else f"{row['temperature_c']:.1f} °C"
    col.metric(city, f"AQI {aqi}", temp)

metrics = {
    "us_aqi": "US AQI",
    "pm2_5": "PM2.5 (µg/m³)",
    "temperature_c": "Temperature (°C)",
    "humidity_pct": "Humidity (%)",
    "precipitation_mm": "Precipitation (mm)",
}
choice = st.selectbox("Metric", list(metrics), format_func=metrics.get)
chart = data.pivot(index="observed_at", columns="city", values=choice)
st.line_chart(chart)

st.subheader("Average by city")
summary = data.groupby("city")[list(metrics)].mean().round(1)
st.dataframe(summary.rename(columns=metrics), use_container_width=True)

st.subheader("Pipeline run log")
st.dataframe(runs, use_container_width=True)
