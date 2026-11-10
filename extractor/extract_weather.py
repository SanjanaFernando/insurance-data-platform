"""Extract daily weather for Colombo from the Open-Meteo archive API (no API key needed).
The request window is split into monthly chunks (our 'pagination'); every call retries with backoff.
Usage: python -m extractor.extract_weather 2024-01-01 2026-10-31 [--load]"""
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL = "https://archive-api.open-meteo.com/v1/archive"
LAT, LON = 6.9271, 79.8612          # Colombo
log = logging.getLogger("weather")


def month_chunks(start: date, end: date):
    """Yield (chunk_start, chunk_end) pairs that never cross a month boundary."""
    cur = start
    while cur <= end:
        next_month = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        yield cur, min(end, next_month - timedelta(days=1))
        cur = next_month


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_chunk(session, start: date, end: date) -> dict:
    params = {"latitude": LAT, "longitude": LON, "start_date": start.isoformat(),
              "end_date": end.isoformat(), "daily": "temperature_2m_max,precipitation_sum",
              "timezone": "Asia/Colombo"}
    response = session.get(URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def to_frame(payload: dict) -> pd.DataFrame:
    daily = payload["daily"]
    return pd.DataFrame({"weather_date": daily["time"],
                         "temp_max_c": daily["temperature_2m_max"],
                         "precip_mm": daily["precipitation_sum"]})


def extract(start: date, end: date, session=None) -> pd.DataFrame:
    session = session or make_session()
    frames = []
    for chunk_start, chunk_end in month_chunks(start, end):
        log.info("fetching %s .. %s", chunk_start, chunk_end)
        frames.append(to_frame(fetch_chunk(session, chunk_start, chunk_end)))
    return pd.concat(frames, ignore_index=True).drop_duplicates("weather_date")


def main(argv):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start, end = date.fromisoformat(argv[1]), date.fromisoformat(argv[2])
    df = extract(start, end)
    out = Path("data/weather_daily.csv")
    out.parent.mkdir(exist_ok=True)
    df.to_csv(out, index=False)
    log.info("wrote %s rows to %s", len(df), out)
    if "--load" in argv:
        from load_raw import connect, load_csv
        conn = connect()
        load_csv(conn, str(out), "weather_daily")
        conn.close()


if __name__ == "__main__":
    main(sys.argv)
