import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

API_URL = "https://api.open-meteo.com/v1/forecast"
LOCATIONS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".github", "locations.json"
)


def load_locations(path: str = LOCATIONS_PATH) -> List[Dict]:
    with open(path) as f:
        return json.load(f)


LOCATIONS = load_locations()


def get_todays_location(now: Optional[datetime] = None) -> Dict:
    """Pick a location by day-of-year (UTC): same city all day, rotates daily."""
    now = now or datetime.now(timezone.utc)
    return LOCATIONS[now.timetuple().tm_yday % len(LOCATIONS)]


def is_valid_reading(temp_c: float, humidity: int) -> bool:
    """Reject physically implausible readings instead of storing bad data."""
    if not (-90 <= temp_c <= 60):
        return False
    if not (0 <= humidity <= 100):
        return False
    return True


def build_url(location: Dict) -> str:
    return (
        f"{API_URL}?latitude={location['lat']}&longitude={location['lon']}"
        "&current=temperature_2m,relative_humidity_2m"
    )


def fetch_with_retry(
    url: str, headers: dict, retries: int = 3, delay: int = 5
) -> Optional[requests.Response]:
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=20)
            if response.status_code == 200:
                return response
            logger.warning(
                "Attempt %d/%d got status %s, retrying", attempt, retries, response.status_code
            )
        except requests.exceptions.RequestException as e:
            logger.warning("Attempt %d/%d network error: %s", attempt, retries, e)
        if attempt < retries:
            time.sleep(delay)
    return None


def parse_current(payload: dict) -> Tuple[float, int, str]:
    """Return (temp_c, humidity, observed_at) from an Open-Meteo response."""
    current = payload["current"]
    return (
        float(current["temperature_2m"]),
        int(current["relative_humidity_2m"]),
        str(current["time"]),
    )


def init_db(conn: sqlite3.Connection) -> None:
    """Create the table if needed and migrate older databases in place."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS weather
           (timestamp TEXT, temp_c REAL, humidity INTEGER, city TEXT, observed_at TEXT)"""
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(weather)")}
    if "observed_at" not in columns:  # databases created before this column existed
        conn.execute("ALTER TABLE weather ADD COLUMN observed_at TEXT")
    # Same city + same API observation can only be stored once (rerun-safe).
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_weather_city_observed "
        "ON weather (city, observed_at)"
    )
    conn.commit()


def save_reading(conn: sqlite3.Connection, entry: tuple) -> bool:
    """Insert one row; return False if this observation was already stored."""
    cur = conn.execute(
        "INSERT OR IGNORE INTO weather "
        "(timestamp, temp_c, humidity, city, observed_at) VALUES (?, ?, ?, ?, ?)",
        entry,
    )
    conn.commit()
    return cur.rowcount == 1


def run_pipeline(db_path: Optional[str] = None) -> None:
    db_path = (
        db_path
        or os.environ.get("WEATHER_DB_PATH")
        or os.path.join(os.getcwd(), "weather_data.db")
    )
    location = get_todays_location()
    city_label = f"{location['city']}, {location['country']}"
    try:
        logger.info("Today's location: %s", city_label)
        # Identify the client politely to the public API.
        headers = {"User-Agent": "PythonWeatherPipeline/1.0"}
        response = fetch_with_retry(build_url(location), headers)

        if response is None:
            logger.error("All retry attempts failed")
            sys.exit(1)

        temp_c, humidity, observed_at = parse_current(response.json())

        # Data-quality gate: never let a garbled response reach the table.
        if not is_valid_reading(temp_c, humidity):
            logger.error(
                "REJECTED implausible reading temp=%s humidity=%s; not inserted",
                temp_c,
                humidity,
            )
            sys.exit(1)

        entry = (
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),  # ingested_at
            temp_c,
            humidity,
            city_label,
            observed_at,  # when the API says the reading was taken
        )

        conn = sqlite3.connect(db_path)
        try:
            init_db(conn)
            inserted = save_reading(conn, entry)
        finally:
            conn.close()

        if inserted:
            logger.info("Loaded %s into %s", entry, db_path)
        else:
            logger.info("Observation %s for %s already stored; skipped", observed_at, city_label)

    except Exception as e:  # any other failure must also fail the run
        logger.error("PIPELINE ERROR: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()
