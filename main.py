import json
import requests
import sqlite3
from datetime import datetime, timezone
import os
import time
from typing import Optional, Dict, List
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Rotates through these locations by day-of-year, so a different country is
# picked each day (same one all day, since the job runs hourly).
def load_locations() -> List[Dict]:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.github', 'locations.json')
    with open(path) as f:
        return json.load(f)


LOCATIONS = load_locations()


def get_todays_location() -> Dict:
    day_of_year = datetime.now(timezone.utc).timetuple().tm_yday
    return LOCATIONS[day_of_year % len(LOCATIONS)]


def is_valid_reading(temp_c: float, humidity: int) -> bool:
    """Reject physically implausible readings instead of storing bad data.

    Earth's recorded surface temperatures never exceed roughly -90C to 60C,
    and relative humidity is a percentage, so it must fall within 0-100.
    """
    if not (-90 <= temp_c <= 60):
        return False
    if not (0 <= humidity <= 100):
        return False
    return True

def fetch_with_retry(url: str, headers: dict, retries: int = 3, delay: int = 5) -> Optional[requests.Response]:
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=20)
            if response.status_code == 200:
                return response
            logger.warning(f"⚠️ Attempt {attempt}/{retries} got status {response.status_code}, retrying...")
        except requests.exceptions.RequestException as e:
            logger.warning(f"⚠️ Attempt {attempt}/{retries} network error: {e}, retrying...")
        if attempt < retries:
            time.sleep(delay)
    return None

def run_pipeline() -> None:
    location = get_todays_location()
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={location['lat']}&longitude={location['lon']}"
        "&current=temperature_2m,relative_humidity_2m"
    )
    try:
        logger.info(f"Today's location: {location['city']}, {location['country']}")
        logger.info(f"Requesting data from: {url}")
        # Standard headers to avoid bot detection
        headers = {'User-Agent': 'PythonWeatherPipeline/1.0'}
        response = fetch_with_retry(url, headers)

        if response is None:
            logger.error("❌ All retry attempts failed")
            exit(1)

        data = response.json()['current']
        temp_c = float(data['temperature_2m'])
        humidity = int(data['relative_humidity_2m'])

        # DATA QUALITY CHECK: don't let a garbled API response poison the table
        if not is_valid_reading(temp_c, humidity):
            logger.error(f"❌ REJECTED: implausible reading temp={temp_c}, humidity={humidity} — not inserted")
            exit(1)

        # TRANSFORM: Format for SQLite
        entry = (
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            temp_c,
            humidity,
            f"{location['city']}, {location['country']}"
        )

        #finds the DB file
        db_path = os.path.join(os.getcwd(), 'weather_data.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS weather
                       (timestamp TEXT, temp_c REAL, humidity INTEGER, city TEXT)''')
        cursor.execute("INSERT INTO weather VALUES (?, ?, ?, ?)", entry)
        conn.commit()
        conn.close()

        logger.info(f"✅ SUCCESS: Logged {entry} to {db_path}")

    except Exception as e:
        logger.error(f"❌ PIPELINE ERROR: {str(e)}")
        exit(1)


if __name__ == "__main__":
    run_pipeline()
