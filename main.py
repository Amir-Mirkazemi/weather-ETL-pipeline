import requests
import sqlite3
from datetime import datetime
import os

# Rotates through these locations by day-of-year, so a different country is
# picked each day (same one all day, since the job runs hourly).
LOCATIONS = [
    {"city": "London", "country": "UK", "lat": 51.5074, "lon": -0.1278},
    {"city": "Tehran", "country": "Iran", "lat": 35.6892, "lon": 51.3890},
    {"city": "New York", "country": "USA", "lat": 40.7128, "lon": -74.0060},
    {"city": "Tokyo", "country": "Japan", "lat": 35.6762, "lon": 139.6503},
    {"city": "Sydney", "country": "Australia", "lat": -33.8688, "lon": 151.2093},
    {"city": "Cairo", "country": "Egypt", "lat": 30.0444, "lon": 31.2357},
    {"city": "Sao Paulo", "country": "Brazil", "lat": -23.5505, "lon": -46.6333},
]


def get_todays_location():
    day_of_year = datetime.now().timetuple().tm_yday
    return LOCATIONS[day_of_year % len(LOCATIONS)]


def is_valid_reading(temp_c, humidity):
    """Reject physically implausible readings instead of storing bad data.

    Earth's recorded surface temperatures never exceed roughly -90C to 60C,
    and relative humidity is a percentage, so it must fall within 0-100.
    """
    if not (-90 <= temp_c <= 60):
        return False
    if not (0 <= humidity <= 100):
        return False
    return True


def run_pipeline():
    location = get_todays_location()
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={location['lat']}&longitude={location['lon']}"
        "&current=temperature_2m,relative_humidity_2m"
    )
    try:
        print(f"Today's location: {location['city']}, {location['country']}")
        print(f"Requesting data from: {url}")
        # Standard headers to avoid bot detection
        headers = {'User-Agent': 'PythonWeatherPipeline/1.0'}
        response = requests.get(url, headers=headers, timeout=20)

        if response.status_code != 200:
            print(f"❌ Server Error {response.status_code}: {response.text}")
            exit(1)

        data = response.json()['current']
        temp_c = float(data['temperature_2m'])
        humidity = int(data['relative_humidity_2m'])

        # DATA QUALITY CHECK: don't let a garbled API response poison the table
        if not is_valid_reading(temp_c, humidity):
            print(f"❌ REJECTED: implausible reading temp={temp_c}, humidity={humidity} — not inserted")
            exit(1)

        # TRANSFORM: Format for SQLite
        entry = (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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

        print(f"✅ SUCCESS: Logged {entry} to {db_path}")

    except Exception as e:
        print(f"❌ PIPELINE ERROR: {str(e)}")
        exit(1)


if __name__ == "__main__":
    run_pipeline()
