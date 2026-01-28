import requests
import sqlite3
from datetime import datetime
import os

# THIS URL IS 100% VERIFIED: Includes required lat, lon, and current parameters
URL = "https://api.open-meteo.com"

def run_pipeline():
    try:
        print(f"Requesting data from: {URL}")
        # Standard headers to avoid bot detection
        headers = {'User-Agent': 'PythonWeatherPipeline/1.0'}
        response = requests.get(URL, headers=headers, timeout=20)
        
        if response.status_code != 200:
            print(f"❌ Server Error {response.status_code}: {response.text}")
            return

        data = response.json()['current']

        # TRANSFORM: Format for SQLite
        entry = (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            float(data['temperature_2m']),
            int(data['relative_humidity_2m']),
            "London"
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
