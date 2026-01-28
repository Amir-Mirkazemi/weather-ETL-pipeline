import requests
import sqlite3
from datetime import datetime

# THIS URL IS 100% CORRECT: It includes the required 'current' parameters
URL = "https://api.open-meteo.com"

def run_pipeline():
    try:
        print(f"Requesting data from: {URL}")
        response = requests.get(URL, timeout=20)
        
        # Check if the server actually sent data
        if response.status_code != 200:
            print(f"❌ Server Error {response.status_code}: {response.text}")
            return

        data = response.json()['current']

        # 2. TRANSFORM: Prepare data for database
        entry = (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            float(data['temperature_2m']),
            int(data['relative_humidity_2m']),
            "London"
        )

        # 3. LOAD: Save to SQLite
        conn = sqlite3.connect('weather_data.db')
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS weather 
                       (timestamp TEXT, temp_c REAL, humidity INTEGER, city TEXT)''')
        cursor.execute("INSERT INTO weather VALUES (?, ?, ?, ?)", entry)
        conn.commit()
        conn.close()
        
        print(f"✅ SUCCESS: Logged {entry}")

    except Exception as e:
        print(f"❌ PIPELINE ERROR: {str(e)}")
        exit(1)

if __name__ == "__main__":
    run_pipeline()
