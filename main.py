import requests
import sqlite3
from datetime import datetime
import os

# Verified Open-Meteo URL
URL = "https://api.open-meteo.com"

def run_pipeline():
    try:
        response = requests.get(URL, timeout=20)
        response.raise_for_status()
        data = response.json()['current']

        entry = (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            float(data['temperature_2m']),
            int(data['relative_humidity_2m']),
            "London"
        )

        # Get the directory where main.py is located to ensure file is saved correctly
        base_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(base_dir, 'weather_data.db')

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS weather 
                       (timestamp TEXT, temp_c REAL, humidity INTEGER, city TEXT)''')
        cursor.execute("INSERT INTO weather VALUES (?, ?, ?, ?)", entry)
        conn.commit()
        conn.close()
        
        print(f"✅ SUCCESS: Saved {entry} to {db_path}")

    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        exit(1)

if __name__ == "__main__":
    run_pipeline()
