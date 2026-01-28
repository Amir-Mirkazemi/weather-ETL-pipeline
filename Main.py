import requests
import sqlite3
from datetime import datetime

# 1. EXTRACT: Use Open-Meteo (More stable than wttr.in for automation)
# Coordinates for London; change 'latitude' and 'longitude' for your city
url = "https://api.open-meteo.com"

try:
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    data = response.json()['current_weather']

    # 2. TRANSFORM: Map the API response to our schema
    weather_entry = (
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        float(data['temperature']),
        int(data['windspeed']), # Using windspeed as a placeholder for humidity if preferred
        f"Condition Code: {data['weathercode']}"
    )

    # 3. LOAD: Save to SQLite
    conn = sqlite3.connect('weather_data.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS weather 
                   (timestamp TEXT, temp_c REAL, wind_speed REAL, description TEXT)''')
    cursor.execute("INSERT INTO weather VALUES (?, ?, ?, ?)", weather_entry)
    conn.commit()
    conn.close()
    print(f"Success! Record added: {weather_entry}")

except Exception as e:
    print(f"Pipeline Failed: {e}")
    exit(1)
