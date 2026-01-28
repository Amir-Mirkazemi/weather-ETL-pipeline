import requests
import sqlite3
from datetime import datetime

# 1. EXTRACT: Final verified Open-Meteo URL for London
# This URL uses the updated 'current' parameter structure
url = "https://api.open-meteo.com"

try:
    print(f"Fetching data from: {url}")
    response = requests.get(url, timeout=15)
    response.raise_for_status() # This will catch any 400 or 500 errors
    
    # The API returns data in a 'current' dictionary
    raw_data = response.json()
    if 'current' not in raw_data:
        raise ValueError("Invalid API response: 'current' key missing")
        
    current_stats = raw_data['current']

    # 2. TRANSFORM
    weather_entry = (
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        float(current_stats['temperature_2m']),
        int(current_stats['relative_humidity_2m']),
        "London"
    )

    # 3. LOAD
    conn = sqlite3.connect('weather_data.db')
    cursor = conn.cursor()
    # Ensure schema matches our clean data
    cursor.execute('''CREATE TABLE IF NOT EXISTS weather 
                   (timestamp TEXT, temp_c REAL, humidity INTEGER, city TEXT)''')
    cursor.execute("INSERT INTO weather VALUES (?, ?, ?, ?)", weather_entry)
    conn.commit()
    conn.close()
    
    print(f"✅ Success! Logged to DB: {weather_entry}")

except Exception as e:
    print(f"❌ Pipeline Failed: {e}")
    exit(1)

