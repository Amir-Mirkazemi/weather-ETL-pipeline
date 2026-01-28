import requests
import sqlite3
from datetime import datetime

# 1. EXTRACT: Use http and add a User-Agent to prevent blocking
headers = {'User-Agent': 'Mozilla/5.0'}
try:
    # Adding ?format=j1 ensures we get clean JSON
    response = requests.get("http://wttr.in", headers=headers, timeout=15)
    response.raise_for_status()
    data = response.json()['current_condition'][0]

    # 2. TRANSFORM
    weather_entry = (
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        float(data['temp_C']),
        int(data['humidity']),
        data['weatherDesc'][0]['value']
    )

    # 3. LOAD
    conn = sqlite3.connect('weather_data.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS weather 
                   (timestamp TEXT, temp_c REAL, humidity INTEGER, description TEXT)''')
    cursor.execute("INSERT INTO weather VALUES (?, ?, ?, ?)", weather_entry)
    conn.commit()
    conn.close()
    print(f"Success! Data Logged: {weather_entry}")

except Exception as e:
    print(f"Pipeline Failed: {e}")
    exit(1) # Tell GitHub Actions it failed

