import requests
import sqlite3
from datetime import datetime

# 1. EXTRACT: Fetch current weather for London (No API key needed for wttr.in)
response = requests.get("https://wttr.in")
data = response.json()['current_condition'][0]

# 2. TRANSFORM: Clean the data
weather_entry = (
    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    float(data['temp_C']),
    int(data['humidity']),
    data['weatherDesc'][0]['value']
)

# 3. LOAD: Save to a local SQLite database
conn = sqlite3.connect('weather_data.db')
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS weather 
               (timestamp TEXT, temp_c REAL, humidity INTEGER, description TEXT)''')
cursor.execute("INSERT INTO weather VALUES (?, ?, ?, ?)", weather_entry)
conn.commit()
conn.close()
print(f"Data Logged: {weather_entry}")
