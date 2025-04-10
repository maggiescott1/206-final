import requests
import sqlite3
import json

# API URL
url = "https://api.open-meteo.com/v1/forecast?latitude=42.3314&longitude=-83.0457&daily=precipitation_hours,weather_code,temperature_2m_max,temperature_2m_min&timezone=America%2FNew_York&temperature_unit=fahrenheit&precipitation_unit=inch&start_date=2025-02-01&end_date=2025-03-31"

response = requests.get(url)
data = response.json()

# Corrected keys based on the new structure
dates = data["daily"]["time"]
precip_hours = data["daily"]["precipitation_hours"]
weather_code = data["daily"]["weather_code"]
temp_max = data["daily"]["temperature_2m_max"]
temp_min = data["daily"]["temperature_2m_min"]

# SQLite setup
conn = sqlite3.connect("weather_events.db")
cur = conn.cursor()
cur.execute('DROP TABLE IF EXISTS weather')
cur.execute("""
    CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY,
        date TEXT,
        precipitation_hours REAL,
        weather_code INTEGER,
        temp_max REAL,
        temp_min REAL
    )
""")

for i in range(len(dates)):
    cur.execute("""
        INSERT INTO weather (date, precipitation_hours, weather_code, temp_max, temp_min)
        VALUES (?, ?, ?, ?, ?)
    """, (dates[i], precip_hours[i], weather_code[i], temp_max[i], temp_min[i]))

conn.commit()
conn.close()




