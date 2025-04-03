import requests
import sqlite3
import json

# API URL
url = "https://archive-api.open-meteo.com/v1/archive?latitude=42.3314&longitude=-83.0457&start_date=2024-05-16&end_date=2024-09-03&daily=precipitation_sum,temperature_2m_max,temperature_2m_min&timezone=auto&temperature_unit=fahrenheit&precipitation_unit=inch"


response = requests.get(url)
data = response.json()


dates = data["daily"]["time"]
precip = data["daily"]["precipitation_sum"]
temp_max = data["daily"]["temperature_2m_max"]
temp_min = data["daily"]["temperature_2m_min"]


conn = sqlite3.connect("weather_events.db")
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY,
        date TEXT,
        precipitation REAL,
        temp_max REAL,
        temp_min REAL
    )
""")


for i in range(len(dates)):
    cur.execute("""
        INSERT INTO weather (date, precipitation, temp_max, temp_min)
        VALUES (?, ?, ?, ?)
    """, (dates[i], precip[i], temp_max[i], temp_min[i]))


conn.commit()
conn.close()


