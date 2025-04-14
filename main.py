import requests
import sqlite3
import json
import pandas as pd

# --- STEP 1: Setup shared database ---
DB_NAME = "weather_events.db"
conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

# --- STEP 2: Create weather table and insert data ---
weather_url = "https://api.open-meteo.com/v1/forecast?latitude=42.3314&longitude=-83.0457&daily=precipitation_hours,weather_code,temperature_2m_max,temperature_2m_min&timezone=America%2FNew_York&temperature_unit=fahrenheit&precipitation_unit=inch&start_date=2025-02-01&end_date=2025-03-31"

weather_response = requests.get(weather_url)
weather_data = weather_response.json()

dates = weather_data["daily"]["time"]
precip_hours = weather_data["daily"]["precipitation_hours"]
weather_codes = weather_data["daily"]["weather_code"]
temp_max = weather_data["daily"]["temperature_2m_max"]
temp_min = weather_data["daily"]["temperature_2m_min"]

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
    """, (dates[i], precip_hours[i], weather_codes[i], temp_max[i], temp_min[i]))

# --- STEP 3: Create events table and insert data ---
cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        event_date TEXT,
        event_name TEXT,
        location TEXT,
        event_type TEXT,
        attendance INTEGER
    )
""")

def fetch_events(limit=25):
    response = requests.get(
        url="https://api.predicthq.com/v1/events",
        headers={
            "Authorization": "Bearer 2ouGKn-PHtl_8cCL6NkXCXORqU2sVgVodDMUMAfs",
            "Accept": "application/json"
        },
        params={
            "limit": limit,
            "location_around.origin": "42.3297,-83.0425",
            "category": "concerts",
            "end.lte": "2025-03-31",
            "start.gte": "2025-02-01"
        }
    )
    data = response.json()
    events = data['results']
    valid_events = []
    for event in events:
        event_id = event.get('id')
        date = event.get('start', 'UNKNOWN').split('T')[0]
        title = event.get('title')
        category = event.get('category')
        location = event.get('geo', {}).get('address', {}).get('formatted_address', 'UNKNOWN')
        predicted_attendance = event.get('phq_attendance', 0)
        valid_events.append({
            'event_id': event_id,
            'event_date': date,
            'event_name': title,
            'location': location,
            'event_type': category,
            'attendance': predicted_attendance,
        })
    return valid_events

events = fetch_events()
for event in events:
    cur.execute('''
        INSERT OR IGNORE INTO events (event_id, event_date, event_name, location, event_type, attendance)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        event['event_id'],
        event['event_date'],
        event['event_name'],
        event['location'],
        event['event_type'],
        event['attendance']
    ))
# --- STEP 4: Create unified event_weather table ---
cur.execute('DROP TABLE IF EXISTS event_weather')
cur.execute('''
    CREATE TABLE event_weather (
        event_id TEXT PRIMARY KEY,
        event_date TEXT,
        event_name TEXT,
        location TEXT,
        event_type TEXT,
        attendance INTEGER,
        precipitation_hours REAL,
        weather_code INTEGER,
        temp_max REAL,
        temp_min REAL
    )
''')

# --- STEP 5: Join events and weather tables, insert into event_weather ---
for event in events:
    # Get matching weather data for the event date
    cur.execute('''
        SELECT precipitation_hours, weather_code, temp_max, temp_min 
        FROM weather 
        WHERE date = ?
    ''', (event['event_date'],))
    
    weather_data = cur.fetchone()

    if weather_data:
        precip_hours, weather_code, temp_max, temp_min = weather_data
    else:
        # If no matching weather data, set as None
        precip_hours, weather_code, temp_max, temp_min = (None, None, None, None)

    # Insert into unified table
    cur.execute('''
        INSERT OR REPLACE INTO event_weather 
        (event_id, event_date, event_name, location, event_type, attendance, precipitation_hours, weather_code, temp_max, temp_min)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        event['event_id'],
        event['event_date'],
        event['event_name'],
        event['location'],
        event['event_type'],
        event['attendance'],
        precip_hours,
        weather_code,
        temp_max,
        temp_min
    ))

conn.commit()