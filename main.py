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

# --- STEP 4: Join tables and interpret weather codes ---
df = pd.read_sql_query('''
    SELECT 
        e.event_name, 
        e.event_date, 
        e.attendance, 
        w.temp_max, 
        w.temp_min, 
        w.precipitation_hours,
        w.weather_code
    FROM events e
    JOIN weather w ON e.event_date = w.date
''', conn)



def interpret_weather_code(code):
    if pd.isna(code):
        return "Unknown weather code"
    code = int(code)
    if code == 0:
        return "Clear sky"
    elif code in [1, 2, 3]:
        return "Mainly clear, partly cloudy, and overcast"
    elif code in [45, 48]:
        return "Fog and depositing rime fog"
    elif code in [51, 53, 55]:
        return "Drizzle: Light, moderate, and dense intensity"
    elif code in [56, 57]:
        return "Freezing Drizzle: Light and dense intensity"
    elif code in [61, 63, 65]:
        return "Rain: Slight, moderate and heavy intensity"
    elif code in [66, 67]:
        return "Freezing Rain: Light and heavy intensity"
    elif code in [71, 73, 75]:
        return "Snow fall: Slight, moderate, and heavy intensity"
    elif code == 77:
        return "Snow grains"
    elif code in [80, 81, 82]:
        return "Rain showers: Slight, moderate, and violent"
    elif code in [85, 86]:
        return "Snow showers slight and heavy"
    elif code == 95:
        return "Thunderstorm: Slight or moderate"
    elif code in [96, 99]:
        return "Thunderstorm with slight and heavy hail"
    else:
        return "Unknown weather code"

for _, row in df.iterrows():
    description = interpret_weather_code(row["weather_code"])
    print(f"{row['event_name']} on {row['event_date']} had weather: {description}")


conn.commit()
conn.close()

