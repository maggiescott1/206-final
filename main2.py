import requests
import sqlite3

# ----- Setup database -----
def setup_database(db_name):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # Events table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            event_date TEXT,
            event_name TEXT,
            location TEXT,
            event_type TEXT,
            attendance INTEGER
        )
    ''')

    # Weather table
    cur.execute('''DROP TABLE IF EXISTS weather''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            precipitation_hours REAL,
            weather_code INTEGER,
            temp_max REAL,
            temp_min REAL
        )
    ''')

    # Combined table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS combined_events_weather (
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

    conn.commit()
    return conn, cur

# ----- Fetch events data -----
def fetch_events(limit=25):
    response = requests.get(
        url="https://api.predicthq.com/v1/events",
        headers={"Authorization": "Bearer 2ouGKn-PHtl_8cCL6NkXCXORqU2sVgVodDMUMAfs"},
        params={
            "limit": limit,
            "location_around.origin": "42.3297,-83.0425",
            "location_around.offset": "50km",
            "category": "concerts",
            "end.lte": "2025-04-16",
            "start.gte": "2025-01-01"
        }
    )
    events = response.json()['results']

    event_data = []
    for event in events:
        event_data.append((
            event.get('id'),
            event.get('start', 'UNKNOWN').split('T')[0],
            event.get('title'),
            event.get('geo', {}).get('address', {}).get('formatted_address', 'UNKNOWN'),
            event.get('category'),
            event.get('phq_attendance', 0)
        ))
    return event_data

# ----- Insert event data -----
def insert_event_data(cur, events):
    for event in events:
        cur.execute('''
            INSERT OR IGNORE INTO events (event_id, event_date, event_name, location, event_type, attendance)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', event)

# ----- Fetch weather data -----
def fetch_weather():
    url = "https://api.open-meteo.com/v1/forecast?latitude=42.3314&longitude=-83.0457&daily=precipitation_hours,weather_code,temperature_2m_max,temperature_2m_min&timezone=America%2FNew_York&temperature_unit=fahrenheit&precipitation_unit=inch&start_date=2025-02-01&end_date=2025-03-31"
    response = requests.get(url)
    data = response.json()["daily"]

    weather_data = []
    for i in range(len(data["time"])):
        weather_data.append((
            data["time"][i],
            data["precipitation_hours"][i],
            data["weather_code"][i],
            data["temperature_2m_max"][i],
            data["temperature_2m_min"][i]
        ))
    return weather_data

# ----- Insert weather data -----
def insert_weather_data(cur, weather):
    for day in weather:
        cur.execute('''
            INSERT OR IGNORE INTO weather (date, precipitation_hours, weather_code, temp_max, temp_min)
            VALUES (?, ?, ?, ?, ?)
        ''', day)

# ----- Combine event and weather data -----
def combine_data(cur):
    # cur.execute('DELETE FROM combined_events_weather')  # clear if exists

    cur.execute('''
        INSERT OR IGNORE INTO combined_events_weather
        SELECT 
            e.event_id, e.event_date, e.event_name, e.location, e.event_type, e.attendance,
            w.precipitation_hours, w.weather_code, w.temp_max, w.temp_min
        FROM events e
        LEFT JOIN weather w ON e.event_date = w.date
    ''')

# ----- Main workflow -----
def main():
    db_name = 'combined_data1.db'
    conn, cur = setup_database(db_name)

    # Fetch and insert events
    events = fetch_events(limit=25)
    insert_event_data(cur, events)
    print(f"{len(events)} events added.")

    # Fetch and insert weather
    weather = fetch_weather()
    insert_weather_data(cur, weather)
    print(f"{len(weather)} weather records added.")

    # Combine into one table
    combine_data(cur)
    print("Combined table created.")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
