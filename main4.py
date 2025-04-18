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

    # Weather table (simplified, no weather_code)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            precipitation_sum REAL,
            temp_mean REAL,
            apparent_temp_mean REAL
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
            precipitation_sum REAL,
            temp_mean REAL,
            apparent_temp_mean REAL
        )
    ''')

    conn.commit()
    return conn, cur

# ----- Fetch events data -----
def fetch_events(limit=25, offset=0):
    response = requests.get(
        url="https://api.predicthq.com/v1/events",
        headers={"Authorization": "Bearer 2ouGKn-PHtl_8cCL6NkXCXORqU2sVgVodDMUMAfs"},
        params={
            "limit": limit,
            "location_around.origin": "42.3297,-83.0425",
            "location_around.offset": "50km",
            "category": "concerts",
            "end.lte": "2025-04-16",
            "start.gte": "2025-01-01",
            "offset": offset
        }
    )

    try:
        data = response.json()
    except Exception as e:
        print("Error decoding JSON:", e)
        print("Raw response text:", response.text)
        return []

    if 'results' not in data:
        print("Unexpected response structure:")
        print(data)
        return []

    events = data['results']
    print(f"Fetched {len(events)} events with offset {offset}")

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
    new_events_added = 0
    for event in events:
        cur.execute('''
            INSERT OR IGNORE INTO events (event_id, event_date, event_name, location, event_type, attendance)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', event)
        if cur.rowcount > 0:
            new_events_added += 1
    return new_events_added

# ----- Fetch weather data from Open-Meteo -----
def fetch_weather():
    url = "https://archive-api.open-meteo.com/v1/archive?latitude=42.3314&longitude=-83.0457&start_date=2025-01-01&end_date=2025-04-16&daily=temperature_2m_mean,precipitation_sum,apparent_temperature_mean&timezone=auto&temperature_unit=fahrenheit&wind_speed_unit=mph&precipitation_unit=inch&utm_source=chatgpt.com"
    response = requests.get(url)
    
    if response.status_code != 200:
        print("Failed to fetch weather data:", response.status_code)
        print(response.text)
        return []

    data = response.json()["daily"]

    weather_data = []
    for i in range(len(data["time"])):
        weather_data.append((
            data["time"][i],
            data["precipitation_sum"][i],
            data["temperature_2m_mean"][i],
            data["apparent_temperature_mean"][i]
        ))
    return weather_data

# ----- Insert weather data -----
def insert_weather_data(cur, weather):
    for day in weather:
        cur.execute('''
            INSERT OR IGNORE INTO weather (date, precipitation_sum, temp_mean, apparent_temp_mean)
            VALUES (?, ?, ?, ?)
        ''', day)

# ----- Combine event and weather data -----
def combine_data(cur):
    cur.execute('''
        INSERT OR IGNORE INTO combined_events_weather
        SELECT 
            e.event_id, e.event_date, e.event_name, e.location, e.event_type, e.attendance,
            w.precipitation_sum, w.temp_mean, w.apparent_temp_mean
        FROM events e
        LEFT JOIN weather w ON e.event_date = w.date
    ''')

# ----- Main workflow -----
def main():
    db_name = 'final.db'  # ✅ updated to "final"
    conn, cur = setup_database(db_name)

    # Check how many events are already in the database
    cur.execute('SELECT COUNT(*) FROM events')
    num_curr_entries = cur.fetchone()[0]
    print(f"Current events in database: {num_curr_entries}")

    limit = 25
    offset = num_curr_entries

    new_events = fetch_events(limit=limit, offset=offset)

    if not new_events:
        print("No new events to fetch.")
    else:
        inserted = insert_event_data(cur, new_events)
        print(f"{inserted} new events added.")
    
    cur.execute('SELECT COUNT(*) FROM events')
    total_events = cur.fetchone()[0]
    print(f"Total events in database: {total_events}")

    # Fetch weather
    weather = fetch_weather()
    insert_weather_data(cur, weather)
    print(f"{len(weather)} weather records inserted from Open-Meteo API.")

    # Combine data
    combine_data(cur)
    print("Combined table created.")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
