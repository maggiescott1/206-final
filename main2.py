import requests
import sqlite3

## ai usage: chat helped create the original tables in the setup database function- that is why our weather table has the primary key autoincrement (creating the sqlite_sequence table in the db)
## chat helped me resolve the duplicate strings with the address_id table and foreign key

## SET UP DATABASE WITH TABLES
def setup_database(db_name):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    ## Events table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            event_date TEXT,
            event_name_id INTEGER,
            address_id INTEGER,
            attendance INTEGER,
            FOREIGN KEY (address_id) REFERENCES addresses(address_id)
        )
    ''')

    ## Weather table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            date TEXT PRIMARY KEY,
            temp_mean REAL,
            precipitation_sum REAL,
            apparent_temp_mean REAL
        )
    ''')

    ## address id table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS addresses (
            address_id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT UNIQUE
    )
    ''')

    ## event name id table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS event_names (
            event_name_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_name TEXT UNIQUE
    )
    ''')

    ## Combined table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS combined_events_weather (
            event_id TEXT PRIMARY KEY,
            event_date TEXT,
            event_name_id INTEGER,
            address_id INTEGER,
            attendance INTEGER,
            precipitation_sum REAL,
            temp_mean REAL,
            apparent_temp_mean REAL,
            FOREIGN KEY (address_id) REFERENCES addresses(address_id)
        )
    ''')

    conn.commit()
    return conn, cur

## events data from api
def fetch_events(limit=25, offset=0):
    response = requests.get(
        url="https://api.predicthq.com/v1/events",
        headers={"Authorization": "Bearer iA-z6ZEwp71oHlRYQf_-XcaeAB3BtJZAqJ94OR3k"},
        params={
            "limit": limit,
            "category": "concerts",
            "end.lte": "2025-04-16",
            "start.gte": "2025-01-01",
            "within": "5km@42.3297,-83.0425",
            "offset": offset
        }
    )
    events = response.json()['results']
    print(f"Fetched {len(events)} events with offset {offset}")

    event_data = []
    for event in events:
        event_data.append((
            event.get('id'),
            event.get('start', 'UNKNOWN').split('T')[0],
            event.get('title'),
            event.get('geo', {}).get('address', {}).get('formatted_address', 'UNKNOWN'),
            event.get('phq_attendance', 0)
        ))
    return event_data

## insert event data into the database
def insert_event_data(cur, events):
    new_events_added = 0
    # cur.execute('DELETE FROM events')
    for event in events:
        event_id, event_date, event_name, location, attendance = event

        ## select or get address_id
        cur.execute('SELECT address_id FROM addresses WHERE address = ?', (location,))
        result = cur.fetchone()
        # print(result)

        if result:
            address_id = result[0]
        else:
            cur.execute('INSERT INTO addresses (address) VALUES (?)', (location,))
            address_id = cur.lastrowid

        cur.execute('SELECT event_name_id FROM event_names WHERE event_name = ?', (event_name,))
        result = cur.fetchone()
        # print(result)

        if result:
            event_name_id = result[0]
        else:
            cur.execute('INSERT INTO event_names (event_name) VALUES (?)', (event_name,))
            event_name_id = cur.lastrowid

        # Insert event with address_id instead of location string
        cur.execute('''
            INSERT OR IGNORE INTO events (event_id, event_date, event_name_id, address_id, attendance)
            VALUES (?, ?, ?, ?, ?)
        ''', (event_id, event_date, event_name_id, address_id, attendance))

        if cur.rowcount > 0:
                new_events_added += 1
    return new_events_added

## weather data from api
def fetch_weather():
    url = "https://archive-api.open-meteo.com/v1/archive?latitude=42.3314&longitude=-83.0457&start_date=2025-01-01&end_date=2025-04-16&daily=temperature_2m_mean,precipitation_sum,apparent_temperature_mean&timezone=auto&temperature_unit=fahrenheit&wind_speed_unit=mph&precipitation_unit=inch&utm_source=chatgpt.com"

    response = requests.get(url)
    data = response.json()

    dates = data["daily"]["time"]
    temp_means = data["daily"]["temperature_2m_mean"]
    precip_sums = data["daily"]["precipitation_sum"]
    apparent_temp_means = data["daily"]["apparent_temperature_mean"]

    weather_data = []
    for i in range(len(dates)):
        weather_data.append((
            dates[i],
            temp_means[i],
            precip_sums[i],
            apparent_temp_means[i]
        ))
    
    # Sort weather_data in descending order by date
    weather_data.sort(key=lambda x: x[0], reverse=True)

    return weather_data

## insert weather data into the database
def insert_weather_data(cur, weather):
    for day in weather:
        cur.execute('''
            INSERT OR IGNORE INTO weather (date, temp_mean, precipitation_sum, apparent_temp_mean)
            VALUES (?, ?, ?, ?)
        ''', day)

## Combine event and weather data
def combine_data(cur):
    # cur.execute('DELETE FROM combined_events_weather')  # clear if exists

    cur.execute('''
        INSERT OR IGNORE INTO combined_events_weather
        SELECT 
            e.event_id, e.event_date, e.event_name_id, e.address_id, e.attendance,
            w.precipitation_sum, w.temp_mean, w.apparent_temp_mean
        FROM events e
                
        LEFT JOIN weather w ON DATE(e.event_date) = DATE(w.date)
    ''')

def main():
    db_name = 'combined_data.db'
    conn, cur = setup_database(db_name)

    ## count current events in event table
    cur.execute('SELECT COUNT(*) FROM events')
    num_curr_entries = cur.fetchone()[0]
    print(f"Current events in database: {num_curr_entries}")

    ## limit to 25 new entries at a time based on offset
    limit = 25
    offset = num_curr_entries

    new_events = fetch_events(limit=limit, offset=offset)

    ## If no new events are returned, stop
    if not new_events:
        print("No new events to fetch.")
    else:
        insert_event_data(cur, new_events)
        print(f"{len(new_events)} new events added.")
    
    ## count current events in table NOW
    cur.execute('SELECT COUNT(*) FROM events')
    total_events = cur.fetchone()[0]
    print(f"Total events in database: {total_events}")

    # Fetch and insert weather
    weather = fetch_weather()

    ##LIMITING WEATHER TABLE TO 25 PER RUN, SAME LOGIC AS EVENTS

    # Count how many weather records are already in the database
    cur.execute('SELECT COUNT(*) FROM weather')
    num_weather_entries = cur.fetchone()[0]
    print(f"Current weather records in database: {num_weather_entries}")

    # Limit to 25 new weather records per run - USING SLICING, NOT OFFSET
    limit = 25
    weather_to_add = weather[num_weather_entries:num_weather_entries+limit]

    # If there’s no new data left to add
    if not weather_to_add:
        print("No new weather data to fetch.")
    else:
        insert_weather_data(cur, weather_to_add)
        print(f"{len(weather_to_add)} new weather records added.")

    cur.execute('SELECT COUNT(*) FROM weather')
    total_weather = cur.fetchone()[0]
    print(f"Total weather records in database: {total_weather}")

    # Combine into one table
    combine_data(cur)
    print("Combined table created.")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()