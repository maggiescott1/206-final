import sqlite3

def combine_databases(events_db, weather_db, combined_db):
    # Connect to the existing databases
    events_conn = sqlite3.connect(events_db)
    weather_conn = sqlite3.connect(weather_db)
    combined_conn = sqlite3.connect(combined_db)

    events_cur = events_conn.cursor()
    weather_cur = weather_conn.cursor()
    combined_cur = combined_conn.cursor()

    # Create combined event_weather table
    combined_cur.execute('''
        CREATE TABLE IF NOT EXISTS event_weather (
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

    # Join events and weather on date, select matching records
    events_cur.execute('''
        SELECT e.event_id, e.event_date, e.event_name, e.location, e.event_type, e.attendance,
               w.precipitation_hours, w.weather_code, w.temp_max, w.temp_min
        FROM events e
        JOIN weather w ON e.event_date = w.date
    ''')

    joined_data = events_cur.fetchall()

    # Insert joined data into the combined table
    for row in joined_data:
        combined_cur.execute('''
            INSERT OR IGNORE INTO event_weather
            (event_id, event_date, event_name, location, event_type, attendance,
             precipitation_hours, weather_code, temp_max, temp_min)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', row)

    combined_conn.commit()

    # Print how many rows were added
    print(f"{len(joined_data)} combined event-weather rows added to {combined_db}.")

    # Close connections
    events_conn.close()
    weather_conn.close()
    combined_conn.close()

def main():
    events_db = 'events_data.db'
    weather_db = 'weather_events.db'
    combined_db = 'combined_event_weather.db'

    combine_databases(events_db, weather_db, combined_db)

if __name__ == "__main__":
    main()