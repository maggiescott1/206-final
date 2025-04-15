import sqlite3

def combine_databases(events_db, weather_db, combined_db):
    # Connect to both source databases
    events_conn = sqlite3.connect(events_db)
    events_cur = events_conn.cursor()

    # Attach the weather database to this connection
    events_cur.execute(f"ATTACH DATABASE '{weather_db}' AS weather_db")

    # Connect to the new combined database
    combined_conn = sqlite3.connect(combined_db)
    combined_cur = combined_conn.cursor()

    # Create the combined table
    combined_cur.execute('''
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

    # Join events with weather based on event_date = weather.date
    events_cur.execute('''
        SELECT e.event_id, e.event_date, e.event_name, e.location, e.event_type, e.attendance,
               w.precipitation_hours, w.weather_code, w.temp_max, w.temp_min
        FROM events e
        LEFT JOIN weather_db.weather w ON e.event_date = w.date
    ''')

    combined_data = events_cur.fetchall()

    for row in combined_data:
        combined_cur.execute('''
            INSERT OR REPLACE INTO combined_events_weather 
            (event_id, event_date, event_name, location, event_type, attendance, 
             precipitation_hours, weather_code, temp_max, temp_min)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', row)

    combined_conn.commit()

    print(f"{len(combined_data)} records combined successfully!")

    # Close connections
    events_conn.close()
    combined_conn.close()

def main():
    events_db = 'events_data.db'
    weather_db = 'weather.db'
    combined_db = 'combined_data.db'

    combine_databases(events_db, weather_db, combined_db)

if __name__ == "__main__":
    main()