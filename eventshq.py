import requests
import sqlite3

def setup_database(db_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            event_date TEXT,
            event_name TEXT,
            location TEXT,
            event_type TEXT,  
            attendance INTEGER
        )
    ''')
    conn.commit()
    return conn, c

def fetch_events(limit=25):
    ##json request format from api website
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
    print(valid_events)
    return valid_events

def insert_event_data(c, valid_events):
    for event in valid_events:
        c.execute('''
        INSERT OR IGNORE INTO events (event_id, event_date, event_name, location, event_type, attendance)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            event['event_id'],
            event['event_date'],
            event['event_name'],
            event['location'],
            event['event_type'],
            event['attendance']
            )
        )

def main():
    # db_name = 'events_data.db'
    db_name = 'combined_data.db'
    conn, c = setup_database(db_name)
    events = fetch_events(limit=25)
    insert_event_data(c, events)
    conn.commit()
    conn.close()
    print(f"{len(events)} PredictHQ events added to the database.")

if __name__ == "__main__":
    main()

