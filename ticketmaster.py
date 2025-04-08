import requests
import sqlite3
import time

api_key = "0vtTNJs8OJf9dE1LNrdeWP9tz08nnsyQ"
base_url = "https://app.ticketmaster.com/discovery/v2/"

database = "ticketmaster.db"

def create_db():
    conn = sqlite3.connect(database)
    cur = conn.cursor()

    # Venues table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Venues (
            id TEXT PRIMARY KEY,
            name TEXT,
            city TEXT,
            state TEXT,
            country TEXT
        )
    ''')

    # Events table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Events (
            id TEXT PRIMARY KEY,
            name TEXT,
            date TEXT,
            time TEXT,
            attendance TEXT,
            venue_id TEXT,
            FOREIGN KEY (venue_id) REFERENCES Venues(id)
        )
    ''')

    conn.commit()
    conn.close()

def fetch_events(page=0):
    url = base_url + "events.json"
    params = {
        "apikey": api_key,
        "size": 25,
        "page": page
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        print(f"Failed to fetch events: {response.status_code}")
        print(response.text)
        return []

    data = response.json()
    events = data.get("_embedded", {}).get("events", [])
    print(events[0])
    return events

def save_data(events):
    conn = sqlite3.connect(database)
    cur = conn.cursor()

    for event in events:
        event_id = event.get("id")
        name = event.get("name")
        dates = event.get("dates", {}).get("start", {})
        date = dates.get("localDate")
        time = dates.get("localTime")
        attendance = event.get("info", "N/A")  # Use actual field if available

        venues = event.get("_embedded", {}).get("venues", [])
        if not venues:
            continue

        venue = venues[0]
        venue_id = venue.get("id")

        # Save venue
        cur.execute('''
            INSERT OR IGNORE INTO Venues (id, name, city, state, country)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            venue_id,
            venue.get("name"),
            venue.get("city", {}).get("name"),
            venue.get("state", {}).get("stateCode"),
            venue.get("country", {}).get("countryCode")
        ))

        # Save event
        cur.execute('''
            INSERT OR IGNORE INTO Events (id, name, date, time, attendance, venue_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            event_id, name, date, time, attendance, venue_id
        ))

    conn.commit()
    conn.close()

def main():
    create_db()

    # Count how many events are already stored
    conn = sqlite3.connect(database)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM Events")
    count = cur.fetchone()[0]
    conn.close()

    if count >= 100:
        print("Already have 100+ events.")
        return

    # Calculate which page to fetch next (25 events per page)
    next_page = count // 25
    print(f"Fetching page {next_page}...")
    events = fetch_events(next_page)
    save_data(events)
    print(f"Added {len(events)} events.")

if __name__ == "__main__":
    main()