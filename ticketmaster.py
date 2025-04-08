import requests
import json
import sqlite3
import os
import re

base_url = 'https://app.ticketmaster.com/discovery/v2/'
api_key = '0vtTNJs8OJf9dE1LNrdeWP9tz08nnsyQ'
consumer_secret = '5ORFooY9s4fxVdeK'

database = "ticketmaster.db"

def create_db():
    conn = sqlite3.connect(database)
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS Venues (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            city TEXT,
            state TEXT,
            country TEXT
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS Events (
            id TEXT PRIMARY KEY,
            name TEXT,
            date TEXT,
            time TEXT,
            attendance TEXT,
            venue_id INTEGER,
            FOREIGN KEY (venue_id) REFERENCES Venues(id)
        )
    ''')

    conn.commit()
    conn.close()

def fetch_events(page=0):
    params = {
        'apikey': '0vtTNJs8OJf9dE1LNrdeWP9tz08nnsyQ',
        'size': 25,
        'page': page
    }
    response = requests.get(base_url, params=params)
    return response.json()

def insert_data(data):
    conn = sqlite3.connect(database)
    cur = conn.cursor()

    events = data.get('_embedded', {}).get('events', [])
    for event in events:
        event_id = event['id']
        name = event['name']
        date = event['dates']['start']['localDate']
        time = event['dates']['start'].get('localTime', None)
        attendance = event.get('info') or event.get('pleaseNote') or "N/A"

        venues = event.get('_embedded', {}).get('venues', [])
        if venues:
            venue = venues[0]
            venue_name = venue['name']
            city = venue.get('city', {}).get('name')
            state = venue.get('state', {}).get('name')
            country = venue.get('country', {}).get('name')

            # Insert venue
            cur.execute('''
                INSERT OR IGNORE INTO Venues (name, city, state, country)
                VALUES (?, ?, ?, ?)
            ''', (venue_name, city, state, country))

            cur.execute('SELECT id FROM Venues WHERE name = ?', (venue_name,))
            venue_id = cur.fetchone()[0]

            # Insert event
            cur.execute('''
                INSERT OR IGNORE INTO Events (id, name, date, time, attendance, venue_id)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (event_id, name, date, time, attendance, venue_id))

    conn.commit()
    conn.close()

def get_event_count():
    conn = sqlite3.connect(database)
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM Events')
    count = cur.fetchone()[0]
    conn.close()
    return count

def main():
    create_db()

    page = 0
    while get_event_count() < 100:
        data = fetch_events(page)
        insert_data(data)
        page += 1

if __name__ == "__main__":
    main()