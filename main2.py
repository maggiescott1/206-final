import sqlite3
import pandas as pd

# --- Sample concert JSON converted to DataFrame ---
concerts = [
    {'event_id': '7hzYWTVCd5nSyC4Z5x', 'event_date': '2025-03-30', 'event_name': 'Sarah and the Sundays',
     'location': '431 E Congress St, Detroit, MI 48226, USA', 'event_type': 'concerts', 'attendance': 853},
    {'event_id': 'CMXCsAz3Lmgmdsud5u', 'event_date': '2025-03-27', 'event_name': 'Nicotine Dolls',
     'location': '431 E Congress St, Detroit, MI 48226, USA', 'event_type': 'concerts', 'attendance': 843},
    {'event_id': 'EDSrLjXmFQkWspTzKP', 'event_date': '2025-03-19', 'event_name': 'The Main Squeeze with The Free Label',
     'location': '431 E Congress St, Detroit, MI 48226, USA', 'event_type': 'concerts', 'attendance': 919},
    {'event_id': 'GpTEDRe3AyfxYFQ23t', 'event_date': '2025-03-12', 'event_name': 'Last Dinosaurs with Tipling Rock',
     'location': '431 E Congress St, Detroit, MI 48226, USA', 'event_type': 'concerts', 'attendance': 919},
    {'event_id': 'J7wV3oYz4YkoSJRFHP', 'event_date': '2025-03-07', 'event_name': 'MacKenzy MacKay',
     'location': '431 E Congress St, Detroit, MI 48226, USA', 'event_type': 'concerts', 'attendance': 1033},
    {'event_id': 'A2FkwGXNUNDzN3yFKZ', 'event_date': '2025-02-23', 'event_name': 'Aborted with Ingested',
     'location': '431 E Congress St, Detroit, MI 48226, USA', 'event_type': 'concerts', 'attendance': 956},
    {'event_id': 'CqgkB8tENkBmAMKewW', 'event_date': '2025-02-22', 'event_name': 'Kxllswxtch with Sxmpra',
     'location': '431 E Congress St, Detroit, MI 48226, USA', 'event_type': 'concerts', 'attendance': 874},
    {'event_id': '5cWqMpxMMMchVw8R9z', 'event_date': '2025-02-01', 'event_name': 'CG5',
     'location': '431 E Congress St, Detroit, MI 48226, USA', 'event_type': 'concerts', 'attendance': 793},
    {'event_id': '67ks8Z7hrASBFr4zw4', 'event_date': '2025-03-31', 'event_name': 'Nettspend with Xaviersobased',
     'location': '431 East Congress Street\nDetroit, MI 48226\nUnited States of America', 'event_type': 'concerts', 'attendance': 1041},
]

concert_df = pd.DataFrame(concerts)

# --- Sample weather data (partial for demo purposes) ---
weather_data = [
    ("2025-03-30", 12.0, 61, 64.2, 48.3),
    ("2025-03-27", 0.0, 1, 56.5, 23.0),
    ("2025-03-19", 0.0, 1, 71.0, 55.7),
    ("2025-03-12", 0.0, 45, 46.0, 26.4),
    ("2025-03-07", 0.0, 1, 40.4, 25.9),
    ("2025-02-23", 0.0, 1, 34.9, 22.3),
    ("2025-02-22", 0.0, 45, 29.0, 18.3),
    ("2025-02-01", 0.0, 1, 29.4, 20.5),
    ("2025-03-31", 0.0, 1, 56.9, 39.7),
]

weather_df = pd.DataFrame(weather_data, columns=["event_date", "precipitation_hours", "weather_code", "temp_max", "temp_min"])

# --- Merge concerts with weather data ---
merged_df = pd.merge(concert_df, weather_df, on="event_date", how="inner")

# --- Create SQLite DB and insert combined table ---
conn = sqlite3.connect("combined_event_weather.db")
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS concert_weather')
cur.execute('''
    CREATE TABLE IF NOT EXISTS concert_weather (
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

merged_df.to_sql('concert_weather', conn, if_exists='append', index=False)

conn.commit()
conn.close()



