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
    ('2025-02-01', 0.0, 3, 29.4, 20.5),
    ('2025-02-02', 2.0, 73, 31.6, 21.6),
    ('2025-02-03', 0.0, 45, 41.4, 31.6),
    ('2025-02-04', 0.0, 3, 32.9, 23.6),
    ('2025-02-05', 0.0, 3, 25.9, 18.6),
    ('2025-02-06', 5.0, 73, 33.7, 24.5),
    ('2025-02-07', 0.0, 3, 32.7, 16.2),
    ('2025-02-08', 4.0, 73, 30.7, 21.0),
    ('2025-02-09', 1.0, 71, 29.6, 17.8),
    ('2025-02-10', 0.0, 3, 30.2, 11.4),
    ('2025-02-11', 0.0, 3, 31.0, 19.4),
    ('2025-02-12', 6.0, 73, 27.7, 21.4),
    ('2025-02-13', 6.0, 73, 29.3, 19.4),
    ('2025-02-14', 0.0, 45, 22.5, 5.2),
    ('2025-02-15', 8.0, 73, 33.3, 20.6),
    ('2025-02-16', 11.0, 75, 30.5, 21.7),
    ('2025-02-17', 1.0, 71, 21.2, 9.0),
    ('2025-02-18', 0.0, 3, 17.3, 2.2),
    ('2025-02-19', 0.0, 3, 18.1, 2.8),
    ('2025-02-20', 1.0, 71, 22.3, 11.6),
    ('2025-02-21', 0.0, 3, 31.8, 18.4),
    ('2025-02-22', 0.0, 45, 29.0, 18.3),
    ('2025-02-23', 0.0, 3, 34.9, 22.3),
    ('2025-02-24', 0.0, 45, 49.2, 31.7),
    ('2025-02-25', 1.0, 53, 54.0, 36.8),
    ('2025-02-26', 6.0, 71, 41.2, 26.0),
    ('2025-02-27', 3.0, 51, 43.9, 34.6),
    ('2025-02-28', 0.0, 3, 54.3, 29.0),
    ('2025-03-01', 0.0, 3, 42.8, 22.4),
    ('2025-03-02', 0.0, 3, 29.6, 17.6),
    ('2025-03-03', 0.0, 3, 40.5, 20.2),
    ('2025-03-04', 6.0, 55, 48.9, 35.0),
    ('2025-03-05', 11.0, 61, 56.0, 37.0),
    ('2025-03-06', 0.0, 3, 38.9, 27.5),
    ('2025-03-07', 0.0, 3, 40.4, 25.9),
    ('2025-03-08', 0.0, 3, 38.2, 22.9),
    ('2025-03-09', 0.0, 3, 54.8, 31.8),
    ('2025-03-10', 0.0, 2, 64.4, 34.8),
    ('2025-03-11', 0.0, 45, 61.0, 31.5),
    ('2025-03-12', 0.0, 45, 46.0, 26.4),
    ('2025-03-13', 0.0, 1, 53.7, 24.9),
    ('2025-03-14', 0.0, 3, 72.9, 33.3),
    ('2025-03-15', 2.0, 53, 70.1, 55.3),
    ('2025-03-16', 5.0, 55, 58.4, 35.2),
    ('2025-03-17', 0.0, 3, 44.9, 24.6),
    ('2025-03-18', 0.0, 3, 66.5, 34.7),
    ('2025-03-19', 0.0, 3, 71.0, 55.7),
    ('2025-03-20', 5.0, 53, 60.7, 32.8),
    ('2025-03-21', 0.0, 3, 53.7, 27.2),
    ('2025-03-22', 2.0, 51, 49.0, 28.5),
    ('2025-03-23', 4.0, 73, 41.2, 20.5),
    ('2025-03-24', 0.0, 3, 43.6, 33.7),
    ('2025-03-25', 0.0, 3, 45.7, 27.7),
    ('2025-03-26', 0.0, 3, 44.3, 26.4),
    ('2025-03-27', 0.0, 3, 56.5, 23.0),
    ('2025-03-28', 2.0, 53, 61.1, 40.0),
    ('2025-03-29', 3.0, 55, 66.9, 58.3),
    ('2025-03-30', 12.0, 63, 64.2, 48.3),
    ('2025-03-31', 0.0, 3, 56.9, 39.7),
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



