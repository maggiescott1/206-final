import sqlite3

# Sample list of your event data as (event_name, event_date, weather_description)
events_weather = [
    ("Sarah and the Sundays", "2025-03-30", "Rain: Slight, moderate and heavy intensity"),
    ("Nicotine Dolls", "2025-03-27", "Mainly clear, partly cloudy, and overcast"),
    ("The Main Squeeze with The Free Label", "2025-03-19", "Mainly clear, partly cloudy, and overcast"),
    ("Last Dinosaurs with Tipling Rock", "2025-03-12", "Fog and depositing rime fog"),
    ("MacKenzy MacKay", "2025-03-07", "Mainly clear, partly cloudy, and overcast"),
    ("Aborted with Ingested", "2025-02-23", "Mainly clear, partly cloudy, and overcast"),
    ("Kxllswxtch with Sxmpra", "2025-02-22", "Fog and depositing rime fog"),
    ("CG5", "2025-02-01", "Mainly clear, partly cloudy, and overcast"),
    ("Nettspend with Xaviersobased", "2025-03-31", "Mainly clear, partly cloudy, and overcast"),
    ("The War and Treaty", "2025-03-29", "Drizzle: Light, moderate, and dense intensity"),
    ("Tommy Richman (Moved from The Shelter)", "2025-03-28", "Drizzle: Light, moderate, and dense intensity"),
    ("The War And Treaty in Detroit", "2025-03-28", "Drizzle: Light, moderate, and dense intensity"),
    ("Whitechapel with Brand Of Sacrifice, 200 Stab Wounds, and Alluvial", "2025-03-26", "Mainly clear, partly cloudy, and overcast"),
    ("Poppy", "2025-03-25", "Mainly clear, partly cloudy, and overcast"),
    ("Poppy in Detroit", "2025-03-24", "Mainly clear, partly cloudy, and overcast"),
    ("Counterparts with Pain Of Truth", "2025-03-22", "Drizzle: Light, moderate, and dense intensity"),
    ("DJ Matt Bennett", "2025-03-21", "Mainly clear, partly cloudy, and overcast"),
    ("Paleface Swiss with Stick To Your Guns", "2025-03-20", "Drizzle: Light, moderate, and dense intensity"),
    ("Buckethead", "2025-03-17", "Mainly clear, partly cloudy, and overcast"),
    ("Armor For Sleep", "2025-03-14", "Mainly clear, partly cloudy, and overcast"),
    ("Stephen Wilson Jr.", "2025-03-13", "Mainly clear, partly cloudy, and overcast"),
    ("Jack Kays with Remo Drive", "2025-03-09", "Mainly clear, partly cloudy, and overcast"),
    ("Daniel Donato", "2025-03-09", "Mainly clear, partly cloudy, and overcast"),
    ("Intervals with Vola", "2025-03-07", "Mainly clear, partly cloudy, and overcast"),
    ("Ted Leo", "2025-03-02", "Mainly clear, partly cloudy, and overcast"),
]

# Create DB connection and cursor
conn = sqlite3.connect("event_weather.db")
cur = conn.cursor()

# Create table
cur.execute('''
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_name TEXT,
        event_date TEXT,
        weather_description TEXT
    )
''')

# Insert data
cur.executemany('''
    INSERT INTO events (event_name, event_date, weather_description)
    VALUES (?, ?, ?)
''', events_weather)

# Commit and close
conn.commit()
conn.close()
