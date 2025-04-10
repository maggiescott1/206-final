import requests
import sqlite3
import pandas as pd  # <- you need this for pd.isna()

# --- STEP 1: Pull weather data from API ---
url = "https://api.open-meteo.com/v1/forecast?latitude=42.3314&longitude=-83.0457&daily=precipitation_hours,weather_code,temperature_2m_max,temperature_2m_min&timezone=America%2FNew_York&temperature_unit=fahrenheit&precipitation_unit=inch&start_date=2025-02-01&end_date=2025-03-31"

response = requests.get(url)
data = response.json()

dates = data["daily"]["time"]
precip_hours = data["daily"]["precipitation_hours"]
weather_codes = data["daily"]["weather_code"]
temp_max = data["daily"]["temperature_2m_max"]
temp_min = data["daily"]["temperature_2m_min"]

# --- STEP 2: Create and fill the weather table ---
conn = sqlite3.connect("weather_events.db")
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS weather')
cur.execute("""
    CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY,
        date TEXT,
        precipitation_hours REAL,
        weather_code INTEGER,
        temp_max REAL,
        temp_min REAL
    )
""")

for i in range(len(dates)):
    cur.execute("""
        INSERT INTO weather (date, precipitation_hours, weather_code, temp_max, temp_min)
        VALUES (?, ?, ?, ?, ?)
    """, (dates[i], precip_hours[i], weather_codes[i], temp_max[i], temp_min[i]))

conn.commit()

# --- STEP 3: Load the weather table as a DataFrame ---
df = pd.read_sql_query("SELECT * FROM weather", conn)

# --- STEP 4: Interpret weather codes ---
def interpret_weather_code(code):
    if pd.isna(code):
        return "Unknown weather code"
    code = int(code)
    if code == 0:
        return "Clear sky"
    elif code in [1, 2, 3]:
        return "Mainly clear, partly cloudy, and overcast"
    elif code in [45, 48]:
        return "Fog and depositing rime fog"
    elif code in [51, 53, 55]:
        return "Drizzle: Light, moderate, and dense intensity"
    elif code in [56, 57]:
        return "Freezing Drizzle: Light and dense intensity"
    elif code in [61, 63, 65]:
        return "Rain: Slight, moderate and heavy intensity"
    elif code in [66, 67]:
        return "Freezing Rain: Light and heavy intensity"
    elif code in [71, 73, 75]:
        return "Snow fall: Slight, moderate, and heavy intensity"
    elif code == 77:
        return "Snow grains"
    elif code in [80, 81, 82]:
        return "Rain showers: Slight, moderate, and violent"
    elif code in [85, 86]:
        return "Snow showers slight and heavy"
    elif code == 95:
        return "Thunderstorm: Slight or moderate"
    elif code in [96, 99]:
        return "Thunderstorm with slight and heavy hail"
    else:
        return "Unknown weather code"

# --- STEP 5: Print interpreted weather descriptions ---
for _, row in df.iterrows():
    description = interpret_weather_code(row["weather_code"])
    print(f"On {row['date']} the weather was: {description}")

conn.close()





