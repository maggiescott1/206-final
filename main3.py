import requests
import sqlite3

# API URL
url = "https://archive-api.open-meteo.com/v1/archive?latitude=42.3314&longitude=-83.0457&start_date=2025-01-01&end_date=2025-04-16&daily=temperature_2m_mean,precipitation_sum,apparent_temperature_mean&timezone=auto&temperature_unit=fahrenheit&wind_speed_unit=mph&precipitation_unit=inch&utm_source=chatgpt.com"

# Fetch the data
response = requests.get(url)
if response.status_code != 200:
    print("Failed to fetch data:", response.status_code)
    print("Response:", response.text)
    exit()

data = response.json()

# Extract relevant values
dates = data["daily"]["time"]
temp_means = data["daily"]["temperature_2m_mean"]
precip_sums = data["daily"]["precipitation_sum"]
apparent_temp_means = data["daily"]["apparent_temperature_mean"]

# Connect to SQLite
conn = sqlite3.connect("combined_data1.db")
cur = conn.cursor()

# Create table
cur.execute("DROP TABLE IF EXISTS weather")
cur.execute('''
    CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        temp_mean REAL,
        precipitation_sum REAL,
        apparent_temp_mean REAL
    )
''')

# Insert data into table
for i in range(len(dates)):
    cur.execute('''
        INSERT INTO weather (
            date, temp_mean, precipitation_sum, apparent_temp_mean
        ) VALUES (?, ?, ?, ?)
    ''', (
        dates[i],
        temp_means[i],
        precip_sums[i],
        apparent_temp_means[i]
    ))

# Commit and close
conn.commit()
conn.close()

print("Weather data successfully inserted into combined_data1.db")
