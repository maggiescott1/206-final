import sqlite3
import matplotlib.pyplot as plt

# Connect to your database
conn = sqlite3.connect('combined_data1.db')
cur = conn.cursor()

# Pull relevant data
query = '''
    SELECT attendance, temp_mean, precipitation_sum
    FROM combined_events_weather
    WHERE attendance IS NOT NULL 
        AND temp_mean IS NOT NULL 
        AND precipitation_sum IS NOT NULL
'''
cur.execute(query)
rows = cur.fetchall()

# Close DB connection
conn.close()

# Split into x and y values
attendance = [row[0] for row in rows]
temp_mean = [row[1] for row in rows]
precipitation = [row[2] for row in rows]

# Attendance vs Temperature
plt.figure(figsize=(10, 5))
plt.scatter(temp_mean, attendance)
plt.title('Attendance vs. Mean Temperature')
plt.xlabel('Mean Temperature (°F)')
plt.ylabel('Attendance')
plt.grid(True)
plt.tight_layout()
plt.savefig('attendance_vs_temp.png')  # Optional: save to file
plt.show()

# Attendance vs Precipitation
plt.figure(figsize=(10, 5))
plt.scatter(precipitation, attendance)
plt.title('Attendance vs. Precipitation')
plt.xlabel('Precipitation (inches)')
plt.ylabel('Attendance')
plt.grid(True)
plt.tight_layout()
plt.savefig('attendance_vs_precip.png')  # Optional: save to file
plt.show()
