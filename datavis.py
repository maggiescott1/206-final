import sqlite3
import matplotlib.pyplot as plt
import numpy as np

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

# --- Visualization 1: Attendance vs. Temperature ---
plt.figure(figsize=(10, 5))
plt.scatter(temp_mean, attendance)
plt.title('Attendance vs. Mean Temperature')
plt.xlabel('Mean Temperature (°F)')
plt.ylabel('Attendance')
plt.grid(True)
plt.tight_layout()
plt.savefig('attendance_vs_temp.png')  
plt.show()

# --- Visualization 2: Attendance vs. Precipitation ---
plt.figure(figsize=(10, 5))
plt.scatter(precipitation, attendance)
plt.title('Attendance vs. Precipitation')
plt.xlabel('Precipitation (inches)')
plt.ylabel('Attendance')
plt.grid(True)
plt.tight_layout()
plt.savefig('attendance_vs_precip.png')  
plt.show()

# --- Visualization 3: Avg Attendance by Temp Range (Bar Chart) ---
# Group temp into bins
temp_bins = {
    'Cold (≤40°F)': [],
    'Cool (41–55°F)': [],
    'Mild (56–70°F)': [],
    'Warm (71–85°F)': [],
    'Hot (>85°F)': []
}

for att, temp in zip(attendance, temp_mean):
    if temp <= 40:
        temp_bins['Cold (≤40°F)'].append(att)
    elif temp <= 55:
        temp_bins['Cool (41–55°F)'].append(att)
    elif temp <= 70:
        temp_bins['Mild (56–70°F)'].append(att)
    elif temp <= 85:
        temp_bins['Warm (71–85°F)'].append(att)
    else:
        temp_bins['Hot (>85°F)'].append(att)

# Calculate averages [had chatgpt help me understand how to do this part]
categories = list(temp_bins.keys())
averages = [np.mean(temp_bins[cat]) if temp_bins[cat] else 0 for cat in categories]

# Plot bar chart
plt.figure(figsize=(10, 6))
plt.bar(categories, averages)
plt.title('Average Attendance by Temperature Range')
plt.xlabel('Temperature Range')
plt.ylabel('Average Attendance')
plt.xticks(rotation=30)
plt.tight_layout()
plt.savefig('avg_attendance_by_temp_range.png')
plt.show()

