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
conn.close()

# Data prep
attendance = [row[0] for row in rows]
temp_mean = [row[1] for row in rows]
precipitation = [row[2] for row in rows]

# --- Vis 1: Scatter plot showing no strong correlation (Attendance vs Temp) ---
plt.figure(figsize=(10, 6))
plt.scatter(temp_mean, attendance, alpha=0.4, edgecolor='gray', color='slateblue')

# OPTIONAL: comment this out to remove the trendline
# z = np.polyfit(temp_mean, attendance, 1)
# p = np.poly1d(z)
# plt.plot(temp_mean, p(temp_mean), linestyle='--', linewidth=2, color='gray')

plt.title('Attendance vs. Temperature', fontsize=14)
plt.xlabel('Mean Temperature (°F)', fontsize=12)
plt.ylabel('Attendance', fontsize=12)
plt.grid(True, linestyle=':', linewidth=0.7)
plt.tight_layout()
plt.savefig('attendance_vs_temp.png')
plt.show()

# --- Vis 2: Bubble plot (Precipitation vs Attendance) ---
plt.figure(figsize=(10, 6))
bubble_size = [p * 150 for p in precipitation]  # scale precipitation to size
plt.scatter(precipitation, attendance, s=bubble_size, alpha=0.5, c='teal', edgecolor='gray')

plt.title('Attendance vs. Precipitation (Bubble Size = Precip)', fontsize=14)
plt.xlabel('Precipitation (inches)', fontsize=12)
plt.ylabel('Attendance', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.savefig('bubble_attendance_vs_precip.png')
plt.show()

# --- Vis 3: Horizontal Bar (Average Attendance by Temp Range) ---
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

categories = list(temp_bins.keys())
averages = [np.mean(temp_bins[cat]) if temp_bins[cat] else 0 for cat in categories]

plt.figure(figsize=(10, 6))
bars = plt.barh(categories, averages, color='coral', edgecolor='black')
plt.title('Avg Attendance by Temperature Range', fontsize=14)
plt.xlabel('Average Attendance', fontsize=12)
plt.gca().invert_yaxis()  # highest temp range at bottom
for bar in bars:
    width = bar.get_width()
    plt.text(width + 50, bar.get_y() + bar.get_height()/2, f'{int(width)}', va='center')
plt.tight_layout()
plt.savefig('horizontal_bar_avg_attendance.png')
plt.show()


