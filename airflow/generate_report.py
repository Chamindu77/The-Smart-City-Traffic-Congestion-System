import psycopg2, psycopg2.extras, csv, os
from datetime import datetime

DB = dict(host='postgres', port=5432, database='traffic_db', user='traffic_user', password='traffic_pass')
REPORTS_DIR = '/opt/airflow/reports'
os.makedirs(REPORTS_DIR, exist_ok=True)
today = datetime.now().strftime('%Y-%m-%d')

conn = psycopg2.connect(**DB)
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

cur.execute("""
    SELECT sensor_id,
           EXTRACT(HOUR FROM event_timestamp) AS hour,
           SUM(vehicle_count) AS total_vehicles,
           AVG(avg_speed) AS avg_speed
    FROM traffic_events
    WHERE event_timestamp >= NOW() - INTERVAL '24 hours'
    GROUP BY sensor_id, EXTRACT(HOUR FROM event_timestamp)
    ORDER BY sensor_id, total_vehicles DESC
""")
all_rows = cur.fetchall()

cur.execute("""SELECT sensor_id, COUNT(*) as cnt FROM critical_traffic
               WHERE alerted_at >= NOW() - INTERVAL '24 hours'
               GROUP BY sensor_id""")
alerts = {r['sensor_id']: r['cnt'] for r in cur.fetchall()}

cur.execute("""SELECT sensor_id, AVG(congestion_idx) as avg_idx
               FROM congestion_index GROUP BY sensor_id""")
congestion = {r['sensor_id']: float(r['avg_idx']) for r in cur.fetchall()}

peaks = {}
for row in all_rows:
    sid = row['sensor_id']
    if sid not in peaks:
        peaks[sid] = {
            'sensor_id':      sid,
            'peak_hour':      int(row['hour']),
            'peak_vehicles':  int(row['total_vehicles']),
            'avg_speed':      float(row['avg_speed']),
            'alert_count':    alerts.get(sid, 0),
            'avg_congestion': congestion.get(sid, 0.0),
            'needs_police':   True
        }

# Police deployment text report 
txt = os.path.join(REPORTS_DIR, f'police_deployment_{today}.txt')
with open(txt, 'w') as f:
    f.write('=' * 60 + '\n')
    f.write('  SMART CITY COLOMBO - NIGHTLY TRAFFIC REPORT\n')
    f.write(f'  Generated: {datetime.now()}\n')
    f.write('=' * 60 + '\n\n')
    f.write('PEAK TRAFFIC SUMMARY\n')
    f.write('-' * 60 + '\n')
    for r in sorted(peaks.values(), key=lambda x: x['peak_vehicles'], reverse=True):
        f.write(f"Junction  : {r['sensor_id']}\n")
        f.write(f"Peak Hour : {r['peak_hour']:02d}:00 - {r['peak_hour']+1:02d}:00\n")
        f.write(f"Vehicles  : {r['peak_vehicles']:,}\n")
        f.write(f"Avg Speed : {r['avg_speed']:.1f} km/h\n")
        f.write(f"Alerts    : {r['alert_count']} critical alerts\n")
        f.write(f"Status    : POLICE DEPLOYMENT RECOMMENDED\n")
        f.write('-' * 60 + '\n')
    f.write('\nPOLICE DEPLOYMENT PLAN FOR TOMORROW\n')
    f.write('=' * 60 + '\n')
    for r in sorted(peaks.values(), key=lambda x: x['peak_vehicles'], reverse=True):
        f.write(f"  -> Deploy to {r['sensor_id']} by {r['peak_hour']:02d}:00\n")
    f.write('\n' + '=' * 60 + '\n')

# Hourly CSV report 
cur.execute("""
    SELECT sensor_id,
           EXTRACT(HOUR FROM event_timestamp) AS hour,
           SUM(vehicle_count) AS total_vehicles,
           ROUND(AVG(avg_speed)::numeric, 1) AS avg_speed
    FROM traffic_events
    WHERE event_timestamp >= NOW() - INTERVAL '24 hours'
    GROUP BY sensor_id, EXTRACT(HOUR FROM event_timestamp)
    ORDER BY sensor_id, hour
""")
hourly = cur.fetchall()

csv_path = os.path.join(REPORTS_DIR, f'traffic_report_{today}.csv')
with open(csv_path, 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['sensor_id', 'hour', 'total_vehicles', 'avg_speed_kmh', 'report_date'])
    for row in hourly:
        w.writerow([row['sensor_id'], f"{int(row['hour']):02d}:00",
                    row['total_vehicles'], row['avg_speed'], today])

# Save to daily_peak_report table 
for r in peaks.values():
    cur.execute("""
        INSERT INTO daily_peak_report
            (report_date, sensor_id, peak_hour, peak_vehicles, avg_congestion, needs_police)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, (today, r['sensor_id'], r['peak_hour'],
          r['peak_vehicles'], r['avg_congestion'], True))
conn.commit()
cur.close()
conn.close()

print('=' * 60)
print('SUCCESS! Reports generated')
print('=' * 60)
print(f'  Text report : {txt}')
print(f'  CSV report  : {csv_path}')
print()
print('PEAK TRAFFIC RESULTS:')
for r in sorted(peaks.values(), key=lambda x: x['peak_vehicles'], reverse=True):
    print(f"  {r['sensor_id']:<25} | Peak: {r['peak_hour']:02d}:00 "
          f"| Vehicles: {r['peak_vehicles']:>6,} | Alerts: {r['alert_count']}")
print()
print('Saved to daily_peak_report table in PostgreSQL')