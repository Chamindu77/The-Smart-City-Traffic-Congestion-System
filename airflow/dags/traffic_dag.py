from datetime import datetime, timedelta
import logging
import os
import json

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# DAG default arguments
default_args = {
    "owner":            "smart_city",
    "depends_on_past":  False,
    "start_date":       datetime(2026, 1, 1),
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id="smart_city_traffic_nightly_report",
    default_args=default_args,
    description="Nightly traffic peak hour analysis and police deployment report",
    schedule_interval="0 0 * * *",   
    catchup=False,
    tags=["smart_city", "traffic", "batch"],
)

# Database connection config 
DB_CONFIG = {
    "host":     "postgres",
    "port":     5432,
    "database": "traffic_db",
    "user":     "traffic_user",
    "password": "traffic_pass",
}

REPORTS_DIR   = "/opt/airflow/reports"
CONGESTION_THRESHOLD = 8.0  



# TASK 1 — Aggregate peak hour per junction
def aggregate_peak_hour(**context):
    """
    Query traffic_events to find the busiest hour per junction.
    Also pulls critical alert counts from critical_traffic.
    Pushes results to XCom for the next task.
    """
    import psycopg2
    import psycopg2.extras

    log.info("=" * 60)
    log.info("TASK 1: Aggregating peak traffic hours from PostgreSQL")
    log.info("=" * 60)

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Query 1: Peak hour per junction
    peak_hour_sql = """
        SELECT
            sensor_id,
            EXTRACT(HOUR FROM event_timestamp) AS hour,
            SUM(vehicle_count)                 AS total_vehicles,
            AVG(avg_speed)                     AS avg_speed,
            COUNT(*)                           AS reading_count
        FROM traffic_events
        WHERE event_timestamp >= NOW() - INTERVAL '24 hours'
        GROUP BY sensor_id, EXTRACT(HOUR FROM event_timestamp)
        ORDER BY sensor_id, total_vehicles DESC
    """
    cursor.execute(peak_hour_sql)
    all_hourly = cursor.fetchall()

    # Query 2: Critical alert count per junction
    alert_sql = """
        SELECT sensor_id, COUNT(*) as alert_count
        FROM critical_traffic
        WHERE alerted_at >= NOW() - INTERVAL '24 hours'
        GROUP BY sensor_id
    """
    cursor.execute(alert_sql)
    alert_counts = {row["sensor_id"]: row["alert_count"]
                    for row in cursor.fetchall()}

    # Query 3: Average congestion index per junction
    congestion_sql = """
        SELECT sensor_id, AVG(congestion_idx) as avg_congestion
        FROM congestion_index
        WHERE window_start >= NOW() - INTERVAL '24 hours'
        GROUP BY sensor_id
    """
    cursor.execute(congestion_sql)
    congestion_avgs = {row["sensor_id"]: float(row["avg_congestion"])
                       for row in cursor.fetchall()}

    cursor.close()
    conn.close()

    # Find peak hour per junction 
    peak_per_junction = {}
    for row in all_hourly:
        sid = row["sensor_id"]
        if sid not in peak_per_junction:
            # First row per junction = highest vehicle count 
            peak_per_junction[sid] = {
                "sensor_id":      sid,
                "peak_hour":      int(row["hour"]),
                "peak_vehicles":  int(row["total_vehicles"]),
                "avg_speed":      float(row["avg_speed"]),
                "alert_count":    alert_counts.get(sid, 0),
                "avg_congestion": congestion_avgs.get(sid, 0.0),
            }

    # Determine if police needed 
    results = []
    for sid, data in peak_per_junction.items():
        needs_police = (
            data["avg_congestion"] > CONGESTION_THRESHOLD or
            data["alert_count"] >= 2 or
            data["avg_speed"] < 15.0
        )
        data["needs_police"] = needs_police
        results.append(data)

        status = "POLICE NEEDED" if needs_police else "✅ Normal"
        log.info(
            "Junction %-25s | Peak Hour: %02d:00 | "
            "Vehicles: %4d | Alerts: %d | %s",
            sid, data["peak_hour"], data["peak_vehicles"],
            data["alert_count"], status
        )

    log.info("TASK 1 complete — %d junctions analysed", len(results))

    
    context["ti"].xcom_push(key="peak_results", value=results)
    return results


# TASK 2 — Generate the nightly report 
def generate_report(**context):
    """
    Pull peak results from XCom.
    Generate a CSV report + text summary file.
    Save to /opt/airflow/reports/
    """
    import csv
    import psycopg2
    import psycopg2.extras

    log.info("=" * 60)
    log.info("TASK 2: Generating nightly traffic report")
    log.info("=" * 60)

    results = context["ti"].xcom_pull(
        key="peak_results", task_ids="aggregate_peak_hour"
    )

    if not results:
        log.warning("No peak results found — skipping report generation")
        return

    os.makedirs(REPORTS_DIR, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    report_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Get hourly traffic data for the full table in report
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    hourly_sql = """
        SELECT
            sensor_id,
            EXTRACT(HOUR FROM event_timestamp) AS hour,
            SUM(vehicle_count)                 AS total_vehicles,
            ROUND(AVG(avg_speed)::numeric, 1)  AS avg_speed
        FROM traffic_events
        WHERE event_timestamp >= NOW() - INTERVAL '24 hours'
        GROUP BY sensor_id, EXTRACT(HOUR FROM event_timestamp)
        ORDER BY sensor_id, hour
    """
    cursor.execute(hourly_sql)
    hourly_data = cursor.fetchall()
    cursor.close()
    conn.close()

    # Write CSV report 
    csv_path = os.path.join(REPORTS_DIR, f"traffic_report_{today}.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "sensor_id", "hour", "total_vehicles",
            "avg_speed_kmh", "report_date"
        ])
        for row in hourly_data:
            writer.writerow([
                row["sensor_id"],
                f"{int(row['hour']):02d}:00",
                row["total_vehicles"],
                row["avg_speed"],
                today
            ])
    log.info("CSV report saved: %s", csv_path)

    # Write text summary 
    txt_path = os.path.join(REPORTS_DIR, f"police_deployment_{today}.txt")
    with open(txt_path, "w") as f:
        f.write("=" * 60 + "\n")
        f.write("  SMART CITY COLOMBO — NIGHTLY TRAFFIC REPORT\n")
        f.write(f"  Generated: {report_date}\n")
        f.write("=" * 60 + "\n\n")

        f.write("PEAK TRAFFIC SUMMARY\n")
        f.write("-" * 60 + "\n")
        for r in sorted(results, key=lambda x: x["peak_vehicles"],
                        reverse=True):
            f.write(
                f"Junction  : {r['sensor_id']}\n"
                f"Peak Hour : {r['peak_hour']:02d}:00 - "
                f"{r['peak_hour']+1:02d}:00\n"
                f"Vehicles  : {r['peak_vehicles']}\n"
                f"Avg Speed : {r['avg_speed']:.1f} km/h\n"
                f"Alerts    : {r['alert_count']} critical alerts\n"
                f"Status    : "
                f"{'POLICE DEPLOYMENT RECOMMENDED' if r['needs_police'] else 'No intervention needed'}\n"
            )
            f.write("-" * 60 + "\n")

        # Police deployment list
        police_needed = [r for r in results if r["needs_police"]]
        f.write("\nPOLICE DEPLOYMENT PLAN FOR TOMORROW\n")
        f.write("=" * 60 + "\n")
        if police_needed:
            for r in police_needed:
                f.write(
                    f"  → Deploy to {r['sensor_id']} "
                    f"by {r['peak_hour']:02d}:00\n"
                )
        else:
            f.write("  → No police deployment needed tomorrow\n")
        f.write("\n" + "=" * 60 + "\n")

    log.info("Police deployment report saved: %s", txt_path)

    log.info("\n" + open(txt_path).read())

    context["ti"].xcom_push(key="csv_path", value=csv_path)
    context["ti"].xcom_push(key="txt_path", value=txt_path)
    context["ti"].xcom_push(key="police_needed",
                            value=[r["sensor_id"] for r in police_needed])


# TASK 3 — Save results to daily_peak_report table

def save_to_database(**context):
    """
    Save peak hour results to daily_peak_report table.
    This builds a historical record for trend analysis.
    """
    import psycopg2

    log.info("=" * 60)
    log.info("TASK 3: Saving report to daily_peak_report table")
    log.info("=" * 60)

    results = context["ti"].xcom_pull(
        key="peak_results", task_ids="aggregate_peak_hour"
    )

    if not results:
        log.warning("No results to save")
        return

    conn   = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    today = datetime.now().date()

    for r in results:
        cursor.execute("""
            INSERT INTO daily_peak_report
                (report_date, sensor_id, peak_hour, peak_vehicles,
                 avg_congestion, needs_police)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            today,
            r["sensor_id"],
            r["peak_hour"],
            r["peak_vehicles"],
            r["avg_congestion"],
            r["needs_police"],
        ))

    conn.commit()
    cursor.close()
    conn.close()

    log.info("TASK 3 complete — saved %d records to daily_peak_report",
             len(results))



# Define tasks and pipeline order

task1_aggregate = PythonOperator(
    task_id="aggregate_peak_hour",
    python_callable=aggregate_peak_hour,
    dag=dag,
)

task2_report = PythonOperator(
    task_id="generate_report",
    python_callable=generate_report,
    dag=dag,
)

task3_save = PythonOperator(
    task_id="save_to_database",
    python_callable=save_to_database,
    dag=dag,
)

# Pipeline order: Task1 → Task2 → Task3
task1_aggregate >> task2_report >> task3_save