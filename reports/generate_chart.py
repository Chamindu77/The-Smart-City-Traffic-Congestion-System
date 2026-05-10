import psycopg2
import psycopg2.extras
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from datetime import datetime
import os

# Database config 
DB = dict(
    host="localhost",
    port=5432,
    database="traffic_db",
    user="traffic_user",
    password="traffic_pass"
)

REPORTS_DIR = "reports"
os.makedirs(REPORTS_DIR, exist_ok=True)
today = datetime.now().strftime("%Y-%m-%d")

# Junction colors 
COLORS = {
    "Junction_Pettah":      "#E74C3C",
    "Junction_Kollupitiya": "#3498DB",
    "Junction_Nugegoda":    "#2ECC71",
    "Junction_Maharagama":  "#F39C12",
}

MARKERS = {
    "Junction_Pettah":      "o",
    "Junction_Kollupitiya": "s",
    "Junction_Nugegoda":    "^",
    "Junction_Maharagama":  "D",
}


def get_hourly_data(conn):
    """Get hourly vehicle counts per junction."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
        SELECT
            sensor_id,
            EXTRACT(HOUR FROM event_timestamp)::int AS hour,
            SUM(vehicle_count)                       AS total_vehicles,
            ROUND(AVG(avg_speed)::numeric, 1)        AS avg_speed,
            COUNT(*)                                 AS readings
        FROM traffic_events
        GROUP BY sensor_id, EXTRACT(HOUR FROM event_timestamp)::int
        ORDER BY sensor_id, hour
    """)
    rows = cur.fetchall()
    cur.close()

    data = {}
    for row in rows:
        sid = row["sensor_id"]
        if sid not in data:
            data[sid] = {"hours": [], "vehicles": [], "speeds": []}
        data[sid]["hours"].append(row["hour"])
        data[sid]["vehicles"].append(int(row["total_vehicles"]))
        data[sid]["speeds"].append(float(row["avg_speed"]))
    return data


def get_alert_hours(conn):
    """Get hours when critical alerts fired per junction."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
        SELECT sensor_id,
               EXTRACT(HOUR FROM alerted_at)::int AS hour,
               COUNT(*) AS alert_count
        FROM critical_traffic
        GROUP BY sensor_id, EXTRACT(HOUR FROM alerted_at)::int
        ORDER BY sensor_id, hour
    """)
    rows = cur.fetchall()
    cur.close()

    alerts = {}
    for row in rows:
        sid = row["sensor_id"]
        if sid not in alerts:
            alerts[sid] = {}
        alerts[sid][row["hour"]] = row["alert_count"]
    return alerts


def get_congestion_data(conn):
    """Get congestion index per junction per window."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
        SELECT sensor_id,
               EXTRACT(HOUR FROM window_start)::int AS hour,
               AVG(congestion_idx) AS avg_idx
        FROM congestion_index
        GROUP BY sensor_id, EXTRACT(HOUR FROM window_start)::int
        ORDER BY sensor_id, hour
    """)
    rows = cur.fetchall()
    cur.close()

    data = {}
    for row in rows:
        sid = row["sensor_id"]
        if sid not in data:
            data[sid] = {"hours": [], "idx": []}
        data[sid]["hours"].append(row["hour"])
        data[sid]["idx"].append(float(row["avg_idx"]))
    return data


def generate_charts():
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB)

    hourly   = get_hourly_data(conn)
    alerts   = get_alert_hours(conn)
    cong     = get_congestion_data(conn)
    conn.close()

    if not hourly:
        print("No data found in traffic_events table!")
        return

    print(f"Found data for {len(hourly)} junctions")


    # FIGURE 1 — Traffic Volume vs Time of Day 

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle(
        f"Smart City Colombo — Traffic Analysis Report\n{today}",
        fontsize=16, fontweight="bold", y=0.98
    )

    all_hours = list(range(24))

    for ax, (sid, d) in zip(axes.flatten(), hourly.items()):
        color  = COLORS.get(sid, "#95A5A6")
        marker = MARKERS.get(sid, "o")

        hour_map = dict(zip(d["hours"], d["vehicles"]))
        vehicles = [hour_map.get(h, 0) for h in all_hours]

        bars = ax.bar(all_hours, vehicles, color=color,
                      alpha=0.6, width=0.8, label="Vehicle Count")

        ax.plot(all_hours, vehicles, color=color,
                linewidth=2, marker=marker, markersize=5, zorder=5)

        if sid in alerts:
            for h, cnt in alerts[sid].items():
                if h < 24:
                    ax.bar(h, hour_map.get(h, 0),
                           color="#E74C3C", alpha=0.8, width=0.8)
                    ax.annotate(f"⚠ {cnt}",
                                xy=(h, hour_map.get(h, 0)),
                                xytext=(0, 5),
                                textcoords="offset points",
                                ha="center", fontsize=7,
                                color="#C0392B", fontweight="bold")

        if vehicles:
            peak_h = all_hours[vehicles.index(max(vehicles))]
            ax.axvline(x=peak_h, color="red",
                       linestyle="--", alpha=0.5, linewidth=1.5)
            ax.text(peak_h + 0.3, max(vehicles) * 0.95,
                    f"Peak\n{peak_h:02d}:00",
                    fontsize=8, color="red", fontweight="bold")

        ax.set_title(sid.replace("Junction_", ""),
                     fontsize=12, fontweight="bold")
        ax.set_xlabel("Hour of Day", fontsize=10)
        ax.set_ylabel("Total Vehicles", fontsize=10)
        ax.set_xticks(range(0, 24, 2))
        ax.set_xticklabels([f"{h:02d}:00" for h in range(0, 24, 2)],
                           rotation=45, fontsize=8)
        ax.grid(axis="y", alpha=0.3)
        ax.set_xlim(-0.5, 23.5)

        normal_patch = mpatches.Patch(color=color, alpha=0.6, label="Normal Traffic")
        alert_patch  = mpatches.Patch(color="#E74C3C", alpha=0.8, label="Alert Period")
        ax.legend(handles=[normal_patch, alert_patch], fontsize=8)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    chart1_path = os.path.join(REPORTS_DIR, f"traffic_volume_by_hour_{today}.png")
    plt.savefig(chart1_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"✅ Chart 1 saved: {chart1_path}")

    
    # FIGURE 2 — All junctions comparison on one chart
    
    fig2, ax2 = plt.subplots(figsize=(14, 7))
    ax2.set_title(
        f"Traffic Volume Comparison — All Junctions\n{today}",
        fontsize=14, fontweight="bold"
    )

    for sid, d in hourly.items():
        color  = COLORS.get(sid, "#95A5A6")
        marker = MARKERS.get(sid, "o")
        hour_map = dict(zip(d["hours"], d["vehicles"]))
        vehicles = [hour_map.get(h, 0) for h in all_hours]
        label = sid.replace("Junction_", "")
        ax2.plot(all_hours, vehicles, color=color, linewidth=2.5,
                 marker=marker, markersize=6, label=label)

    ax2.set_xlabel("Hour of Day", fontsize=12)
    ax2.set_ylabel("Total Vehicles", fontsize=12)
    ax2.set_xticks(range(0, 24, 1))
    ax2.set_xticklabels([f"{h:02d}:00" for h in range(0, 24)],
                        rotation=45, fontsize=9)
    ax2.legend(fontsize=11, loc="upper right")
    ax2.grid(alpha=0.3)
    ax2.set_xlim(-0.5, 23.5)

    plt.tight_layout()
    chart2_path = os.path.join(REPORTS_DIR, f"traffic_comparison_{today}.png")
    plt.savefig(chart2_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"✅ Chart 2 saved: {chart2_path}")

    
    # FIGURE 3 — Congestion Index chart
    
    if cong:
        fig3, ax3 = plt.subplots(figsize=(14, 6))
        ax3.set_title(
            f"Congestion Index by Junction\n{today}",
            fontsize=14, fontweight="bold"
        )

        for sid, d in cong.items():
            color = COLORS.get(sid, "#95A5A6")
            label = sid.replace("Junction_", "")
            ax3.plot(d["hours"], d["idx"], color=color,
                     linewidth=2.5, marker="o", markersize=8, label=label)
            for h, idx in zip(d["hours"], d["idx"]):
                ax3.annotate(f"{idx:.1f}",
                             xy=(h, idx), xytext=(0, 8),
                             textcoords="offset points",
                             ha="center", fontsize=9, color=color)

        ax3.axhline(y=8, color="red", linestyle="--",
                    alpha=0.7, label="Police threshold (8.0)")
        ax3.set_xlabel("Hour of Day", fontsize=12)
        ax3.set_ylabel("Congestion Index", fontsize=12)
        ax3.legend(fontsize=11)
        ax3.grid(alpha=0.3)

        plt.tight_layout()
        chart3_path = os.path.join(REPORTS_DIR, f"congestion_index_{today}.png")
        plt.savefig(chart3_path, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"✅ Chart 3 saved: {chart3_path}")

    print()
    print("=" * 60)
    print("All analytic charts generated successfully!")
    print(f"Location: {os.path.abspath(REPORTS_DIR)}/")
    print("=" * 60)


if __name__ == "__main__":
    generate_charts()