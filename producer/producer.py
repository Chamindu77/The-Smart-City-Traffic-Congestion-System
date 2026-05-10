import json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Logging 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("TrafficProducer")

# Configuration
KAFKA_BROKER  = "localhost:9093"
KAFKA_TOPIC   = "traffic-raw"
SEND_INTERVAL = 1                    


ALERT_CHANCE            = 0.005      
MAX_ALERTS_PER_JUNCTION = 3          

# 4 Colombo junctions 
JUNCTIONS = [
    {
        "sensor_id":    "Junction_Pettah",
        "normal_speed": (15, 55),
        "normal_count": (20, 80),
    },
    {
        "sensor_id":    "Junction_Kollupitiya",
        "normal_speed": (20, 60),
        "normal_count": (15, 70),
    },
    {
        "sensor_id":    "Junction_Nugegoda",
        "normal_speed": (10, 50),
        "normal_count": (25, 90),
    },
    {
        "sensor_id":    "Junction_Maharagama",
        "normal_speed": (15, 45),
        "normal_count": (30, 100),
    },
]


def create_producer():
    """Connect to Kafka with retries."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
            )
            log.info("✅ Connected to Kafka at %s", KAFKA_BROKER)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka not ready. Retrying in 5 seconds...")
            time.sleep(5)


def generate_normal_event(junction: dict) -> dict:
    """Normal traffic — speed and count within typical range."""
    return {
        "sensor_id":     junction["sensor_id"],
        "timestamp":     datetime.now().isoformat(),
        "vehicle_count": random.randint(*junction["normal_count"]),
        "avg_speed":     round(random.uniform(*junction["normal_speed"]), 1),
    }


def generate_critical_event(junction: dict) -> dict:
    """
    Critical congestion — avg_speed below 10 km/h.
    This triggers the real-time alert pipeline.
    Generated OCCASIONALLY — controlled by MAX_ALERTS_PER_JUNCTION.
    """
    return {
        "sensor_id":     junction["sensor_id"],
        "timestamp":     datetime.now().isoformat(),
        "vehicle_count": random.randint(90, 150),
        "avg_speed":     round(random.uniform(2.0, 9.9), 1),
    }


def main():
    log.info("=" * 65)
    log.info("  Smart City Traffic Producer — Starting")
    log.info("  Kafka Topic : %s", KAFKA_TOPIC)
    log.info("  Junctions   : 4 (Pettah, Kollupitiya, Nugegoda, Maharagama)")
    log.info("  Interval    : %ds per cycle", SEND_INTERVAL)
    log.info("  Alert Rate  : %.1f%% chance (CONTROLLED — max %d per junction)",
             ALERT_CHANCE * 100, MAX_ALERTS_PER_JUNCTION)
    log.info("=" * 65)

    producer = create_producer()
    message_count = 0

    alert_counts = {j["sensor_id"]: 0 for j in JUNCTIONS}

    try:
        while True:
            for junction in JUNCTIONS:
                sid = junction["sensor_id"]

                under_cap  = alert_counts[sid] < MAX_ALERTS_PER_JUNCTION
                is_critical = under_cap and (random.random() < ALERT_CHANCE)

                if is_critical:
                    event = generate_critical_event(junction)
                    alert_counts[sid] += 1
                    log.warning(
                        "🚨 CRITICAL | %-28s | vehicles: %3d | "
                        "speed: %5.1f km/h  ← ALERT [%d/%d for this junction]",
                        sid,
                        event["vehicle_count"],
                        event["avg_speed"],
                        alert_counts[sid],
                        MAX_ALERTS_PER_JUNCTION
                    )
                else:
                    event = generate_normal_event(junction)
                    log.info(
                        "✅ NORMAL   | %-28s | vehicles: %3d | speed: %5.1f km/h",
                        sid,
                        event["vehicle_count"],
                        event["avg_speed"],
                    )

                producer.send(KAFKA_TOPIC, value=event)
                producer.flush()
                message_count += 1

            # Summary every 20 messages
            if message_count % 20 == 0:
                active_alerts = {k: v for k, v in alert_counts.items() if v > 0}
                log.info("-" * 65)
                log.info("  📊 Messages sent: %d", message_count)
                if active_alerts:
                    log.info("  🚨 Alerts so far: %s", active_alerts)
                else:
                    log.info("  🚨 No critical alerts yet (waiting for 0.5%% chance)")
                log.info("-" * 65)

            time.sleep(SEND_INTERVAL)

    except KeyboardInterrupt:
        log.info("=" * 65)
        log.info("  Producer stopped by user.")
        log.info("  Total messages sent : %d", message_count)
        log.info("  Total critical alerts: %d", sum(alert_counts.values()))
        log.info("  Per junction breakdown:")
        for sid, count in alert_counts.items():
            log.info("      %-28s → %d alerts", sid, count)
        log.info("=" * 65)
    finally:
        producer.close()
        log.info("  Kafka connection closed.")


if __name__ == "__main__":
    main()