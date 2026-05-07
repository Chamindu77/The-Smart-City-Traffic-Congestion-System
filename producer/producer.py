"""
=============================================================
 Smart City Traffic - Kafka Producer (Sensor Simulator)
 Scenario 1: IoT Traffic Sensors at 4 Colombo Junctions
=============================================================
 What this does:
   - Simulates 4 traffic sensors sending data every 1 second
   - Publishes JSON messages to Kafka topic: traffic-raw
   - Occasionally injects LOW SPEED events (avg_speed < 10)
     to trigger the critical alert pipeline (5% chance)

 Output format:
   {
     "sensor_id":     "Junction_Pettah",
     "timestamp":     "2024-01-15T08:32:01.123456",
     "vehicle_count": 47,
     "avg_speed":     34.2
   }
=============================================================
"""

import json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Logging setup ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("TrafficProducer")

# ── Configuration ──────────────────────────────────────────
KAFKA_BROKER  = "localhost:9093"   # external listener port
KAFKA_TOPIC   = "traffic-raw"
SEND_INTERVAL = 1                  # seconds between cycles
ALERT_CHANCE  = 0.05               # 5% chance of critical event

# ── 4 Colombo junctions ────────────────────────────────────
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
    """Connect to Kafka with retries every 5 seconds."""
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
            log.warning("⏳ Kafka not ready. Retrying in 5 seconds...")
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
    """Critical congestion — avg_speed below 10 km/h triggers alert."""
    return {
        "sensor_id":     junction["sensor_id"],
        "timestamp":     datetime.now().isoformat(),
        "vehicle_count": random.randint(90, 150),
        "avg_speed":     round(random.uniform(2.0, 9.9), 1),
    }


def send_event(producer: KafkaProducer, event: dict, is_critical: bool):
    """Send event to Kafka and log clearly."""
    producer.send(KAFKA_TOPIC, value=event)
    producer.flush()

    if is_critical:
        log.warning(
            "🚨 CRITICAL | %-28s | vehicles: %3d | speed: %5.1f km/h  ← ALERT",
            event["sensor_id"],
            event["vehicle_count"],
            event["avg_speed"],
        )
    else:
        log.info(
            "✅ NORMAL   | %-28s | vehicles: %3d | speed: %5.1f km/h",
            event["sensor_id"],
            event["vehicle_count"],
            event["avg_speed"],
        )


def main():
    log.info("=" * 65)
    log.info("  Smart City Traffic Producer — Starting")
    log.info("  Kafka Topic : %s", KAFKA_TOPIC)
    log.info("  Junctions   : %d (Pettah, Kollupitiya, Nugegoda, Maharagama)")
    log.info("  Interval    : %ds per cycle", SEND_INTERVAL)
    log.info("  Alert Rate  : %.0f%% chance of critical event", ALERT_CHANCE * 100)
    log.info("=" * 65)

    producer = create_producer()
    message_count = 0

    try:
        while True:
            for junction in JUNCTIONS:
                is_critical = random.random() < ALERT_CHANCE
                event = generate_critical_event(junction) if is_critical \
                        else generate_normal_event(junction)
                send_event(producer, event, is_critical)
                message_count += 1

            if message_count % 20 == 0:
                log.info("-" * 65)
                log.info("  📊 Total messages sent: %d", message_count)
                log.info("-" * 65)

            time.sleep(SEND_INTERVAL)

    except KeyboardInterrupt:
        log.info("=" * 65)
        log.info("  🛑 Producer stopped by user.")
        log.info("  📊 Total messages sent: %d", message_count)
        log.info("=" * 65)
    finally:
        producer.close()
        log.info("  Kafka connection closed cleanly.")


if __name__ == "__main__":
    main()