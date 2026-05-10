import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType
)

# Logging 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("SparkStreamProcessor")

# Config — Docker internal hostnames
KAFKA_BROKER   = "kafka:9092"
KAFKA_TOPIC    = "traffic-raw"
PG_URL         = "jdbc:postgresql://postgres:5432/traffic_db"
PG_PROPS       = {
    "user":     "traffic_user",
    "password": "traffic_pass",
    "driver":   "org.postgresql.Driver"
}
CRITICAL_SPEED = 10.0
WINDOW_SIZE    = "5 minutes"
WATERMARK      = "10 minutes"

# Schema matching producer JSON 
SCHEMA = StructType([
    StructField("sensor_id",     StringType(),  True),
    StructField("timestamp",     StringType(),  True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed",     FloatType(),   True),
])


def create_spark():
    log.info("Starting Spark session...")
    spark = (
        SparkSession.builder
        .appName("SmartCity_TrafficStreamProcessor")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    log.info("Spark session ready")
    return spark


def get_parsed_stream(spark):
    """
    Read Kafka stream and parse JSON.
    event_time uses EVENT TIME from sensor message timestamp.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        raw
        .select(
            F.from_json(F.col("value").cast("string"), SCHEMA).alias("d")
        )
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("ingested_at", F.current_timestamp())
        .filter(F.col("sensor_id").isNotNull())
    )
    log.info("Kafka stream parsed with schema")
    return parsed


# QUERY 1 — Raw events → traffic_events
def start_raw_stream(parsed):
    def write_batch(df, batch_id):
        count = df.count()
        if count == 0:
            return
        df.select(
            "sensor_id",
            F.col("event_time").alias("event_timestamp"),
            "vehicle_count",
            "avg_speed",
            "ingested_at"
        ).write.jdbc(PG_URL, "traffic_events", "append", PG_PROPS)
        log.info("📥 [Raw Events]  Batch %d → %d rows → traffic_events",
                 batch_id, count)

    return (
        parsed.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", "/tmp/cp/raw")
        .trigger(processingTime="10 seconds")
        .start()
    )



# QUERY 2 — Critical alerts → critical_traffic
def start_alerts_stream(parsed):
    alerts = parsed.filter(F.col("avg_speed") < CRITICAL_SPEED)

    def write_batch(df, batch_id):
        count = df.count()
        if count == 0:
            return
        for r in df.collect():
            log.warning(
                "🚨 ALERT | %-28s | vehicles: %3d | speed: %.1f km/h ← CRITICAL",
                r["sensor_id"], r["vehicle_count"], r["avg_speed"]
            )
        df.select(
            "sensor_id",
            F.col("event_time").alias("event_timestamp"),
            "vehicle_count",
            "avg_speed",
            F.lit("CRITICAL: avg_speed below 10 km/h").alias("alert_message"),
            F.current_timestamp().alias("alerted_at")
        ).write.jdbc(PG_URL, "critical_traffic", "append", PG_PROPS)
        log.warning("🚨 [Alerts]      Batch %d → %d alerts → critical_traffic",
                    batch_id, count)

    return (
        alerts.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", "/tmp/cp/alerts")
        .trigger(processingTime="5 seconds")
        .start()
    )



# QUERY 3 — 5-min tumbling window → congestion_index

def start_congestion_stream(parsed):
    windowed = (
        parsed
        .withWatermark("event_time", WATERMARK)
        .groupBy(
            "sensor_id",
            F.window("event_time", WINDOW_SIZE)
        )
        .agg(
            F.sum("vehicle_count").alias("total_vehicles"),
            F.round(F.avg("avg_speed"), 2).alias("avg_speed"),
        )
        .withColumn(
            "congestion_idx",
            F.round(F.col("total_vehicles") / F.col("avg_speed"), 2)
        )
        .select(
            "sensor_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "total_vehicles",
            "avg_speed",
            "congestion_idx"
        )
    )

    def write_batch(df, batch_id):
        count = df.count()
        if count == 0:
            return
        for r in df.collect():
            log.info(
                "[Window] %s→%s | %-28s | "
                "vehicles: %4d | speed: %5.1f | idx: %.2f",
                r["window_start"].strftime("%H:%M"),
                r["window_end"].strftime("%H:%M"),
                r["sensor_id"],
                r["total_vehicles"],
                r["avg_speed"],
                r["congestion_idx"]
            )
        df.write.jdbc(PG_URL, "congestion_index", "append", PG_PROPS)
        log.info("✅ [Congestion]  Batch %d → %d windows → congestion_index",
                 batch_id, count)

    return (
        windowed.writeStream
        .outputMode("append")
        .foreachBatch(write_batch)
        .option("checkpointLocation", "/tmp/cp/windows")
        .trigger(processingTime="30 seconds")
        .start()
    )


def main():
    log.info("=" * 65)
    log.info("  Smart City — Spark Structured Streaming Processor")
    log.info("  Kafka  : %s  topic: %s", KAFKA_BROKER, KAFKA_TOPIC)
    log.info("  Sink   : PostgreSQL postgres:5432/traffic_db")
    log.info("  Window : %s tumbling (EVENT TIME)", WINDOW_SIZE)
    log.info("  Alert  : avg_speed < %.1f km/h", CRITICAL_SPEED)
    log.info("=" * 65)

    spark  = create_spark()
    stream = get_parsed_stream(spark)

    log.info("Starting 3 parallel streaming queries...")
    q1 = start_raw_stream(stream)
    q2 = start_alerts_stream(stream)
    q3 = start_congestion_stream(stream)

    log.info("   All 3 streams active:")
    log.info("   Q1 → Every raw event  → traffic_events    (every 10s)")
    log.info("   Q2 → speed < 10 km/h → critical_traffic  (every  5s)")
    log.info("   Q3 → 5-min windows   → congestion_index  (every 30s)")
    log.info("   Waiting for data... (Ctrl+C to stop)")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()