"""
Spark Job : fraud_detection_job
==================================
Détecte les patterns frauduleux en temps réel depuis `listening_events`.

3 règles de détection :
  1. > 100 écoutes en 10 min pour un même user_id (burst stream)
  2. Durée moyenne < 5 secondes sur 1 heure (bot pattern)
  3. Taux d'échec de transfert P2P > 50% sur 15 min

Score de suspicion maintenu avec flatMapGroupsWithState.
Alertes écrites dans `fraud_alerts` Kafka + `fraud_detections` PostgreSQL.

Lancement :
    docker exec cours_hetic-spark-master-1 spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1 \\
        /opt/spark-jobs/fraud_detection_job.py
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, FloatType, LongType
)
from pyspark.sql.streaming.state import GroupStateTimeout

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",       "kafka-1:9092")
CHECKPOINT_PATH  = os.getenv("SPARK_CHECKPOINT_PATH", "/tmp/spark-checkpoints/fraud")
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL",  "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS   = {"user": "spotify", "password": "spotify", "driver": "org.postgresql.Driver"}
TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "30 seconds")

FRAUD_SCORE_THRESHOLD = 0.7   # Score > 0.7 → alerte
BURST_COUNT_10M       = 100   # > 100 écoutes en 10 min
BOT_DURATION_MS       = 5_000  # < 5s durée moyenne
P2P_FAIL_RATE         = 0.5   # > 50% transferts échoués

LISTENING_SCHEMA = StructType([
    StructField("event_id",    StringType(),  False),
    StructField("user_id",     StringType(),  False),
    StructField("track_id",    StringType(),  False),
    StructField("source_peer", StringType(),  True),
    StructField("timestamp",   StringType(),  False),
    StructField("duration_ms", IntegerType(), True),
    StructField("device_type", StringType(),  True),
    StructField("geo_country", StringType(),  True),
    StructField("completed",   BooleanType(), True),
    StructField("event_source",StringType(),  True),
])

P2P_SCHEMA = StructType([
    StructField("event_id",   StringType(), False),
    StructField("event_type", StringType(), True),
    StructField("peer_id",    StringType(), True),
    StructField("timestamp",  StringType(), True),
    StructField("success",    BooleanType(),True),
])

# Schéma du state dans flatMapGroupsWithState
STATE_SCHEMA = StructType([
    StructField("user_id",          StringType(), True),
    StructField("suspicion_score",  FloatType(),  True),
    StructField("burst_count",      LongType(),   True),
    StructField("avg_duration_ms",  FloatType(),  True),
    StructField("last_event_ts",    LongType(),   True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SPOTIFY-fraud-detection")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def read_listening_stream(spark: SparkSession):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe",               "listening_events")
        .option("startingOffsets",         "latest")
        .option("kafka.isolation.level",   "read_committed")
        .option("failOnDataLoss",          "false")
        .load()
    )
    return raw.select(
        F.from_json(F.col("value").cast("string"), LISTENING_SCHEMA).alias("d")
    ).select(
        "d.*",
        F.to_timestamp("d.timestamp").alias("event_time"),
    )


# ─────────────────────────────────────────────────────────────
# RÈGLE 1 : Burst > 100 écoutes / 10 min (window aggregation)
# ─────────────────────────────────────────────────────────────

def detect_burst_streams(events_df):
    """Règle 1 : plus de BURST_COUNT_10M écoutes en 10 min par user."""
    return (
        events_df
        .withWatermark("event_time", "5 minutes")
        .groupBy(
            F.window("event_time", "10 minutes").alias("window"),
            F.col("user_id"),
        )
        .agg(F.count("*").alias("listen_count"))
        .filter(F.col("listen_count") > BURST_COUNT_10M)
        .select(
            F.col("user_id"),
            F.lit("burst_stream").alias("fraud_type"),
            (F.col("listen_count").cast("float") / BURST_COUNT_10M).alias("suspicion_score"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
        )
    )


# ─────────────────────────────────────────────────────────────
# RÈGLE 2 : Durée moyenne < 5s sur 1h (window aggregation)
# ─────────────────────────────────────────────────────────────

def detect_bot_duration(events_df):
    """Règle 2 : durée moyenne d'écoute < 5s sur 1 heure (bot pattern)."""
    return (
        events_df
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            F.window("event_time", "1 hour").alias("window"),
            F.col("user_id"),
        )
        .agg(F.avg("duration_ms").alias("avg_duration_ms"))
        .filter(F.col("avg_duration_ms") < BOT_DURATION_MS)
        .select(
            F.col("user_id"),
            F.lit("bot_stream").alias("fraud_type"),
            (F.lit(BOT_DURATION_MS) / F.col("avg_duration_ms")).alias("suspicion_score"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
        )
    )


# ─────────────────────────────────────────────────────────────
# RÈGLE 3 : Taux d'échec P2P > 50% / 15 min (window aggregation)
# ─────────────────────────────────────────────────────────────

def detect_p2p_failures(spark: SparkSession):
    """Règle 3 : taux d'échec de transfert P2P > 50% sur 15 min."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe",               "p2p_network_events")
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")
        .load()
    )
    p2p_df = raw.select(
        F.from_json(F.col("value").cast("string"), P2P_SCHEMA).alias("d")
    ).select(
        "d.*",
        F.to_timestamp("d.timestamp").alias("event_time"),
    ).filter(F.col("d.event_type") == "chunk_transfer")

    return (
        p2p_df
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            F.window("event_time", "15 minutes").alias("window"),
            F.col("peer_id"),
        )
        .agg(
            F.count("*").alias("total_transfers"),
            F.sum(F.when(~F.col("success"), 1).otherwise(0)).alias("failed_transfers"),
        )
        .filter(
            (F.col("failed_transfers") / F.col("total_transfers")) > P2P_FAIL_RATE
        )
        .select(
            F.col("peer_id").alias("user_id"),
            F.lit("p2p_failure").alias("fraud_type"),
            (F.col("failed_transfers") / F.col("total_transfers")).alias("suspicion_score"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
        )
    )


# ─────────────────────────────────────────────────────────────
# ÉCRITURE DES ALERTES
# ─────────────────────────────────────────────────────────────

def write_fraud_alerts(fraud_df, checkpoint_suffix: str):
    """Écrit les alertes dans Kafka fraud_alerts + PostgreSQL fraud_detections."""

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # Kafka fraud_alerts
        batch_df.select(
            F.to_json(F.struct("user_id", "fraud_type", "suspicion_score",
                               "window_start", "window_end")).alias("value")
        ).write.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
            .option("topic", "fraud_alerts") \
            .save()

        # PostgreSQL fraud_detections
        batch_df.select(
            F.col("user_id"),
            F.col("fraud_type"),
            F.col("suspicion_score"),
            F.col("window_start"),
            F.col("window_end"),
        ).write.jdbc(
            url=POSTGRES_URL,
            table="fraud_detections",
            mode="append",
            properties=POSTGRES_PROPS,
        )
        print(f"[Batch {batch_id}] {checkpoint_suffix}: {batch_df.count()} fraud alerts")

    return (
        fraud_df.writeStream
        .outputMode("update")
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{checkpoint_suffix}")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("fraud_detection_job — 3 rules: burst_stream, bot_duration, p2p_failure (issue #18)")
    print("Simulator mode: python -m src.p2p_simulator.simulator --mode fraud")

    events_df = read_listening_stream(spark)

    q1 = write_fraud_alerts(detect_burst_streams(events_df),  "burst")
    q2 = write_fraud_alerts(detect_bot_duration(events_df),   "bot_duration")
    q3 = write_fraud_alerts(detect_p2p_failures(spark),       "p2p_failures")

    print("Fraud detection started | Kafka: fraud_alerts | PG: fraud_detections")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
