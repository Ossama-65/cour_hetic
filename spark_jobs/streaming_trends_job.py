"""
Spark Job : streaming_trends_job
==================================
Consomme le topic Kafka `listening_events` et produit en continu
les tendances musicales temps réel.

Outputs (Issue #13 — console) :
    - Console sink : affichage des events bruts pour validation
    - Console sink : top tracks par fenêtre tumbling de 5 min (debug)

Outputs (Issue #14+ — production) :
    - PostgreSQL → table `realtime_top_tracks`
    - Redis      → clé `genre_listeners:live`

Lancement :
    docker exec cours_hetic-spark-master-1 spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1 \\
        /opt/spark-jobs/streaming_trends_job.py

Trigger modes (à expérimenter) :
    processingTime("10 seconds")   : traitement toutes les 10s
    Once()                          : traitement unique des offsets disponibles
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",        "kafka-1:9092")
KAFKA_TOPIC      = "listening_events"
CHECKPOINT_PATH  = os.getenv("SPARK_CHECKPOINT_PATH", "/tmp/spark-checkpoints/streaming_trends")
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL",
                             "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS   = {
    "user":     "spotify",
    "password": "spotify",
    "driver":   "org.postgresql.Driver",
}

# Trigger mode : "processingTime" (continu) ou "once" (one-shot)
TRIGGER_MODE     = os.getenv("SPARK_TRIGGER_MODE",     "processingTime")
TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "10 seconds")
WATERMARK_DELAY  = os.getenv("SPARK_WATERMARK_DELAY",  "10 minutes")
LATE_EVENTS_TOPIC = "late_listening_events"

# ─────────────────────────────────────────────────────────────
# SCHÉMA DES ÉVÉNEMENTS D'ÉCOUTE
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",     StringType(),  False),
    StructField("user_id",      StringType(),  False),
    StructField("track_id",     StringType(),  False),
    StructField("source_peer",  StringType(),  True),
    StructField("timestamp",    StringType(),  False),
    StructField("duration_ms",  IntegerType(), True),
    StructField("device_type",  StringType(),  True),
    StructField("geo_country",  StringType(),  True),
    StructField("completed",    BooleanType(), True),
    StructField("event_source", StringType(),  True),
])


# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """Crée la SparkSession avec packages Kafka + PostgreSQL + MinIO."""
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-trends")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint",          "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",        "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",        "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """
    Lit le topic Kafka `listening_events` en streaming.

    1. readStream.format("kafka") avec bootstrap.servers et subscribe
    2. Cast de la colonne value (bytes) en string
    3. Parsing JSON avec from_json() et LISTENING_EVENT_SCHEMA
    4. Cast timestamp string ISO 8601 → TimestampType (event_time)
    5. Filtre isolation.level=read_committed pour exactly-once

    Returns:
        DataFrame streaming avec colonnes typées
    """
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",       KAFKA_BOOTSTRAP)
        .option("subscribe",                      KAFKA_TOPIC)
        .option("startingOffsets",                "latest")
        .option("kafka.isolation.level",          "read_committed")
        .option("failOnDataLoss",                 "false")
        .load()
    )

    # Désérialiser la valeur JSON
    json_df = raw_df.select(
        F.from_json(F.col("value").cast("string"), LISTENING_EVENT_SCHEMA).alias("data"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("partition"),
        F.col("offset"),
    )

    # Aplatir + caster le timestamp événement
    events_df = json_df.select(
        "data.*",
        F.to_timestamp(F.col("data.timestamp")).alias("event_time"),
        "kafka_timestamp",
        "partition",
        "offset",
    )

    return events_df


def route_late_events(events_df):
    """
    Route les events tardifs (> WATERMARK_DELAY avant NOW) vers le topic
    `late_listening_events` Kafka pour retraitement par Airflow.

    Un event est considéré tardif si son event_time est plus ancien que
    le délai watermark par rapport à l'heure Kafka de réception.
    """
    from pyspark.sql.functions import current_timestamp, expr

    late_df = events_df.filter(
        F.col("event_time") < (F.col("kafka_timestamp") - expr(f"INTERVAL {WATERMARK_DELAY}"))
    )

    def write_late_to_kafka(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        batch_df.select(
            F.to_json(F.struct(
                "event_id", "user_id", "track_id", "timestamp",
                "duration_ms", "device_type", "geo_country",
                "completed", "event_source",
            )).alias("value")
        ).write.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
            .option("topic", LATE_EVENTS_TOPIC) \
            .save()
        print(f"[Batch {batch_id}] {batch_df.count()} late events routed → {LATE_EVENTS_TOPIC}")

    query = (
        late_df.writeStream
        .outputMode("append")
        .foreachBatch(write_late_to_kafka)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/late_events")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    return query


# ─────────────────────────────────────────────────────────────
# AGRÉGATIONS STREAMING
# ─────────────────────────────────────────────────────────────

def compute_top_tracks_tumbling(events_df):
    """
    Top 10 des tracks par tumbling window de 5 minutes — sink console.

    Issue #13 : console output pour valider la lecture Kafka.
    Issue #14 : remplacer le sink console par PostgreSQL via foreachBatch.

    Trigger : processingTime("10 seconds") ou Once()
    """
    # Watermark de 10 min pour gérer les late events
    windowed_df = (
        events_df
        .withWatermark("event_time", "10 minutes")
        .where(F.col("completed") == True)  # noqa: E712
        .groupBy(
            F.window("event_time", "5 minutes").alias("window"),
            F.col("track_id"),
        )
        .agg(
            F.count("*").alias("stream_count"),
            F.approx_count_distinct("user_id").alias("unique_listeners"),
        )
        # orderBy non supporté en streaming — le tri se fera dans foreachBatch (issue #14)
    )

    # Sink console (Issue #13) — affiche les résultats dans les logs Spark
    if TRIGGER_MODE == "once":
        trigger_opts = {"once": True}
    else:
        trigger_opts = {"processingTime": TRIGGER_INTERVAL}

    query = (
        windowed_df.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", "false")
        .option("numRows", 10)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/top_tracks_console")
        .trigger(**trigger_opts)
        .start()
    )

    return query


def stream_raw_events_console(events_df):
    """
    Affiche les events bruts en console — utile pour valider la désérialisation.
    Sink console append mode, trigger 10 secondes.
    """
    query = (
        events_df
        .select("event_id", "user_id", "track_id", "device_type", "geo_country",
                "completed", "event_source", "event_time")
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "true")
        .option("numRows", 5)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/raw_events_console")
        .trigger(processingTime="10 seconds")
        .start()
    )
    return query


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("streaming_trends_job — watermarking + late events (issue #15)")
    print(f"Kafka     : {KAFKA_BOOTSTRAP} → {KAFKA_TOPIC}")
    print(f"Watermark : {WATERMARK_DELAY}")
    print(f"Trigger   : {TRIGGER_INTERVAL}")
    print(f"Late topic: {LATE_EVENTS_TOPIC}")
    print("=" * 60)

    events_df = read_kafka_stream(spark)

    # Routing des late events → late_listening_events topic
    query_late = route_late_events(events_df)

    # Top tracks (console sink pour validation)
    query_top = compute_top_tracks_tumbling(events_df)

    print("Streaming lancé | Kafka UI: http://localhost:8090 | Spark UI: http://localhost:8888")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
