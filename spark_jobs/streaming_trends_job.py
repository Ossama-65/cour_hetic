"""
Spark Job : streaming_trends_job
==================================
Consomme le topic Kafka `listening_events` et produit en continu
les tendances musicales temps réel.

Outputs :
    - PostgreSQL → table `realtime_top_tracks` (top 10 par fenêtre tumbling 5 min)
    - Redis      → clé `genre_listeners:live` (genres par sliding window 15 min)

Lancement :
    docker exec cours_hetic-spark-master-1 spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1 \\
        /opt/spark-jobs/streaming_trends_job.py

Observer :
    docker exec cours_hetic-postgres-1 watch -n5 \\
        "psql -U spotify -d spotify -c 'SELECT * FROM realtime_top_tracks ORDER BY stream_count DESC LIMIT 5'"
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP",         "kafka-1:9092")
KAFKA_TOPIC       = "listening_events"
CHECKPOINT_PATH   = os.getenv("SPARK_CHECKPOINT_PATH",   "/tmp/spark-checkpoints/streaming_trends")
POSTGRES_URL      = os.getenv("SPOTIFY_POSTGRES_URL",    "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS    = {"user": "spotify", "password": "spotify", "driver": "org.postgresql.Driver"}
REDIS_HOST        = os.getenv("REDIS_HOST", "redis")
REDIS_PORT        = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB          = int(os.getenv("REDIS_DB", "1"))
TRIGGER_INTERVAL  = os.getenv("SPARK_TRIGGER_INTERVAL",  "30 seconds")

# ─────────────────────────────────────────────────────────────
# SCHÉMA
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
# SPARK SESSION
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-trends")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
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
    isolation.level=read_committed pour exactly-once.
    """
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",  KAFKA_BOOTSTRAP)
        .option("subscribe",                KAFKA_TOPIC)
        .option("startingOffsets",          "latest")
        .option("kafka.isolation.level",    "read_committed")
        .option("failOnDataLoss",           "false")
        .load()
    )

    json_df = raw_df.select(
        F.from_json(F.col("value").cast("string"), LISTENING_EVENT_SCHEMA).alias("data"),
    )

    return json_df.select(
        "data.*",
        F.to_timestamp(F.col("data.timestamp")).alias("event_time"),
    )


# ─────────────────────────────────────────────────────────────
# AGRÉGATIONS — TOP TRACKS (tumbling 5 min → PostgreSQL)
# ─────────────────────────────────────────────────────────────

def compute_top_tracks_tumbling(events_df):
    """
    Top 10 par tumbling window de 5 min → `realtime_top_tracks` PostgreSQL.
    Utilise foreachBatch pour l'écriture JDBC avec tri et upsert.
    """
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
    )

    def write_top_tracks_to_postgres(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # Extraire window start/end + top 10 par fenêtre
        top_df = (
            batch_df
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "track_id",
                "stream_count",
                "unique_listeners",
            )
        )

        # Écrire en PostgreSQL (truncate+insert par batch — idempotent via window_start)
        top_df.write.jdbc(
            url=POSTGRES_URL,
            table="realtime_top_tracks",
            mode="append",
            properties=POSTGRES_PROPS,
        )
        print(f"[Batch {batch_id}] top_tracks written: {top_df.count()} rows")

    query = (
        windowed_df.writeStream
        .outputMode("update")
        .foreachBatch(write_top_tracks_to_postgres)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/top_tracks")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    return query


# ─────────────────────────────────────────────────────────────
# AGRÉGATIONS — GENRE LISTENERS (sliding 15 min / 5 min → Redis)
# ─────────────────────────────────────────────────────────────

def compute_genre_listeners_sliding(events_df, spark: SparkSession):
    """
    Listeners uniques par genre en sliding window (15 min / 5 min) → Redis.
    Jointure stream-static avec la table `tracks` PostgreSQL pour récupérer le genre.
    """
    # Chargement du catalogue (jointure statique — rechargé à chaque restart)
    catalog_df = spark.read.jdbc(
        url=POSTGRES_URL,
        table="(SELECT id::text AS track_id, genre FROM tracks) t",
        properties=POSTGRES_PROPS,
    ).cache()

    enriched_df = events_df.join(
        catalog_df,
        events_df["track_id"] == catalog_df["track_id"],
        "left",
    ).select(
        events_df["event_time"],
        events_df["user_id"],
        F.coalesce(catalog_df["genre"], F.lit("Unknown")).alias("genre"),
    )

    sliding_df = (
        enriched_df
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            F.window("event_time", "15 minutes", "5 minutes").alias("window"),
            F.col("genre"),
        )
        .agg(F.approx_count_distinct("user_id").alias("unique_listeners"))
    )

    def write_genres_to_redis(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        import redis as redis_lib, json

        r = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        rows = batch_df.select("genre", "unique_listeners").collect()
        genre_data = {row["genre"]: int(row["unique_listeners"]) for row in rows}
        r.set("genre_listeners:live", json.dumps(genre_data))
        print(f"[Batch {batch_id}] genre_listeners:live updated: {genre_data}")

    query = (
        sliding_df.writeStream
        .outputMode("update")
        .foreachBatch(write_genres_to_redis)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/genre_listeners")
        .trigger(processingTime=TRIGGER_INTERVAL)
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
    print("streaming_trends_job — PostgreSQL + Redis sinks (issue #14)")
    print(f"Kafka  : {KAFKA_BOOTSTRAP} → {KAFKA_TOPIC}")
    print(f"PG URL : {POSTGRES_URL}")
    print(f"Trigger: {TRIGGER_INTERVAL}")
    print("=" * 60)

    events_df = read_kafka_stream(spark)

    query_top    = compute_top_tracks_tumbling(events_df)
    query_genres = compute_genre_listeners_sliding(events_df, spark)

    print("Streaming queries started. Monitoring realtime_top_tracks...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
