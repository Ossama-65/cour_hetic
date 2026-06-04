"""
Spark Job : streaming_trends_job
==================================
Consomme le topic Kafka `listening_events` et produit en continu
les tendances musicales temps réel.

Outputs :
    - PostgreSQL -> table `realtime_top_tracks` (top 10 par fenetre de 5 min)
    - Redis      -> cle `genre_listeners:live` (top genres par sliding window)

Lancement :
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1 \\
        spark_jobs/streaming_trends_job.py
"""

import json
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

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka-1:9092")
KAFKA_TOPIC      = "listening_events"
CHECKPOINT_PATH  = "/tmp/spark-checkpoints/streaming_trends"
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL",
                             "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS   = {
    "user":     "airflow",
    "password": "airflow",
    "driver":   "org.postgresql.Driver",
    "stringtype": "unspecified",
}
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = 1

# ─────────────────────────────────────────────────────────────
# SCHEMA DES EVENEMENTS D'ECOUTE
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
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-trends")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )

# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    events_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(F.from_json(F.col("json_str"), LISTENING_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time",
                    F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"))
        .withWatermark("event_time", "1 minutes")
    )

    return events_df

# ─────────────────────────────────────────────────────────────
# AGREGATIONS STREAMING
# ─────────────────────────────────────────────────────────────

def compute_top_tracks_tumbling(events_df):
    top_tracks_df = (
        events_df
        .groupBy(
            F.window("event_time", "5 minutes"),
            "track_id"
        )
        .agg(
            F.count("*").alias("stream_count"),
            F.approx_count_distinct("user_id").alias("unique_listeners")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "track_id",
            "stream_count",
            "unique_listeners"
        )
    )

    def write_to_postgres(batch_df, batch_id):
        try:
            if batch_df.count() == 0:
                return
            batch_df.write.jdbc(
                url=POSTGRES_URL,
                table="realtime_top_tracks",
                mode="overwrite",
                properties={**POSTGRES_PROPS, "stringtype": "unspecified"},
            )
            print(f"[batch {batch_id}] top_tracks: {batch_df.count()} lignes ecrites dans PostgreSQL")
        except Exception as e:
            print(f"[batch {batch_id}] Erreur PostgreSQL: {e}")

    query = (
        top_tracks_df.writeStream
        .outputMode("update")
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/top_tracks")
        .trigger(processingTime="30 seconds")
        .start()
    )

    return query


def compute_genre_listeners_sliding(events_df, catalog_df):
    enriched_df = events_df.join(
        catalog_df.select("id", "genre").withColumnRenamed("id", "track_id_cat"),
        events_df["track_id"] == F.col("track_id_cat"),
        "left"
    )

    genre_df = (
        enriched_df
        .groupBy(
            F.window("event_time", "15 minutes", "5 minutes"),
            "genre"
        )
        .agg(
            F.approx_count_distinct("user_id").alias("unique_listeners")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "genre",
            "unique_listeners"
        )
    )

    def write_to_redis(batch_df, batch_id):
        try:
            if batch_df.count() == 0:
                return
            import redis
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
            rows = batch_df.collect()
            data = [
                {"genre": row["genre"], "unique_listeners": row["unique_listeners"]}
                for row in rows if row["genre"] is not None
            ]
            data.sort(key=lambda x: x["unique_listeners"], reverse=True)
            r.setex("genre_listeners:live", 3600, json.dumps(data))
            print(f"[batch {batch_id}] genre_listeners: {len(data)} genres ecrits dans Redis")
        except Exception as e:
            print(f"[batch {batch_id}] Erreur Redis: {e}")

    query = (
        genre_df.writeStream
        .outputMode("update")
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/genre_listeners")
        .trigger(processingTime="30 seconds")
        .start()
    )

    return query

# ─────────────────────────────────────────────────────────────
# POINT D'ENTREE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Demarrage streaming_trends_job...")
    print(f"Kafka : {KAFKA_BOOTSTRAP} -> topic : {KAFKA_TOPIC}")
    print(f"Checkpoint : {CHECKPOINT_PATH}")

    events_df = read_kafka_stream(spark)

    catalog_df = spark.read.jdbc(POSTGRES_URL, "tracks", properties=POSTGRES_PROPS)

    query_top_tracks = compute_top_tracks_tumbling(events_df)
    query_genres     = compute_genre_listeners_sliding(events_df, catalog_df)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()