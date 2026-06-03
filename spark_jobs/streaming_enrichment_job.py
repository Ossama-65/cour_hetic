"""
Spark Job : streaming_enrichment_job
======================================
Enrichit les events listening avec le catalogue PostgreSQL (stream-static)
et les events P2P (stream-stream), déduplique, et écrit dans :
  - Topic Kafka `enriched_events`
  - Parquet MinIO partitionné par date/hour

Lancement :
    docker exec cours_hetic-spark-master-1 spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1 \\
        /opt/spark-jobs/streaming_enrichment_job.py
"""

import io
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP",       "kafka-1:9092")
CHECKPOINT_PATH     = os.getenv("SPARK_CHECKPOINT_PATH", "/tmp/spark-checkpoints/enrichment")
POSTGRES_URL        = os.getenv("SPOTIFY_POSTGRES_URL",  "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS      = {"user": "spotify", "password": "spotify", "driver": "org.postgresql.Driver"}
TRIGGER_INTERVAL    = os.getenv("SPARK_TRIGGER_INTERVAL", "30 seconds")
STREAM_JOIN_WATERMARK = "2 minutes"

LISTENING_SCHEMA = StructType([
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

P2P_SCHEMA = StructType([
    StructField("event_id",    StringType(), False),
    StructField("event_type",  StringType(), True),
    StructField("peer_id",     StringType(), True),
    StructField("timestamp",   StringType(), True),
    StructField("track_id",    StringType(), True),
    StructField("target_peer", StringType(), True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-enrichment")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def read_stream(spark: SparkSession, topic: str, schema):
    """Lit un topic Kafka et parse le JSON selon le schéma fourni."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe",               topic)
        .option("startingOffsets",         "latest")
        .option("kafka.isolation.level",   "read_committed")
        .option("failOnDataLoss",          "false")
        .load()
    )
    return raw.select(
        F.from_json(F.col("value").cast("string"), schema).alias("d")
    ).select("d.*", F.to_timestamp("d.timestamp").alias("event_time"))


def enrich_with_catalog(listening_df, spark: SparkSession):
    """
    Jointure stream-static : listening_events × tracks (PostgreSQL).
    Ajoute : track_title, artist_id, genre.
    """
    catalog_df = spark.read.jdbc(
        url=POSTGRES_URL,
        table="(SELECT id::text AS track_id, title AS track_title, artist_id::text, genre FROM tracks) t",
        properties=POSTGRES_PROPS,
    ).cache()

    return listening_df.join(
        catalog_df,
        listening_df["track_id"] == catalog_df["track_id"],
        "left",
    ).select(
        listening_df["*"],
        F.coalesce(catalog_df["track_title"], F.lit("Unknown")).alias("track_title"),
        catalog_df["artist_id"],
        F.coalesce(catalog_df["genre"], F.lit("Unknown")).alias("genre"),
    )


def join_with_p2p_events(enriched_df, p2p_df):
    """
    Jointure stream-stream : listening_events × p2p_network_events (chunk_transfer).
    Fenêtre de jointure : 2 minutes max (watermark).
    Ajoute : transfer_peer (qui a servi le morceau).
    """
    p2p_transfers = p2p_df.filter(
        F.col("event_type") == "chunk_transfer"
    ).select(
        F.col("track_id").alias("p2p_track_id"),
        F.col("peer_id").alias("transfer_peer"),
        F.col("event_time").alias("p2p_event_time"),
    ).withWatermark("p2p_event_time", STREAM_JOIN_WATERMARK)

    enriched_wm = enriched_df.withWatermark("event_time", STREAM_JOIN_WATERMARK)

    return enriched_wm.join(
        p2p_transfers,
        (enriched_wm["track_id"] == p2p_transfers["p2p_track_id"]) &
        (enriched_wm["event_time"].between(
            p2p_transfers["p2p_event_time"] - F.expr(f"INTERVAL {STREAM_JOIN_WATERMARK}"),
            p2p_transfers["p2p_event_time"] + F.expr(f"INTERVAL {STREAM_JOIN_WATERMARK}"),
        )),
        "left",
    ).drop("p2p_track_id", "p2p_event_time")


def write_to_kafka_and_parquet(enriched_df):
    """
    Écrit dans :
    - Kafka topic `enriched_events` (valeur JSON)
    - MinIO Parquet partitionné date/hour
    """
    output_cols = [
        "event_id", "user_id", "track_id", "track_title", "artist_id",
        "genre", "timestamp", "duration_ms", "device_type", "geo_country",
        "completed", "event_source", "transfer_peer",
    ]

    # Déduplique par event_id (exactly-once)
    deduped = enriched_df.dropDuplicates(["event_id"])

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # Kafka
        batch_df.select(
            F.to_json(F.struct(*[c for c in output_cols if c in batch_df.columns])).alias("value")
        ).write.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
            .option("topic", "enriched_events") \
            .save()

        # Parquet MinIO (partitionné par date/hour)
        import boto3, io as _io
        import pandas as pd
        import pyarrow as pa, pyarrow.parquet as pq

        pdf = batch_df.select(*[c for c in output_cols if c in batch_df.columns]).toPandas()
        pdf["_date"] = pd.to_datetime(pdf["timestamp"], utc=True, errors="coerce").dt.strftime("%Y-%m-%d")
        pdf["_hour"] = pd.to_datetime(pdf["timestamp"], utc=True, errors="coerce").dt.hour

        s3 = boto3.client("s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin")

        for (date, hour), grp in pdf.groupby(["_date", "_hour"]):
            export = grp.drop(columns=["_date", "_hour"])
            table = pa.Table.from_pandas(export, preserve_index=False)
            buf = _io.BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)
            key = f"enriched_events/date={date}/hour={int(hour):02d}/part-{batch_id}.parquet"
            s3.upload_fileobj(buf, "spotify-parquet", key)

        print(f"[Batch {batch_id}] enriched: {len(pdf)} rows → Kafka + MinIO")

    query = (
        deduped.writeStream
        .outputMode("append")
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/enriched")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    return query


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("streaming_enrichment_job — stream-static + stream-stream joins (issue #17)")

    listening_df = read_stream(spark, "listening_events", LISTENING_SCHEMA)
    p2p_df       = read_stream(spark, "p2p_network_events", P2P_SCHEMA)

    # Jointure stream-static avec catalogue
    enriched_df = enrich_with_catalog(listening_df, spark)

    # Jointure stream-stream avec P2P events
    final_df = join_with_p2p_events(enriched_df, p2p_df)

    # Écriture Kafka + Parquet
    query = write_to_kafka_and_parquet(final_df)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
