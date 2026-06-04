"""
Job #17 - streaming_enrichment_job
Enrichit les événements Kafka avec les métadonnées tracks/artists depuis PostgreSQL
et écrit dans la table listening_events_enriched
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os

KAFKA_BOOTSTRAP = "kafka-1:9092"
TOPIC = "listening_events"
CHECKPOINT_PATH = "/tmp/spark-checkpoints/streaming_enrichment"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/spotify"
POSTGRES_PROPS = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",
}

def main():
    spark = SparkSession.builder \
        .appName("SPOTIFY-streaming-enrichment") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("Demarrage streaming_enrichment_job...")

    # Schéma des événements Kafka
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("user_id", StringType()),
        StructField("track_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("duration_ms", IntegerType()),
        StructField("source", StringType()),
    ])

    # Lecture Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    events_df = raw_df.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    ).select("data.*") \
     .withColumn("timestamp", F.to_timestamp("timestamp"))

    # Chargement référentiel tracks depuis PostgreSQL
    tracks_df = spark.read.jdbc(
        url=POSTGRES_URL,
        table="tracks",
        properties=POSTGRES_PROPS
    ).select("id", "title", "genre", "duration_ms", "artist_id") \
     .withColumnRenamed("id", "track_id_ref") \
     .withColumnRenamed("duration_ms", "track_duration_ms")

    artists_df = spark.read.jdbc(
        url=POSTGRES_URL,
        table="artists",
        properties=POSTGRES_PROPS
    ).select("id", "name", "label") \
     .withColumnRenamed("id", "artist_id_ref")

    def enrich_batch(batch_df, batch_id):
        try:
            if batch_df.count() == 0:
                return

            # Jointure avec tracks
            enriched = batch_df.join(
                tracks_df,
                batch_df.track_id == tracks_df.track_id_ref,
                "left"
            ).join(
                artists_df,
                tracks_df.artist_id == artists_df.artist_id_ref,
                "left"
            ).select(
                batch_df.event_id,
                batch_df.user_id,
                batch_df.track_id,
                tracks_df.title.alias("track_title"),
                tracks_df.genre,
                artists_df.name.alias("artist_name"),
                artists_df.label,
                batch_df.timestamp,
                batch_df.duration_ms,
                batch_df.source,
            ).dropDuplicates(["event_id"])

            enriched.write.jdbc(
                url=POSTGRES_URL,
                table="listening_events",
                mode="append",
                properties=POSTGRES_PROPS
            )
            print(f"[batch {batch_id}] enrichment: {enriched.count()} événements enrichis")
        except Exception as e:
            print(f"[batch {batch_id}] Erreur enrichment: {e}")

    query = events_df.writeStream \
        .outputMode("append") \
        .foreachBatch(enrich_batch) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
