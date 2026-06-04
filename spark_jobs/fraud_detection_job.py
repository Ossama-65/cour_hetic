"""
Job #18 - fraud_detection_job
Détection de fraude stateful avec flatMapGroupsWithState
Détecte les bots : > 10 écoutes en 60 secondes pour un même user
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.streaming.state import GroupStateTimeout, GroupState
import json

KAFKA_BOOTSTRAP = "kafka-1:9092"
TOPIC = "listening_events"
CHECKPOINT_PATH = "/tmp/spark-checkpoints/fraud_detection"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/spotify"
POSTGRES_PROPS = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",
}

# Seuils de détection
MAX_EVENTS_PER_MINUTE = 10
FRAUD_WINDOW_SECONDS = 60

def main():
    spark = SparkSession.builder \
        .appName("SPOTIFY-fraud-detection") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("Demarrage fraud_detection_job...")

    schema = StructType([
        StructField("event_id", StringType()),
        StructField("user_id", StringType()),
        StructField("track_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("duration_ms", IntegerType()),
        StructField("source", StringType()),
    ])

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
     .withColumn("event_time", F.to_timestamp("timestamp")) \
     .withWatermark("event_time", "2 minutes")

    # Agrégation par user sur fenêtre glissante 1 minute
    windowed = events_df.groupBy(
        F.window("event_time", "1 minute", "30 seconds"),
        F.col("user_id")
    ).agg(
        F.count("event_id").alias("event_count"),
        F.collect_list("track_id").alias("track_ids")
    ).withColumn("window_start", F.col("window.start")) \
     .withColumn("window_end", F.col("window.end")) \
     .drop("window")

    # Filtre : users suspects
    fraud_df = windowed.filter(F.col("event_count") > MAX_EVENTS_PER_MINUTE) \
        .withColumn("fraud_score", F.col("event_count") / MAX_EVENTS_PER_MINUTE) \
        .withColumn("fraud_reason", F.lit(f">{MAX_EVENTS_PER_MINUTE} events/min")) \
        .withColumn("detected_at", F.current_timestamp())

    def write_fraud(batch_df, batch_id):
        try:
            if batch_df.count() == 0:
                return
            # Log des fraudes détectées
            fraud_rows = batch_df.select(
                "user_id", "event_count", "fraud_score",
                "fraud_reason", "window_start", "window_end", "detected_at"
            )
            fraud_rows.show(truncate=False)
            count = fraud_rows.count()
            print(f"[batch {batch_id}] fraud_detection: {count} utilisateurs suspects détectés")

            # Écriture dans DLQ pour retraitement
            if count > 0:
                fraud_rows.write.jdbc(
                    url=POSTGRES_URL,
                    table="dead_letter_events",
                    mode="append",
                    properties={
                        **POSTGRES_PROPS,
                        "stringtype": "unspecified",
                    }
                )
        except Exception as e:
            print(f"[batch {batch_id}] Erreur fraud_detection: {e}")

    query = fraud_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_fraud) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
