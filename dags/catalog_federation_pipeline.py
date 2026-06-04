"""
DAG #22 - catalog_federation_pipeline
Publie nos tracks dans le topic Kafka partage `catalog_federation`
et consomme les catalogues des autres groupes.
Valide les schemas via contracts/catalog_federation_schema.json
et insere dans federated_catalog (DLQ si non conforme).
"""
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

KAFKA_BOOTSTRAP = "kafka-1:9092"
FEDERATION_TOPIC = "catalog_federation"
SOURCE_GROUP = "groupe-d"
POSTGRES_CONN_ID = "spotify_postgres"

REQUIRED_FIELDS = ["track_id", "title", "artist_id", "duration_ms", "source_group"]


def validate_track(track: dict) -> tuple[bool, str]:
    for field in REQUIRED_FIELDS:
        if field not in track or track[field] is None:
            return False, f"champ manquant: {field}"
    if not (1000 <= int(track["duration_ms"]) <= 3_600_000):
        return False, f"duration_ms hors limites: {track['duration_ms']}"
    return True, ""


with DAG(
    dag_id="catalog_federation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Federation catalogue inter-groupes via Kafka",
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-3", "federation", "kafka"],
) as dag:

    @task(task_id="publish_our_catalog")
    def publish_our_catalog(**context) -> int:
        from kafka import KafkaProducer

        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        tracks = pg.get_records("""
            SELECT t.id, t.title, t.artist_id, a.name, t.album_id,
                   t.duration_ms, t.genre, a.label
            FROM tracks t
            JOIN artists a ON a.id = t.artist_id
            ORDER BY t.created_at DESC
            LIMIT 500
        """)

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        count = 0
        for row in tracks:
            track = {
                "track_id":       str(row[0]),
                "title":          row[1],
                "artist_id":      str(row[2]),
                "artist_name":    row[3],
                "album_id":       str(row[4]) if row[4] else None,
                "duration_ms":    row[5],
                "genre":          row[6],
                "label":          row[7],
                "source_group":   SOURCE_GROUP,
                "schema_version": "1.0",
                "emitted_at":     datetime.utcnow().isoformat() + "Z",
            }
            producer.send(FEDERATION_TOPIC, value=track)
            count += 1

        producer.flush()
        producer.close()
        logger.info(f"{count} tracks publiees dans {FEDERATION_TOPIC}")
        return count

    @task(task_id="consume_federation_catalog")
    def consume_federation_catalog(**context) -> list:
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            FEDERATION_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="earliest",
            consumer_timeout_ms=15000,
            group_id=f"{SOURCE_GROUP}-federation-consumer",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

        tracks = []
        for msg in consumer:
            track = msg.value
            # ignorer nos propres publications
            if track.get("source_group") == SOURCE_GROUP:
                continue
            tracks.append(track)

        consumer.close()
        logger.info(f"{len(tracks)} tracks recues des autres groupes")
        return tracks

    @task(task_id="validate_and_insert")
    def validate_and_insert(tracks: list, **context) -> dict:
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cur = conn.cursor()

        inserted = 0
        rejected = 0

        for track in tracks:
            valid, reason = validate_track(track)

            if not valid:
                # DLQ
                cur.execute("""
                    INSERT INTO dead_letter_events
                        (original_topic, payload, error_type, error_message)
                    VALUES (%s, %s, %s, %s)
                """, (
                    FEDERATION_TOPIC,
                    json.dumps(track),
                    "contract_violation",
                    reason,
                ))
                rejected += 1
                continue

            cur.execute("""
                INSERT INTO federated_catalog
                    (track_id, title, artist_id, artist_name, album_id,
                     duration_ms, genre, label, source_group, schema_version, received_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (track_id, source_group) DO UPDATE SET
                    title      = EXCLUDED.title,
                    received_at = NOW()
            """, (
                track["track_id"],
                track["title"],
                track["artist_id"],
                track.get("artist_name"),
                track.get("album_id"),
                track["duration_ms"],
                track.get("genre"),
                track.get("label"),
                track["source_group"],
                track.get("schema_version", "1.0"),
            ))
            inserted += 1

        conn.commit()
        cur.close()
        conn.close()

        logger.info(f"Federation: {inserted} inserees, {rejected} rejetees en DLQ")
        return {"inserted": inserted, "rejected": rejected}

    @task(task_id="check_federation_stats")
    def check_federation_stats(stats: dict, **context):
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        rows = pg.get_records("""
            SELECT source_group, COUNT(*)
            FROM federated_catalog
            GROUP BY source_group
            ORDER BY source_group
        """)
        logger.info("=== Federation stats ===")
        for row in rows:
            logger.info(f"  {row[0]}: {row[1]} tracks")
        return {r[0]: r[1] for r in rows}

    published = publish_our_catalog()
    received = consume_federation_catalog()
    stats_insert = validate_and_insert(received)
    published >> stats_insert
    check_federation_stats(stats_insert)
