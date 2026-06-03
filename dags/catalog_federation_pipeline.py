"""
DAG : catalog_federation_pipeline
====================================
Publie le catalogue local dans le topic Kafka `catalog_federation`
et consomme les catalogues des autres groupes pour peupler `federated_catalog`.

Planification : quotidienne à 07:00 UTC
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## catalog_federation_pipeline

### Rôle
Phase 3 — Interconnexion inter-groupes.
Partage et consomme les catalogues musicaux entre groupes SPOTIFY.

### Sources
- Table `tracks` locale → publication dans `catalog_federation` Kafka
- Topic Kafka `catalog_federation` → consommation des autres groupes

### Destinations
- Table `federated_catalog` : tracks de tous les groupes

### Format
Utilise `contracts/catalog_federation_schema.json` (version 1.0).
Events invalides → `dead_letter_events` avec error_type=federation_schema.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"
KAFKA_BOOTSTRAP  = "kafka-1:9092"
FEDERATION_TOPIC = "catalog_federation"
SOURCE_GROUP     = "groupe-c"
BATCH_SIZE       = 200


with DAG(
    dag_id="catalog_federation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Phase 3 : partage catalogue inter-groupes via Kafka catalog_federation",
    schedule_interval="0 7 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-3", "federation", "inter-groupes"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="publish_local_catalog")
    def publish_local_catalog(**context) -> int:
        """
        Publie les tracks locaux dans le topic `catalog_federation`.
        Format : catalog_federation_schema.json v1.0.
        """
        logger = logging.getLogger(__name__)
        from confluent_kafka import Producer

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT t.id::text, a.name, t.title, t.duration_ms, t.genre, t.audio_file_path
            FROM tracks t
            JOIN artists a ON t.artist_id = a.id
            LIMIT 500
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "all",
        })

        published = 0
        for row in rows:
            track_id, artist_name, title, duration_ms, genre, audio_path = row
            msg = {
                "track_id":           track_id,
                "source_group":       SOURCE_GROUP,
                "artist_name":        artist_name,
                "track_title":        title,
                "duration_ms":        duration_ms or 0,
                "genre":              genre,
                "audio_peer_endpoint": audio_path,
                "published_at":       datetime.utcnow().isoformat() + "Z",
                "schema_version":     "1.0",
            }
            producer.produce(FEDERATION_TOPIC, key=track_id, value=json.dumps(msg))
            published += 1
            if published % BATCH_SIZE == 0:
                producer.flush()

        producer.flush()
        logger.info("Catalogue publié dans %s : %d tracks", FEDERATION_TOPIC, published)
        return published

    @task(task_id="consume_federated_catalogs")
    def consume_federated_catalogs(**context) -> dict:
        """
        Consomme le topic `catalog_federation`, filtre les tracks des autres groupes,
        valide le schéma et insère dans `federated_catalog`.
        """
        logger = logging.getLogger(__name__)
        try:
            from kafka import KafkaConsumer
        except ImportError:
            logger.warning("kafka-python non installé — return")
            return {"inserted": 0, "rejected": 0}

        consumer = KafkaConsumer(
            FEDERATION_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=10_000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id=f"airflow-federation-{SOURCE_GROUP}",
        )

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        upsert_sql = """
            INSERT INTO federated_catalog
                (track_id, source_group, artist_name, track_title, duration_ms, genre,
                 audio_peer_endpoint, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (track_id, source_group) DO UPDATE SET
                artist_name        = EXCLUDED.artist_name,
                track_title        = EXCLUDED.track_title,
                duration_ms        = EXCLUDED.duration_ms,
                genre              = EXCLUDED.genre,
                audio_peer_endpoint= EXCLUDED.audio_peer_endpoint,
                ingested_at        = NOW()
        """
        dlq_sql = """
            INSERT INTO dead_letter_events (payload, error_type, error_message, original_topic)
            VALUES (%s::jsonb, %s, %s, %s)
        """

        inserted = 0
        rejected = 0
        sources  = {}

        for msg in consumer:
            track = msg.value
            source = track.get("source_group", "unknown")

            # Ignorer nos propres tracks
            if source == SOURCE_GROUP:
                continue

            # Validation schéma minimal
            required = {"track_id", "source_group", "artist_name", "track_title", "duration_ms"}
            missing  = required - set(track.keys())
            if missing:
                cur.execute(dlq_sql, (
                    json.dumps(track), "federation_schema",
                    f"Missing: {missing}", FEDERATION_TOPIC,
                ))
                rejected += 1
                continue

            cur.execute(upsert_sql, (
                track["track_id"], source,
                track["artist_name"], track["track_title"],
                int(track.get("duration_ms", 0)),
                track.get("genre"),
                track.get("audio_peer_endpoint"),
            ))
            inserted += 1
            sources[source] = sources.get(source, 0) + 1

        conn.commit()
        consumer.commit()
        consumer.close()
        cur.close()

        logger.info("Fédération : %d insérés, %d rejetés | sources : %s",
                    inserted, rejected, sources)
        return {"inserted": inserted, "rejected": rejected, "sources": sources}

    # ── Orchestration ─────────────────────────────────────────
    published = publish_local_catalog()
    consume_federated_catalogs()
