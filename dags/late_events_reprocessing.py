"""
DAG : late_events_reprocessing
================================
Retraite périodiquement les événements tardifs routés par Spark vers
le topic Kafka `late_listening_events`.

Planification : toutes les heures
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## late_events_reprocessing

### Rôle
Consomme le topic Kafka `late_listening_events` (mode availableNow),
revalide les événements, et les insère dans `listening_events`.
Recalcule ensuite les agrégats `daily_streams` pour les dates affectées.

### Sources
- Topic Kafka `late_listening_events` (routé par streaming_trends_job)

### Destinations
- Table `listening_events` (insert si valide)
- Table `daily_streams` (recalcul incrémental des dates affectées)

### Planification
`@hourly` — consomme les offsets disponibles à chaque run (availableNow)
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID = "spotify_postgres"
KAFKA_BOOTSTRAP  = "kafka-1:9092"
LATE_TOPIC       = "late_listening_events"
REQUIRED_FIELDS  = {"event_id", "user_id", "track_id", "timestamp", "duration_ms"}


with DAG(
    dag_id="late_events_reprocessing",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des late events depuis Kafka vers listening_events",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-2", "late-events", "resilience"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_late_events")
    def consume_late_events(**context) -> list:
        """
        Consomme le topic late_listening_events en mode availableNow
        (one-shot : consomme uniquement les offsets disponibles au moment du run).
        Utilise kafka-python (plus simple pour un micro-batch Airflow).
        """
        logger = logging.getLogger(__name__)
        try:
            from kafka import KafkaConsumer
        except ImportError:
            logger.warning("kafka-python non installé — return []")
            return []

        consumer = KafkaConsumer(
            LATE_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=5_000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="airflow-late-events-reprocessing",
        )

        events = []
        for msg in consumer:
            events.append(msg.value)
            if len(events) >= 1000:
                break

        consumer.commit()
        consumer.close()

        logger.info("Late events consommés depuis Kafka : %d", len(events))
        return events

    @task(task_id="validate_and_insert")
    def validate_and_insert(late_events: list, **context) -> dict:
        """
        Revalide les late events et insère les valides dans listening_events.
        Retourne les dates affectées pour le recalcul des agrégats.
        """
        logger = logging.getLogger(__name__)

        if not late_events:
            logger.info("Aucun late event à retraiter.")
            return {"inserted": 0, "rejected": 0, "affected_dates": []}

        # Vérifier les track_ids valides
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT id::text FROM tracks")
        valid_track_ids = {row[0] for row in cur.fetchall()}

        sql = """
            INSERT INTO listening_events
                (id, user_id, track_id, timestamp, duration_ms,
                 device_type, geo_country, completed, event_source, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO NOTHING
        """

        inserted = 0
        rejected = 0
        affected_dates = set()

        for event in late_events:
            # Validation
            if not all(f in event for f in REQUIRED_FIELDS):
                rejected += 1
                continue
            if event.get("track_id") not in valid_track_ids:
                rejected += 1
                continue
            if not isinstance(event.get("duration_ms"), (int, float)) or event["duration_ms"] <= 0:
                rejected += 1
                continue

            try:
                cur.execute(sql, (
                    event["event_id"],
                    event["user_id"],
                    event["track_id"],
                    event["timestamp"],
                    int(event["duration_ms"]),
                    event.get("device_type"),
                    event.get("geo_country"),
                    bool(event.get("completed", False)),
                    event.get("event_source", "late_reprocessed"),
                ))
                if cur.rowcount == 1:
                    inserted += 1
                    # Extraire la date pour recalcul
                    ts = event["timestamp"][:10]  # YYYY-MM-DD
                    affected_dates.add(ts)
            except Exception as exc:
                logger.warning("Échec insert event %s : %s", event.get("event_id"), exc)
                conn.rollback()
                rejected += 1

        conn.commit()
        cur.close()

        logger.info("Late events : %d insérés, %d rejetés, dates affectées : %s",
                    inserted, rejected, list(affected_dates))

        return {
            "inserted":       inserted,
            "rejected":       rejected,
            "affected_dates": list(affected_dates),
        }

    @task(task_id="recalculate_aggregates")
    def recalculate_aggregates(results: dict, **context) -> dict:
        """
        Recalcule daily_streams pour les dates affectées par les late events insérés.
        Upsert idempotent via ON CONFLICT.
        """
        logger = logging.getLogger(__name__)
        affected_dates = results.get("affected_dates", [])

        if not affected_dates or results.get("inserted", 0) == 0:
            logger.info("Aucune date à recalculer.")
            return {"recalculated_dates": 0}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        for date_str in affected_dates:
            cur.execute("""
                INSERT INTO daily_streams
                    (track_id, date, total_streams, unique_listeners, total_duration_ms, updated_at)
                SELECT
                    le.track_id,
                    DATE(le.timestamp),
                    COUNT(*)                   AS total_streams,
                    COUNT(DISTINCT le.user_id) AS unique_listeners,
                    COALESCE(SUM(le.duration_ms), 0) AS total_duration_ms,
                    NOW()
                FROM listening_events le
                WHERE DATE(le.timestamp) = %s AND le.completed = TRUE
                GROUP BY le.track_id, DATE(le.timestamp)
                ON CONFLICT (track_id, date) DO UPDATE SET
                    total_streams     = EXCLUDED.total_streams,
                    unique_listeners  = EXCLUDED.unique_listeners,
                    total_duration_ms = EXCLUDED.total_duration_ms,
                    updated_at        = NOW()
            """, (date_str,))

        conn.commit()
        cur.close()

        logger.info("Agrégats recalculés pour %d dates : %s",
                    len(affected_dates), affected_dates)
        return {"recalculated_dates": len(affected_dates)}

    # ── Orchestration ─────────────────────────────────────────
    late_events = consume_late_events()
    results     = validate_and_insert(late_events)
    recalculate_aggregates(results)
