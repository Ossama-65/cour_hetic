"""
DAG : streaming_events_pipeline
=================================
Consomme les événements d'écoute depuis Redis (LIST *_queue),
les valide, les enrichit avec le catalogue et les stocke.

Planification : toutes les 5 minutes
Catchup       : désactivé (micro-batch temps réel)

Architecture (2 branches après validate_events) :
    Redis LIST listening_events_queue + p2p_network_events_queue
        → consume_from_redis()
        → validate_events()
              ├─ Branche listening → enrich_events()
              │       ├─ store_to_parquet()      (MinIO spotify-parquet/listening_events/)
              │       └─ upsert_to_postgres()    (table listening_events)
              └─ Branche P2P      → store_p2p_to_parquet()
                                                 (MinIO spotify-parquet/p2p_network_events/)
"""

import io
import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## streaming_events_pipeline

### Rôle
Consomme en micro-batch les événements du simulateur P2P depuis Redis,
les valide, les enrichit et les stocke en dual : Parquet (MinIO) + PostgreSQL.

### Sources
- Redis LIST `listening_events_queue`
- Redis LIST `p2p_network_events_queue`

### Destinations
- Table `listening_events` (PostgreSQL) — upsert idempotent via ON CONFLICT (id) DO NOTHING
- Fichiers Parquet partitionnés sur MinIO : `s3://spotify-parquet/listening_events/date=.../hour=.../`
- Table `dead_letter_events` (pour les events invalides ou avec track_id inconnu)

### Idempotence
Chaque event est identifié par `event_id` (UUID). L'upsert utilise
`ON CONFLICT (id) DO NOTHING` pour éviter les doublons.

### Branches
- listening_events  : validation + enrichissement catalogue + Parquet + PostgreSQL
- p2p_network_events : validation uniquement (pas d'enrichissement catalogue)
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

POSTGRES_CONN_ID = "spotify_postgres"

REQUIRED_LISTENING_FIELDS = {"event_id", "user_id", "track_id", "timestamp", "duration_ms"}
REQUIRED_P2P_FIELDS       = {"event_id", "event_type", "peer_id", "timestamp"}


with DAG(
    dag_id="streaming_events_pipeline",
    default_args=DEFAULT_ARGS,
    description="Micro-batch : Redis → validation → enrichissement → MinIO + PostgreSQL",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "events", "streaming"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_from_redis")
    def consume_from_redis(**context) -> dict:
        """
        Draine les LISTs Redis listening_events_queue et p2p_network_events_queue.
        Utilise un pipeline atomique (lrange + delete) pour éviter les doublons.

        Returns:
            dict: {"listening": [...], "p2p_network": [...]}
        """
        import redis as redis_lib

        logger = logging.getLogger(__name__)

        r = redis_lib.from_url(
            os.environ.get("REDIS_URL", "redis://redis:6379/1"),
            decode_responses=True,
        )

        def drain_queue(queue_name: str) -> list:
            pipe = r.pipeline()
            pipe.lrange(queue_name, 0, -1)
            pipe.delete(queue_name)
            results = pipe.execute()
            raw_events = results[0] or []
            events = []
            for raw in raw_events:
                try:
                    events.append(json.loads(raw))
                except json.JSONDecodeError as exc:
                    logger.warning("Skipping malformed JSON in %s: %s", queue_name, exc)
            return events

        listening_events = drain_queue("listening_events_queue")
        p2p_events       = drain_queue("p2p_network_events_queue")

        logger.info(
            "Consumed from Redis — listening: %d, p2p: %d",
            len(listening_events), len(p2p_events),
        )
        return {"listening": listening_events, "p2p_network": p2p_events}

    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        """
        Valide les champs obligatoires et les types de chaque événement.
        Les invalides sont routés en DLQ (dead_letter_events).

        Returns:
            dict: {"valid_listening": [...], "valid_p2p": [...], "errors": N}
        """
        logger = logging.getLogger(__name__)
        hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        dlq_sql = """
            INSERT INTO dead_letter_events
                (payload, error_type, error_message, original_topic)
            VALUES (%s::jsonb, %s, %s, %s)
        """

        valid_listening, valid_p2p = [], []
        errors = 0

        for event in raw_events.get("listening", []):
            missing = REQUIRED_LISTENING_FIELDS - set(event.keys())
            if missing:
                hook.run(dlq_sql, parameters=(
                    json.dumps(event), "validation",
                    f"Missing fields: {missing}", "listening_events_queue",
                ))
                errors += 1
                continue
            # Vérification du type de duration_ms
            if not isinstance(event.get("duration_ms"), (int, float)) or event["duration_ms"] <= 0:
                hook.run(dlq_sql, parameters=(
                    json.dumps(event), "validation",
                    "Invalid duration_ms", "listening_events_queue",
                ))
                errors += 1
                continue
            valid_listening.append(event)

        for event in raw_events.get("p2p_network", []):
            missing = REQUIRED_P2P_FIELDS - set(event.keys())
            if missing:
                hook.run(dlq_sql, parameters=(
                    json.dumps(event), "validation",
                    f"Missing fields: {missing}", "p2p_network_events_queue",
                ))
                errors += 1
                continue
            valid_p2p.append(event)

        logger.info(
            "Validated — listening: %d, p2p: %d, errors: %d",
            len(valid_listening), len(valid_p2p), errors,
        )
        return {
            "valid_listening": valid_listening,
            "valid_p2p":       valid_p2p,
            "errors":          errors,
        }

    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        """
        Enrichit les listening_events avec les métadonnées du catalogue
        (track_title, artist_id, genre) via une requête batch sur PostgreSQL.
        Les track_id inconnus sont routés en DLQ avec error_type="unknown_track".

        Returns:
            list[dict]: événements d'écoute enrichis
        """
        logger = logging.getLogger(__name__)
        hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        events = validated.get("valid_listening", [])
        if not events:
            return []

        # Récupérer tous les track_ids uniques en une seule requête
        track_ids = list({e["track_id"] for e in events})
        conn = hook.get_conn()
        cur  = conn.cursor()
        cur.execute(
            "SELECT id::text, title, artist_id::text, genre FROM tracks WHERE id = ANY(%s::uuid[])",
            (track_ids,)
        )
        catalog = {row[0]: {"title": row[1], "artist_id": row[2], "genre": row[3]}
                   for row in cur.fetchall()}
        cur.close()
        conn.close()

        dlq_sql = """
            INSERT INTO dead_letter_events
                (payload, error_type, error_message, original_topic)
            VALUES (%s::jsonb, %s, %s, %s)
        """

        enriched = []
        unknown  = 0

        for event in events:
            track_info = catalog.get(event["track_id"])
            if track_info is None:
                hook.run(dlq_sql, parameters=(
                    json.dumps(event), "unknown_track",
                    f"track_id {event['track_id']} not found in catalog",
                    "listening_events_queue",
                ))
                unknown += 1
                continue
            event["track_title"] = track_info["title"]
            event["artist_id"]   = track_info["artist_id"]
            event["genre"]       = track_info["genre"]
            enriched.append(event)

        logger.info(
            "Enriched — valid: %d, unknown_track: %d",
            len(enriched), unknown,
        )
        return enriched

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        """
        Sauvegarde les événements enrichis en Parquet sur MinIO.
        Partitionnement par date et heure du timestamp.

        Returns:
            str: liste des chemins S3 écrits (ou "no_events")
        """
        import boto3
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        logger = logging.getLogger(__name__)

        if not enriched_events:
            logger.info("No events to store.")
            return "no_events"

        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        )

        df = pd.DataFrame(enriched_events)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["_date"]     = df["timestamp"].dt.strftime("%Y-%m-%d")
        df["_hour"]     = df["timestamp"].dt.hour

        dag_run = context["dag_run"]
        run_id  = dag_run.run_id.replace(":", "_").replace("+", "_").replace(" ", "_")

        paths = []
        for (date, hour), group in df.groupby(["_date", "_hour"]):
            export = group.drop(columns=["_date", "_hour"]).copy()
            export["timestamp"] = export["timestamp"].astype(str)
            table = pa.Table.from_pandas(export, preserve_index=False)

            buf = io.BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)

            key = f"listening_events/date={date}/hour={int(hour):02d}/part-{run_id}.parquet"
            s3.upload_fileobj(buf, "spotify-parquet", key)
            paths.append(f"s3://spotify-parquet/{key}")

        logger.info("Stored %d partition(s) to MinIO", len(paths))
        return str(paths)

    @task(task_id="store_p2p_to_parquet")
    def store_p2p_to_parquet(validated: dict, **context) -> str:
        """
        Branche P2P : sauvegarde les p2p_network_events validés en Parquet sur MinIO.
        Partitionnement séparé : s3://spotify-parquet/p2p_network_events/date=.../hour=.../

        Returns:
            str: liste des chemins S3 écrits (ou "no_events")
        """
        import boto3
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        logger = logging.getLogger(__name__)

        p2p_events = validated.get("valid_p2p", [])
        if not p2p_events:
            logger.info("No p2p events to store.")
            return "no_events"

        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        )

        dag_run = context["dag_run"]
        run_id  = dag_run.run_id.replace(":", "_").replace("+", "_").replace(" ", "_")

        df = pd.DataFrame(p2p_events)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df["_date"]     = df["timestamp"].dt.strftime("%Y-%m-%d")
        df["_hour"]     = df["timestamp"].dt.hour

        paths = []
        for (date, hour), group in df.groupby(["_date", "_hour"]):
            export = group.drop(columns=["_date", "_hour"]).copy()
            export["timestamp"] = export["timestamp"].astype(str)
            table = pa.Table.from_pandas(export, preserve_index=False)

            buf = io.BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)

            key = f"p2p_network_events/date={date}/hour={int(hour):02d}/part-{run_id}.parquet"
            s3.upload_fileobj(buf, "spotify-parquet", key)
            paths.append(f"s3://spotify-parquet/{key}")

        logger.info("P2P stored — %d partition(s) to MinIO", len(paths))
        return str(paths)

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        """
        Insère les événements enrichis dans PostgreSQL de façon idempotente.
        ON CONFLICT (id) DO NOTHING garantit l'idempotence.

        Returns:
            dict: {"inserted": N, "skipped": M}
        """
        logger = logging.getLogger(__name__)

        if not enriched_events:
            return {"inserted": 0, "skipped": 0}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        sql = """
            INSERT INTO listening_events
                (id, user_id, track_id, timestamp, duration_ms,
                 device_type, geo_country, completed, event_source, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO NOTHING
        """

        inserted = 0
        skipped  = 0

        for event in enriched_events:
            try:
                cur.execute(sql, (
                    event["event_id"],
                    event["user_id"],
                    event["track_id"],
                    event["timestamp"],
                    int(event.get("duration_ms", 0)),
                    event.get("device_type"),
                    event.get("geo_country"),
                    bool(event.get("completed", False)),
                    event.get("event_source", "p2p"),
                ))
                if cur.rowcount == 1:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as exc:
                logger.warning("Could not insert event %s : %s", event.get("event_id"), exc)
                conn.rollback()
                skipped += 1

        conn.commit()
        cur.close()

        context["ti"].xcom_push(key="events_inserted", value=inserted)
        logger.info("Upsert — inserted: %d, skipped: %d", inserted, skipped)
        return {"inserted": inserted, "skipped": skipped}

    # ── Orchestration ─────────────────────────────────────────
    raw       = consume_from_redis()
    validated = validate_events(raw)

    # Branche listening : enrichissement → Parquet + PostgreSQL
    enriched = enrich_events(validated)
    store_to_parquet(enriched)
    upsert_to_postgres(enriched)

    # Branche P2P : stockage direct en Parquet (pas de jointure catalogue)
    store_p2p_to_parquet(validated)
