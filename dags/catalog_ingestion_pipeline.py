"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.

Planification : quotidienne à 02:00 UTC
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

DAG_DOC = """
## catalog_ingestion_pipeline
Ingère le catalogue musical depuis MinIO (labels JSON) vers PostgreSQL.
Idempotent : utilise ON CONFLICT DO UPDATE pour les upserts.
"""

DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_CONN_ID   = "spotify_minio"
MINIO_BUCKET    = "labels-raw"
LABEL_FILES     = ["sunset_records.json", "nightwave_music.json", "urban_pulse.json"]

with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion quotidienne du catalogue musical",
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "catalogue"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="extract_from_minio")
    def extract_from_minio() -> list:
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        catalogs = []
        for file in LABEL_FILES:
            if s3_hook.check_for_key(file, bucket_name=MINIO_BUCKET):
                file_obj = s3_hook.get_key(file, bucket_name=MINIO_BUCKET)
                data = json.loads(file_obj.get()["Body"].read().decode("utf-8"))
                catalogs.append(data)
            else:
                logging.warning(f"Fichier {file} introuvable.")
        return catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list) -> dict:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        valid_data = {"tracks": []}
        errors = 0
        for cat in raw_catalogs:
            for track in cat.get("tracks", []):
                if all(k in track for k in ["id", "artist_id", "title"]):
                    valid_data["tracks"].append(track)
                else:
                    errors += 1
                    hook.run("INSERT INTO dead_letter_events (payload, error_type) VALUES (%s, 'schema_validation')", 
                             parameters=(json.dumps(track),))
        return {"valid": valid_data, "errors": errors}

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict) -> dict:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        count = 0
        for track in transformed["valid"]["tracks"]:
            cursor.execute("""
                INSERT INTO tracks (id, artist_id, title, duration_ms)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET 
                    title = EXCLUDED.title, 
                    duration_ms = EXCLUDED.duration_ms,
                    updated_at = NOW();
            """, (track["id"], track["artist_id"], track["title"], track.get("duration_ms", 0)))
            count += 1
        conn.commit()
        cursor.close()
        return {"tracks_inserted": count}

    # Orchestration
    raw = extract_from_minio()
    validated = validate_schema(raw)
    load_to_postgres(validated)