"""
DAG : streaming_events_pipeline
=================================
Consomme les événements d'écoute depuis Redis,
les valide, les enrichit avec le catalogue et les stocke.
"""

from datetime import datetime, timedelta
import json
import os
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configuration identique
DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_CHANNELS = ["listening_events", "p2p_network_events"]

with DAG(
    dag_id="streaming_events_pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "events", "streaming"],
) as dag:

    @task(task_id="consume_from_redis")
    def consume_from_redis(**context) -> dict:
        import redis
        redis_host = os.getenv("REDIS_HOST", "redis")
        r = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)
        events = {"listening": [], "p2p_network": []}
        for channel in REDIS_CHANNELS:
            messages = []
            while True:
                msg = r.rpop(channel)
                if not msg: break
                messages.append(json.loads(msg))
            key = "listening" if channel == "listening_events" else "p2p_network"
            events[key] = messages
        return events

    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        valid_listening = []
        valid_p2p = []
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Validation Listening : error_message (selon ton schéma)
        for ev in raw_events.get("listening", []):
            if all(k in ev for k in ["event_id", "user_id", "track_id", "timestamp"]):
                valid_listening.append(ev)
            else:
                hook.run(
                    "INSERT INTO dead_letter_events (error_message, payload) VALUES (%s, %s)",
                    parameters=("validation_error", json.dumps(ev))
                )

        for ev in raw_events.get("p2p_network", []):
            if "event_id" in ev: valid_p2p.append(ev)
            else:
                hook.run(
                    "INSERT INTO dead_letter_events (error_message, payload) VALUES (%s, %s)",
                    parameters=("p2p_validation_error", json.dumps(ev))
                )

        return {"valid_listening": valid_listening, "valid_p2p": valid_p2p}

    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        valid_listening = validated.get("valid_listening", [])
        if not valid_listening: return []

        track_ids = list(set(ev["track_id"] for ev in valid_listening))
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM tracks WHERE id = ANY(%s)", (track_ids,))
        catalog = [row[0] for row in cursor.fetchall()]
        cursor.close()
        connection.close()

        enriched_list = []
        for ev in valid_listening:
            if ev["track_id"] in catalog:
                enriched_list.append(ev)
            else:
                # Correction du nom de la colonne : error_message
                hook.run(
                    "INSERT INTO dead_letter_events (error_message, payload) VALUES (%s, %s)",
                    parameters=("unknown_track", json.dumps(ev))
                )
        return enriched_list
    
    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        if not enriched_events: return "Aucune donnée"
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        import boto3

        df = pd.DataFrame(enriched_events)
        df['datetime'] = pd.to_datetime(df['timestamp'])
        df['date'] = df['datetime'].dt.strftime('%Y-%m-%d')
        df['hour'] = df['datetime'].dt.strftime('%H')
        
        s3 = boto3.client('s3', endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
                          aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
                          aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))

        for _, row in df[['date', 'hour']].drop_duplicates().iterrows():
            sub_df = df[(df['date'] == row['date']) & (df['hour'] == row['hour'])]
            table = pa.Table.from_pandas(sub_df, preserve_index=False)
            sink = pa.BufferOutputStream()
            pq.write_table(table, sink)
            s3.put_object(Bucket='spotify-parquet', 
                          Key=f"listening_events/date={row['date']}/hour={row['hour']}/part-{context['run_id']}.parquet",
                          Body=sink.getvalue().to_pybytes())
        return "Stockage MinIO OK"

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        if not enriched_events: return {"inserted": 0}
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        records = [(ev["event_id"], ev["user_id"], ev["track_id"], ev["timestamp"]) for ev in enriched_events]
        query = "INSERT INTO listening_events (id, user_id, track_id, timestamp) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING"
        
        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.executemany(query, records)
        connection.commit()
        cursor.close()
        connection.close()
        return {"processed": len(records)}

    # Orchestration
    raw = consume_from_redis()
    validated = validate_events(raw)
    enriched = enrich_events(validated)
    store_to_parquet(enriched)
    upsert_to_postgres(enriched)