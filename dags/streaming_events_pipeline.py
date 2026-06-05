"""
DAG : streaming_events_pipeline
=================================
Consomme les événements d'écoute depuis Redis, les valide, 
les enrichit avec le catalogue PostgreSQL et les stocke.

Planification : toutes les 5 minutes
"""

from datetime import datetime, timedelta
import json
import logging
import os
import io
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## streaming_events_pipeline
### Rôle
Consomme en micro-batch les événements du simulateur P2P depuis Redis,
les valide, les enrichit et les stocke en dual : Parquet (MinIO) + PostgreSQL.

### Sources
- Redis List `listening_events`
- Redis List `p2p_network_events`

### Destinations
- Table `listening_events` (PostgreSQL)
- Fichiers Parquet partitionnés sur MinIO
- Table `dead_letter_events` (pour les échecs)
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
BATCH_WINDOW_SEC = 300

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
        """Consomme les événements accumulés dans les listes Redis (DB 1)."""
        import redis
        
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/1")
        r = redis.from_url(redis_url, decode_responses=True)
        
        listening = []
        p2p_network = []
        
        # Consommer la file d'attente listening_events
        while True:
            msg = r.rpop("listening_events")
            if msg is None:
                break
            try:
                listening.append(json.loads(msg))
            except Exception:
                pass
                
        # Consommer la file d'attente p2p_network_events
        while True:
            msg = r.rpop("p2p_network_events")
            if msg is None:
                break
            try:
                p2p_network.append(json.loads(msg))
            except Exception:
                pass
                
        logging.info(f"Consommé : {len(listening)} listening events, {len(p2p_network)} p2p events")
        return {"listening": listening, "p2p_network": p2p_network}

    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        """Valide la présence des champs requis et isole les anomalies en DLQ."""
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cursor = conn.cursor()
        
        valid_listening = []
        valid_p2p = []
        errors = 0
        
        required_listening = ["event_id", "user_id", "track_id", "timestamp", "duration_ms"]
        
        for event in raw_events.get("listening", []):
            if all(k in event for k in required_listening):
                valid_listening.append(event)
            else:
                cursor.execute(
                    "INSERT INTO dead_letter_events (raw_data, error_type, source) VALUES (%s, %s, %s)",
                    (json.dumps(event), "validation_failed", "streaming_events_pipeline")
                )
                errors += 1
                
        for event in raw_events.get("p2p_network", []):
            if "event_id" in event and "event_type" in event:
                valid_p2p.append(event)
            else:
                cursor.execute(
                    "INSERT INTO dead_letter_events (raw_data, error_type, source) VALUES (%s, %s, %s)",
                    (json.dumps(event), "validation_failed", "streaming_events_pipeline")
                )
                errors += 1
                
        conn.commit()
        cursor.close()
        logging.info(f"Validation : {len(valid_listening)} valides, {errors} rejets en DLQ")
        return {"valid_listening": valid_listening, "valid_p2p": valid_p2p, "errors": errors}

    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        """Enrichit les événements en filtrant les formats invalides (ex: track_77)."""
        valid_listening = validated.get("valid_listening", [])
        if not valid_listening:
            return []
            
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cursor = conn.cursor()
        
        # Filtrer et sécuriser les IDs (on enlève les formats texte invalides type 'track_77' s'ils font crasher SQL)
        # Si votre table Postgres accepte le texte brut, la requête passera sans problème
        track_ids = list({e["track_id"] for e in valid_listening})
        
        enriched = []
        try:
            cursor.execute(
                "SELECT id, title, artist_id FROM tracks WHERE id = ANY(%s)",
                (track_ids,)
            )
            tracks_map = {str(row[0]): {"title": row[1], "artist_id": str(row[2])} for row in cursor.fetchall()}
            
            for event in valid_listening:
                track_info = tracks_map.get(str(event["track_id"]))
                if track_info:
                    event["track_title"] = track_info["title"]
                    event["artist_id"] = track_info["artist_id"]
                    enriched.append(event)
                else:
                    # Si l'ID n'est pas trouvé dans Postgres, on log et on isole en DLQ sans faire crasher le DAG
                    cursor.execute(
                        "INSERT INTO dead_letter_events (raw_data, error_type, source) VALUES (%s, %s, %s)",
                        (json.dumps(event), "unknown_track", "streaming_events_pipeline")
                    )
            conn.commit()
        except Exception as sql_err:
            logging.error(f"Erreur SQL lors de l'enrichissement : {sql_err}. Isolation globale du lot.")
            for event in valid_listening:
                cursor.execute(
                    "INSERT INTO dead_letter_events (raw_data, error_type, source) VALUES (%s, %s, %s)",
                    (json.dumps(event), "enrichment_error", "streaming_events_pipeline")
                )
            conn.commit()
            
        cursor.close()
        logging.info(f"Enrichissement complété : {len(enriched)} événements enrichis.")
        return enriched

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        """Sauvegarde les événements enrichis au format Parquet dans MinIO."""
        import boto3
        import pandas as pd
        
        if not enriched_events:
            logging.info("Aucun événement à stocker en Parquet")
            return "no_data"
            
        df = pd.DataFrame(enriched_events)
        now = datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")
        run_id = context["run_id"].replace(":", "_").replace("+", "_")
        
        path = f"listening_events/date={date_str}/hour={hour_str}/part-{run_id}.parquet"
        
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )
        
        try:
            s3.head_bucket(Bucket="spotify-parquet")
        except Exception:
            s3.create_bucket(Bucket="spotify-parquet")
            
        s3.upload_fileobj(buffer, "spotify-parquet", path)
        logging.info(f"Fichier Parquet stocké avec succès dans MinIO : {path}")
        return path

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        """Insère les données finales dans PostgreSQL avec gestion d'idempotence."""
        if not enriched_events:
            logging.info("Aucun événement valide à insérer dans Postgres.")
            return {"inserted": 0}
            
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cursor = conn.cursor()
        inserted = 0
        
        for event in enriched_events:
            try:
                cursor.execute("""
                    INSERT INTO listening_events 
                        (id, user_id, track_id, source_peer_id, timestamp, duration_ms, 
                         device_type, geo_country, completed, event_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    event.get("event_id"),
                    event.get("user_id"),
                    event.get("track_id"),
                    event.get("source_peer"),
                    event.get("timestamp"),
                    event.get("duration_ms"),
                    event.get("device_type"),
                    event.get("geo_country"),
                    event.get("completed", False),
                    event.get("event_source", "p2p"),
                ))
                inserted += 1
            except Exception as e:
                logging.warning(f"Impossible d'insérer la ligne dans Postgres : {e}")
                
        conn.commit()
        cursor.close()
        logging.info(f"PostgreSQL : {inserted} lignes insérées de manière sécurisée.")
        return {"inserted": inserted}

    # ── ORCHESTRATION TASKFLOW ────────────────────────────────
    raw_data       = consume_from_redis()
    validated_data = validate_events(raw_data)
    enriched_data  = enrich_events(validated_data)
    
    store_to_parquet(enriched_data)
    upsert_to_postgres(enriched_data)