"""
DAG : streaming_events_pipeline
=================================
Consomme les événements d'écoute depuis Redis,
les valide, les enrichit avec le catalogue et les stocke.

Planification : toutes les 5 minutes
Catchup       : désactivé (micro-batch temps réel)
"""

from datetime import datetime, timedelta
import json
import os
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## streaming_events_pipeline

### Rôle
Consomme en micro-batch les événements du simulateur P2P depuis Redis,
les valide, les enrichit et les stocke en dual : Parquet (MinIO) + PostgreSQL.

### Sources
- Redis channel `listening_events`
- Redis channel `p2p_network_events`

### Destinations
- Table `listening_events` (PostgreSQL)
- Fichiers Parquet partitionnés sur MinIO : `s3://spotify-parquet/listening_events/date=.../hour=.../`
- Table `dead_letter_events` (pour les events invalides)

### Idempotence
Chaque event est identifié par `event_id` (UUID). L'upsert utilise
`ON CONFLICT (id) DO NOTHING` pour éviter les doublons.
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
REDIS_CHANNELS   = ["listening_events", "p2p_network_events"]
BATCH_WINDOW_SEC = 300  # 5 minutes


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
        """Consomme les événements accumulés dans les listes Redis (pattern RPOP)."""
        import redis
        
        redis_host = os.getenv("REDIS_HOST", "redis")
        r = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)

        events = {"listening": [], "p2p_network": []}

        for channel in REDIS_CHANNELS:
            raw_messages = []
            while True:
                msg = r.rpop(channel)
                if not msg:
                    break
                raw_messages.append(json.loads(msg))
            
            key = "listening" if channel == "listening_events" else "p2p_network"
            events[key] = raw_messages

        print(f"📥 Consommé : {len(events['listening'])} écoutes et {len(events['p2p_network'])} events réseau.")
        return events


    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        """Valide la présence et le type des champs. Échecs → DLQ PostgreSQL (JSONB)."""
        valid_listening = []
        valid_p2p = []
        invalid_count = 0
        
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        required_fields = ["event_id", "user_id", "track_id", "timestamp", "duration_ms"]

        # 1. Validation des événements d'écoute (listening_events)
        for ev in raw_events.get("listening", []):
            if all(k in ev for k in required_fields) and isinstance(ev.get("duration_ms"), (int, float)):
                valid_listening.append(ev)
            else:
                invalid_count += 1
                hook.run(
                    "INSERT INTO dead_letter_events (error_type, payload) VALUES (%s, %s)",
                    parameters=("validation_error", json.dumps(ev))
                )

        # 2. Branche conditionnelle & Filtrage : Séparation directe du flux technique p2p_network_events
        for ev in raw_events.get("p2p_network", []):
            if "event_id" in ev:
                valid_p2p.append(ev)
            else:
                invalid_count += 1
                hook.run(
                    "INSERT INTO dead_letter_events (error_type, payload) VALUES (%s, %s)",
                    parameters=("p2p_validation_error", json.dumps(ev))
                )

        return {
            "valid_listening": valid_listening, 
            "valid_p2p": valid_p2p, 
            "errors": invalid_count
        }


    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        """Jointure batch PostgreSQL avec la clause ANY pour associer l'artiste et le genre."""
        valid_listening = validated.get("valid_listening", [])
        if not valid_listening:
            return []

        # Récupération unique de tous les track_ids du micro-batch
        track_ids = list(set(ev["track_id"] for ev in valid_listening))
        
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        # Jointure catalogue (track_id -> artiste, genre)
        connection = hook.get_conn()
        cursor = connection.cursor()
        
        cursor.execute(
            "SELECT id, title, artist_id, genre FROM tracks WHERE id = ANY(%s)",
            (track_ids,)
        )
        catalog = {row[0]: {"track_title": row[1], "artist_id": row[2], "genre": row[3]} for row in cursor.fetchall()}
        cursor.close()
        connection.close()

        enriched_list = []
        for ev in valid_listening:
            track_info = catalog.get(ev["track_id"])
            if track_info:
                # Enrichissement de l'événement d'écoute
                ev.update(track_info)
                enriched_list.append(ev)
            else:
                # track_id inconnu → DLQ avec error_type="unknown_track"
                hook.run(
                    "INSERT INTO dead_letter_events (error_type, payload) VALUES (%s, %s)",
                    parameters=("unknown_track", json.dumps(ev))
                )
                
        return enriched_list


    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        """Convertit en DataFrame et écrit en Parquet sur MinIO, partitionné par date et heure."""
        if not enriched_events:
            print("Aucun événement à stocker sur MinIO.")
            return "Aucune donnée"

        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        import boto3

        df = pd.DataFrame(enriched_events)
        
        # Sécurisation du parsing de la date pour le partitionnement horaire
        df['datetime'] = pd.to_datetime(df['timestamp'])
        df['date'] = df['datetime'].dt.strftime('%Y-%m-%d')
        df['hour'] = df['datetime'].dt.strftime('%H')
        df = df.drop(columns=['datetime'])

        # Définition des partitions uniques rencontrées dans ce batch
        partitions = df[['date', 'hour']].drop_duplicates()
        
        s3 = boto3.client(
            's3',
            endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        )

        run_id = context['run_id']

        for _, row in partitions.iterrows():
            sub_df = df[(df['date'] == row['date']) & (df['hour'] == row['hour'])]
            table = pa.Table.from_pandas(sub_df, preserve_index=False)
            
            # Écriture temporaire en mémoire tampon
            sink = pa.BufferOutputStream()
            pq.write_table(table, sink)
            parquet_bytes = sink.getvalue().to_pybytes()

            # Format du chemin cible demandé
            s3_key = f"listening_events/date={row['date']}/hour={row['hour']}/part-{run_id}.parquet"
            
            s3.put_object(
                Bucket='spotify-parquet',
                Key=s3_key,
                Body=parquet_bytes
            )
            
        return f"s3://spotify-parquet/listening_events/ (Batch processed for {len(partitions)} partitions)"


    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        """Insère les données de manière idempotente avec ON CONFLICT DO NOTHING."""
        if not enriched_events:
            return {"inserted": 0, "skipped": 0}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Préparation des tuples pour executemany()
        records = [
            (
                ev["event_id"], ev["user_id"], ev["track_id"], 
                ev["timestamp"], ev["duration_ms"], ev["genre"], 
                ev["artist_id"], ev["track_title"]
            )
            for ev in enriched_events
        ]

        query = """
            INSERT INTO listening_events (id, user_id, track_id, timestamp, duration_ms, genre, artist_id, track_title)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """

        cursor.executemany(query, records)
        conn.commit()
        
        inserted_count = cursor.rowcount
        cursor.close()
        conn.close()

        print(f"💾 Upsert PostgreSQL complété. Lignes affectées : {inserted_count}")
        return {"processed": len(records), "affected_rows": inserted_count}


    # ── Orchestration ─────────────────────────────────────────
    raw       = consume_from_redis()
    validated = validate_events(raw)
    enriched  = enrich_events(validated)

    store_to_parquet(enriched)
    upsert_to_postgres(enriched)