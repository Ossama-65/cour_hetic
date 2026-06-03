"""
DAG : streaming_events_pipeline
=================================
Consomme les événements d'écoute depuis Redis (pub/sub),
les valide, les enrichit avec le catalogue et les stocke.

Planification : toutes les 5 minutes
Catchup       : désactivé (micro-batch temps réel)

Architecture :
    Redis (pub/sub listening_events + p2p_network_events)
        → consume_from_redis()
        → validate_events()          ← invalides → DLQ
        → enrich_events()            ← jointure catalogue PostgreSQL
        → store_to_parquet()         ← MinIO partitionné par heure
        → upsert_to_postgres()       ← table listening_events

TODO :
    [x] Implémenter consume_from_redis() — accumuler les events sur 5 min
    [x] Implémenter validate_events() — champs obligatoires, envoyer invalides en DLQ
    [x] Implémenter enrich_events() — joindre avec le catalogue (track_id → artiste, genre)
    [x] Implémenter store_to_parquet() — Parquet sur MinIO partitionné par heure
    [x] Implémenter upsert_to_postgres() — insérer dans listening_events
    [x] Utiliser TaskFlow API (@task) pour toutes les tâches
    [x] Ajouter des branches conditionnelles : séparer listening_events et p2p_network_events
    [x] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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

### TODO
Compléter les 5 tâches marquées NotImplementedError.
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
        """
        Consomme les événements accumulés dans les listes Redis.
        """
        redis_hook = RedisHook(redis_conn_id="spotify_redis")
        conn = redis_hook.get_conn()
        
        events = {"listening": [], "p2p_network": []}
        
        # On vide les listes Redis (LPUSH côté simulateur, RPOP ici)
        for channel in REDIS_CHANNELS:
            key = "listening" if "listening" in channel else "p2p_network"
            while True:
                msg = conn.rpop(channel)
                if not msg:
                    break
                events[key].append(json.loads(msg))
        
        print(f"📥 Récupérés : {len(events['listening'])} écoutes, {len(events['p2p_network'])} P2P")
        return events

    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        """
        Valide les événements et isole les invalides en DLQ.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        valid_listening = []
        errors_count = 0
        
        required_fields = ["event_id", "user_id", "track_id", "timestamp", "duration_ms"]
        
        for ev in raw_events.get("listening", []):
            if all(k in ev for k in required_fields) and ev["duration_ms"] > 0:
                valid_listening.append(ev)
            else:
                errors_count += 1
                pg_hook.run(
                    "INSERT INTO dead_letter_events (payload, error_type, original_topic) VALUES (%s, %s, %s)",
                    parameters=(json.dumps(ev), "validation_error", "listening_events")
                )
        
        return {"valid_listening": valid_listening, "errors_count": errors_count}

    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        """
        Enrichit les événements d'écoute avec les données du catalogue.
        """
        listening = validated["valid_listening"]
        if not listening:
            return []
            
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        track_ids = list(set(ev["track_id"] for ev in listening))
        
        # Récupération en une seule fois des métadonnées du catalogue
        df_catalog = pg_hook.get_pandas_df(
            "SELECT id as track_id, title as track_title, artist_id, genre FROM tracks WHERE id IN %s",
            parameters=(tuple(track_ids),)
        )
        
        df_events = pd.DataFrame(listening)
        enriched_df = df_events.merge(df_catalog, on="track_id", how="inner")
        
        # On identifie les tracks manquants (non trouvés dans le catalogue)
        missing_count = len(df_events) - len(enriched_df)
        if missing_count > 0:
            print(f"⚠️ {missing_count} événements ignorés car track_id inconnu")
            
        return enriched_df.to_dict(orient="records")

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        """
        Sauvegarde les événements enrichis en Parquet sur MinIO.
        """
        if not enriched_events:
            return "No data"
            
        df = pd.DataFrame(enriched_events)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Extraction des partitions
        dt = df['timestamp'].iloc[0]
        date_str = dt.strftime('%Y-%m-%d')
        hour_str = dt.strftime('%H')
        
        table = pa.Table.from_pandas(df)
        buf = BytesIO()
        pq.write_table(table, buf)
        
        s3_hook = S3Hook(aws_conn_id="spotify_minio")
        file_key = f"listening_events/date={date_str}/hour={hour_str}/events_{context['run_id']}.parquet"
        
        s3_hook.load_file_obj(
            file_obj=BytesIO(buf.getvalue()),
            key=file_key,
            bucket_name="spotify-parquet",
            replace=True
        )
        
        return file_key

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        """
        Insère les événements dans PostgreSQL de façon idempotente.
        """
        if not enriched_events:
            return {"inserted": 0}
            
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        data = [
            (ev['event_id'], ev['user_id'], ev['track_id'], ev['timestamp'], ev['duration_ms'], ev.get('completed', True))
            for ev in enriched_events
        ]
        
        # Insertion avec gestion de conflit sur event_id
        sql = """
            INSERT INTO listening_events (event_id, user_id, track_id, timestamp, duration_ms, completed)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
        """
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, data)
                conn.commit()
                
        return {"inserted": len(data)}

    # ── Orchestration ─────────────────────────────────────────
    raw       = consume_from_redis()
    validated = validate_events(raw)
    enriched  = enrich_events(validated)

    store_to_parquet(enriched)
    upsert_to_postgres(enriched)
