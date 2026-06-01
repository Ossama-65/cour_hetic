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
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

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
- Fichiers Parquet partitionnés sur MinIO
- Table `dead_letter_events` (pour les events invalides)

### Idempotence
Chaque event est identifié par `event_id` (UUID).
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
REDIS_URL        = "redis://redis:6379/1"
BATCH_SIZE       = 1000


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
        import redis as redis_lib
        import json

        r = redis_lib.from_url(REDIS_URL, decode_responses=True)

        listening  = []
        p2p        = []

        # Lire depuis les listes Redis (le simulateur publie sur pub/sub
        # ET on utilise une liste buffer pour la persistance)
        # On lit jusqu'à BATCH_SIZE events
        raw_listening = r.lrange("listening_events:buffer", 0, BATCH_SIZE - 1)
        r.ltrim("listening_events:buffer", len(raw_listening), -1)

        raw_p2p = r.lrange("p2p_network_events:buffer", 0, BATCH_SIZE - 1)
        r.ltrim("p2p_network_events:buffer", len(raw_p2p), -1)

        for raw in raw_listening:
            try:
                listening.append(json.loads(raw))
            except Exception:
                pass

        for raw in raw_p2p:
            try:
                p2p.append(json.loads(raw))
            except Exception:
                pass

        # Si les buffers sont vides, lire directement depuis pub/sub history
        # via une liste temporaire pushée par le simulateur
        if not listening:
            # Utiliser KEYS pattern pour récupérer les events récents
            keys = r.keys("event:listening:*")
            for key in keys[:BATCH_SIZE]:
                try:
                    val = r.get(key)
                    if val:
                        listening.append(json.loads(val))
                        r.delete(key)
                except Exception:
                    pass

        print(f"✅ Consommé : {len(listening)} listening events, {len(p2p)} p2p events")
        return {"listening": listening, "p2p_network": p2p}

    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        import json
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        valid_listening = []
        valid_p2p       = []
        errors          = 0

        required_listening = ["event_id", "user_id", "track_id", "timestamp", "duration_ms"]

        for event in raw_events.get("listening", []):
            # Vérifier les champs obligatoires
            if not all(event.get(f) for f in required_listening):
                pg.run("""
                    INSERT INTO dead_letter_events
                        (original_topic, payload, error_type, error_message)
                    VALUES (%s, %s, %s, %s)
                """, parameters=[
                    "listening_events",
                    json.dumps(event),
                    "validation",
                    f"Champs manquants : {[f for f in required_listening if not event.get(f)]}"
                ])
                errors += 1
                continue

            # Vérifier duration_ms > 0
            try:
                if int(event["duration_ms"]) <= 0:
                    raise ValueError("duration_ms <= 0")
            except (ValueError, TypeError) as e:
                pg.run("""
                    INSERT INTO dead_letter_events
                        (original_topic, payload, error_type, error_message)
                    VALUES (%s, %s, %s, %s)
                """, parameters=["listening_events", json.dumps(event), "validation", str(e)])
                errors += 1
                continue

            valid_listening.append(event)

        for event in raw_events.get("p2p_network", []):
            if event.get("event_id") and event.get("event_type"):
                valid_p2p.append(event)
            else:
                errors += 1

        print(f"✅ Validation : {len(valid_listening)} valides, {errors} erreurs DLQ")
        return {
            "valid_listening": valid_listening,
            "valid_p2p":       valid_p2p,
            "errors":          errors,
        }

    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        import json
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        events = validated.get("valid_listening", [])

        if not events:
            print("⚠️  Aucun event à enrichir")
            return []

        # Récupérer tous les track_ids uniques
        track_ids = list({e["track_id"] for e in events})

        # Une seule requête pour tous les tracks
        if track_ids:
            placeholders = ",".join(["%s"] * len(track_ids))
            tracks_data  = pg.get_records(
                f"SELECT id::text, title, artist_id::text, genre FROM tracks WHERE id::text = ANY(%s)",
                parameters=[track_ids]
            )
            tracks_map = {row[0]: {"title": row[1], "artist_id": row[2], "genre": row[3]}
                          for row in tracks_data}
        else:
            tracks_map = {}

        enriched = []
        unknown  = 0

        for event in events:
            track_info = tracks_map.get(event["track_id"])
            if not track_info:
                unknown += 1
                # Track inconnue → DLQ
                pg.run("""
                    INSERT INTO dead_letter_events
                        (original_topic, payload, error_type, error_message)
                    VALUES (%s, %s, %s, %s)
                """, parameters=[
                    "listening_events", json.dumps(event),
                    "unknown_track", f"track_id inconnu : {event['track_id']}"
                ])
                continue

            enriched.append({
                **event,
                "track_title": track_info["title"],
                "artist_id":   track_info["artist_id"],
                "genre":       track_info["genre"],
            })

        print(f"✅ Enrichi : {len(enriched)} events | {unknown} tracks inconnues → DLQ")
        return enriched

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        import io, json, boto3
        from datetime import datetime

        if not enriched_events:
            print("⚠️  Aucun event à stocker")
            return "no_data"

        try:
            import pandas as pd
            df = pd.DataFrame(enriched_events)
        except ImportError:
            # pandas non disponible — stocker en JSON
            s3 = boto3.client("s3",
                endpoint_url="http://minio:9000",
                aws_access_key_id="minioadmin",
                aws_secret_access_key="minioadmin",
            )
            now     = datetime.utcnow()
            key     = f"listening_events/date={now.strftime('%Y-%m-%d')}/hour={now.hour}/part-{context['run_id']}.json"
            payload = json.dumps(enriched_events).encode()
            try:
                s3.create_bucket(Bucket="spotify-parquet")
            except Exception:
                pass
            s3.put_object(Bucket="spotify-parquet", Key=key, Body=payload)
            print(f"✅ Stocké en JSON sur MinIO : s3://spotify-parquet/{key}")
            return key

        s3  = boto3.client("s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )
        now = datetime.utcnow()
        key = f"listening_events/date={now.strftime('%Y-%m-%d')}/hour={now.hour}/part-{context['run_id']}.parquet"

        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)

        try:
            s3.create_bucket(Bucket="spotify-parquet")
        except Exception:
            pass
        s3.upload_fileobj(buf, "spotify-parquet", key)
        print(f"✅ Parquet stocké : s3://spotify-parquet/{key} ({len(df)} lignes)")
        return key

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if not enriched_events:
            print("⚠️  Aucun event à insérer")
            return {"inserted": 0, "skipped": 0}

        pg   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cur  = conn.cursor()

        rows = [
            (
                e["event_id"],
                e["user_id"],
                e["track_id"],
                e["timestamp"],
                int(e.get("duration_ms", 0)),
                e.get("device_type"),
                e.get("geo_country"),
                bool(e.get("completed", False)),
                e.get("event_source", "p2p"),
            )
            for e in enriched_events
        ]

        inserted = 0
        skipped  = 0

        for row in rows:
            cur.execute("""
                INSERT INTO listening_events
                    (id, user_id, track_id, timestamp, duration_ms,
                     device_type, geo_country, completed, event_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, row)
            if cur.rowcount == 1:
                inserted += 1
            else:
                skipped += 1

        conn.commit()
        cur.close()

        print(f"✅ PostgreSQL : {inserted} insérés, {skipped} doublons ignorés")
        return {"inserted": inserted, "skipped": skipped}

    # ── Orchestration ─────────────────────────────────────────
    raw      = consume_from_redis()
    validated = validate_events(raw)
    enriched  = enrich_events(validated)

    store_to_parquet(enriched)
    upsert_to_postgres(enriched)