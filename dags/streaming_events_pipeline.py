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
        → route_events()             ← branche conditionnelle (listening / p2p / skip)
        ├─ enrich_events()           ← jointure catalogue PostgreSQL
        │   → store_to_parquet()     ← MinIO partitionné par heure
        │   → upsert_to_postgres()   ← table listening_events
        ├─ store_p2p_to_parquet()    ← MinIO p2p_network_events
        └─ skip_processing           ← batch vide
        → summarize()                ← jointure finale (trigger_rule)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

DAG_DOC = """
## streaming_events_pipeline

### Rôle
Consomme en micro-batch les événements du simulateur P2P depuis Redis,
les valide, les enrichit et les stocke en dual : Parquet (MinIO) + PostgreSQL.

### Sources
- Redis LIST `listening_events`
- Redis LIST `p2p_network_events`

### Destinations
- Table `listening_events` (PostgreSQL)
- Fichiers Parquet sur MinIO : `s3://spotify-parquet/listening_events/date=.../hour=.../`
- Fichiers Parquet sur MinIO : `s3://spotify-parquet/p2p_network_events/date=.../hour=.../`
- Table `dead_letter_events` (pour les events invalides)

### Branches conditionnelles
Après validation, `route_events` (BranchPythonOperator via @task.branch)
choisit dynamiquement les chemins à exécuter selon le contenu du batch :
- des listening events valides → `enrich_events` → store + upsert
- des p2p events valides       → `store_p2p_to_parquet`
- batch vide                   → `skip_processing` (EmptyOperator)
La tâche finale `summarize` se déclenche avec NONE_FAILED_MIN_ONE_SUCCESS
pour s'exécuter quel que soit le chemin emprunté.

### Idempotence
Chaque event est identifié par `event_id` (UUID).
L'upsert utilise `ON CONFLICT (id) DO NOTHING` pour éviter les doublons.
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
BATCH_SIZE       = 500  # nombre max d'events à lire par run


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
        Consomme les événements stockés dans les Redis LISTs.
        Le simulateur publie via pub/sub ET écrit dans des listes (lpush).
        On lit avec rpop pour vider la liste batch par batch.
        """
        import redis
        import json
        import logging

        logger = logging.getLogger(__name__)

        r = redis.from_url(REDIS_URL, decode_responses=True)

        listening_events = []
        p2p_events       = []

        # Lire jusqu'à BATCH_SIZE events depuis la liste listening_events
        for _ in range(BATCH_SIZE):
            msg = r.rpop("listening_events_list")
            if not msg:
                break
            try:
                listening_events.append(json.loads(msg))
            except Exception as e:
                logger.warning(f"Message invalide ignoré : {e}")

        # Lire les events réseau P2P
        for _ in range(BATCH_SIZE):
            msg = r.rpop("p2p_network_events_list")
            if not msg:
                break
            try:
                p2p_events.append(json.loads(msg))
            except Exception as e:
                logger.warning(f"Message P2P invalide ignoré : {e}")

        logger.info(f"Consommé — listening: {len(listening_events)}, p2p: {len(p2p_events)}")

        return {
            "listening":   listening_events,
            "p2p_network": p2p_events,
        }

    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        """
        Valide les événements et envoie les invalides en DLQ.
        """
        import json
        import logging

        logger = logging.getLogger(__name__)

        REQUIRED_LISTENING = ["event_id", "user_id", "track_id", "timestamp", "duration_ms"]

        valid_listening = []
        valid_p2p       = []
        errors          = []

        # Valider les listening events
        for event in raw_events.get("listening", []):
            missing = [f for f in REQUIRED_LISTENING if not event.get(f)]
            if missing:
                errors.append({
                    "event":   event,
                    "missing": missing,
                })
                continue

            # Valider duration_ms > 0
            if event.get("duration_ms", 0) <= 0:
                errors.append({"event": event, "missing": ["duration_ms > 0"]})
                continue

            valid_listening.append(event)

        # Les events P2P ont moins de contraintes
        for event in raw_events.get("p2p_network", []):
            if event.get("event_id") and event.get("timestamp"):
                valid_p2p.append(event)
            else:
                errors.append({"event": event, "missing": ["event_id", "timestamp"]})

        # Envoyer les erreurs en DLQ
        if errors:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            cur  = conn.cursor()
            for error in errors:
                cur.execute("""
                    INSERT INTO dead_letter_events
                        (original_topic, payload, error_type, error_message)
                    VALUES (%s, %s, %s, %s)
                """, (
                    "streaming_events",
                    json.dumps(error["event"]),
                    "validation",
                    f"Champs manquants : {error['missing']}",
                ))
            conn.commit()
            cur.close()
            conn.close()

        logger.info(f"Validation — valides: {len(valid_listening)}, erreurs: {len(errors)}")

        return {
            "valid_listening": valid_listening,
            "valid_p2p":       valid_p2p,
            "errors":          len(errors),
        }

    @task.branch(task_id="route_events")
    def route_events(validated: dict, **context) -> list:
        """
        Branche conditionnelle : choisit dynamiquement les chemins à exécuter
        en fonction du contenu du batch validé.

        - listening events présents → "enrich_events"
        - p2p events présents       → "store_p2p_to_parquet"
        - rien à traiter            → "skip_processing"

        @task.branch s'appuie sur BranchPythonOperator : seules les tâches
        dont l'id est retourné s'exécutent, les autres sont marquées skipped.
        """
        import logging

        logger = logging.getLogger(__name__)

        paths = []
        if validated.get("valid_listening"):
            paths.append("enrich_events")
        if validated.get("valid_p2p"):
            paths.append("store_p2p_to_parquet")

        if not paths:
            paths.append("skip_processing")

        logger.info(f"Routage du batch → {paths}")
        return paths

    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        """
        Enrichit les événements d'écoute avec les données du catalogue.
        Ajoute : genre, artist_id, track_title pour chaque event.
        """
        import logging
        import json

        logger = logging.getLogger(__name__)

        events = validated.get("valid_listening", [])

        if not events:
            logger.info("Aucun événement à enrichir")
            return []

        # Récupérer tous les track_ids uniques
        track_ids = list(set(e["track_id"] for e in events))

        # Une seule requête pour tous les tracks
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT id, title, artist_id, genre
            FROM tracks
            WHERE id = ANY(%s)
        """, (track_ids,))

        tracks_map = {
            str(row[0]): {
                "title":     row[1],
                "artist_id": str(row[2]),
                "genre":     row[3],
            }
            for row in cur.fetchall()
        }

        cur.close()
        conn.close()

        # Enrichir chaque event
        enriched = []
        unknown  = []

        for event in events:
            track_info = tracks_map.get(event["track_id"])
            if not track_info:
                unknown.append(event)
                continue

            event["track_title"] = track_info["title"]
            event["artist_id"]   = track_info["artist_id"]
            event["genre"]       = track_info["genre"]
            enriched.append(event)

        # Envoyer les tracks inconnus en DLQ
        if unknown:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            cur  = conn.cursor()
            for event in unknown:
                cur.execute("""
                    INSERT INTO dead_letter_events
                        (original_topic, payload, error_type, error_message)
                    VALUES (%s, %s, %s, %s)
                """, (
                    "streaming_events",
                    json.dumps(event),
                    "unknown_track",
                    f"track_id inconnu : {event['track_id']}",
                ))
            conn.commit()
            cur.close()
            conn.close()

        logger.info(f"Enrichissement — enrichis: {len(enriched)}, inconnus: {len(unknown)}")
        return enriched

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        """
        Sauvegarde les événements enrichis en Parquet sur MinIO.
        """
        import logging
        import boto3
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        import io

        logger = logging.getLogger(__name__)

        if not enriched_events:
            logger.info("Aucun événement à sauvegarder")
            return ""

        # Convertir en DataFrame
        df = pd.DataFrame(enriched_events)

        # Partitionner par date et heure
        now  = datetime.utcnow()
        date = now.strftime("%Y-%m-%d")
        hour = now.strftime("%H")
        run_id = context["run_id"].replace(":", "-").replace("+", "-")

        path = f"listening_events/date={date}/hour={hour}/part-{run_id}.parquet"

        # Convertir en Parquet en mémoire
        table  = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        # Upload sur MinIO
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1",
        )
        s3.put_object(
            Bucket="spotify-parquet",
            Key=path,
            Body=buffer.getvalue(),
        )

        logger.info(f"✅ Parquet sauvegardé : s3://spotify-parquet/{path} ({len(enriched_events)} events)")
        return path

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        """
        Insère les événements dans PostgreSQL de façon idempotente.
        """
        import logging

        logger = logging.getLogger(__name__)

        if not enriched_events:
            logger.info("Aucun événement à insérer")
            return {"inserted": 0, "skipped": 0}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        inserted = 0
        skipped  = 0

        for event in enriched_events:
            try:
                cur.execute("""
                    INSERT INTO listening_events
                        (id, user_id, track_id, timestamp, duration_ms,
                         device_type, geo_country, completed, event_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    event["event_id"],
                    event["user_id"],
                    event["track_id"],
                    event["timestamp"],
                    event["duration_ms"],
                    event.get("device_type"),
                    event.get("geo_country"),
                    event.get("completed", False),
                    event.get("event_source", "p2p"),
                ))
                if cur.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                logger.warning(f"Erreur insertion event {event.get('event_id')} : {e}")
                skipped += 1

        conn.commit()
        cur.close()
        conn.close()

        logger.info(f"✅ PostgreSQL — insérés: {inserted}, skippés: {skipped}")
        return {"inserted": inserted, "skipped": skipped}

    @task(task_id="store_p2p_to_parquet")
    def store_p2p_to_parquet(validated: dict, **context) -> str:
        """
        Branche P2P : sauvegarde les événements réseau p2p_network_events
        en Parquet sur MinIO, partitionnés par heure.
        Ces events ne vont pas dans la table listening_events ; ils sont
        conservés bruts pour l'analyse réseau / détection de fraude.
        """
        import logging
        import boto3
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        import io

        logger = logging.getLogger(__name__)

        events = validated.get("valid_p2p", [])
        if not events:
            logger.info("Aucun événement P2P à sauvegarder")
            return ""

        df = pd.DataFrame(events)

        now    = datetime.utcnow()
        date   = now.strftime("%Y-%m-%d")
        hour   = now.strftime("%H")
        run_id = context["run_id"].replace(":", "-").replace("+", "-")

        path = f"p2p_network_events/date={date}/hour={hour}/part-{run_id}.parquet"

        table  = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1",
        )
        s3.put_object(
            Bucket="spotify-parquet",
            Key=path,
            Body=buffer.getvalue(),
        )

        logger.info(f"✅ Parquet P2P sauvegardé : s3://spotify-parquet/{path} ({len(events)} events)")
        return path

    @task(
        task_id="summarize",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    def summarize(
        parquet_path: str = "",
        upsert_result: dict = None,
        p2p_path: str = "",
        **context,
    ) -> dict:
        """
        Jointure finale des branches. S'exécute quel que soit le chemin
        emprunté grâce à trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS
        (les branches non choisies sont skipped, pas en échec).
        """
        import logging

        logger = logging.getLogger(__name__)

        upsert_result = upsert_result or {}
        summary = {
            "listening_parquet": parquet_path or None,
            "p2p_parquet":       p2p_path or None,
            "inserted":          upsert_result.get("inserted", 0),
            "skipped":           upsert_result.get("skipped", 0),
        }
        logger.info(f"📊 Résumé du run : {summary}")
        return summary

    # ── Orchestration ─────────────────────────────────────────
    raw       = consume_from_redis()
    validated = validate_events(raw)

    # Branche conditionnelle
    choice = route_events(validated)

    # Chemin "skip" si batch vide
    skip = EmptyOperator(task_id="skip_processing")

    # Chemin listening
    enriched       = enrich_events(validated)
    parquet_path   = store_to_parquet(enriched)
    upsert_result  = upsert_to_postgres(enriched)

    # Chemin p2p
    p2p_path = store_p2p_to_parquet(validated)

    # Le branch contrôle les têtes de chaque chemin
    choice >> [enriched, p2p_path, skip]

    # Jointure finale
    final = summarize(parquet_path, upsert_result, p2p_path)
    [parquet_path, upsert_result, p2p_path, skip] >> final
