"""
DAG : dlq_reprocessing_pipeline
==================================
Retraite périodiquement les événements défectueux de la Dead Letter Queue.

Planification : toutes les heures
Catchup       : désactivé

Architecture :
    PostgreSQL dead_letter_events (status='pending')
        → fetch_pending_dlq()       ← récupérer les events à retraiter
        → reprocess_events()        ← tenter de corriger et réinjecter
        → update_dlq_status()       ← marquer reprocessed ou abandoned
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle
Retraite les événements défectueux isolés dans `dead_letter_events`.
Tente de corriger les erreurs et de réinjecter les events valides.

### Sources
- Table `dead_letter_events` où `status = 'pending'`

### Logique de retraitement
1. Récupérer les events `pending` avec `retry_count < 3`
2. Tenter la validation et la correction
3. Si succès → réinjecter dans `listening_events` + `status = 'reprocessed'`
4. Si échec après 3 tentatives → `status = 'abandoned'`

### Test d'injection
```sql
INSERT INTO dead_letter_events (payload, error_type, original_topic)
VALUES ('{"user_id": null, "track_id": "invalid"}', 'missing_fields', 'listening_events');
```
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
MAX_RETRIES      = 3
BATCH_SIZE       = 100


with DAG(
    dag_id="dlq_reprocessing_pipeline",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des événements Dead Letter Queue",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "dlq", "resilience"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="fetch_pending_dlq")
    def fetch_pending_dlq(**context) -> list:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, payload, error_type, retry_count, original_topic
            FROM dead_letter_events
            WHERE status = 'pending'
              AND retry_count < %s
            ORDER BY created_at ASC
            LIMIT %s
        """, (MAX_RETRIES, BATCH_SIZE))

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        events = [
            {
                "id":             str(row[0]),
                "payload":        row[1],
                "error_type":     row[2],
                "retry_count":    row[3],
                "original_topic": row[4],
            }
            for row in rows
        ]

        logger.info(f"{len(events)} événements pending trouvés")
        return events

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        reprocessed = []
        failed = []

        for event in pending_events:
            try:
                payload = event["payload"]
                if isinstance(payload, str):
                    payload = json.loads(payload)

                if not payload.get("user_id"):
                    failed.append(event)
                    continue

                if not payload.get("track_id"):
                    failed.append(event)
                    continue

                if not payload.get("timestamp"):
                    payload["timestamp"] = datetime.utcnow().isoformat()

                event["payload_corrected"] = payload
                reprocessed.append(event)

            except Exception as e:
                logger.warning(f"Event {event['id']} non retraitable : {e}")
                failed.append(event)

        logger.info(f"{len(reprocessed)} corrigés, {len(failed)} en échec")
        return {"reprocessed": reprocessed, "failed": failed}

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict, **context) -> dict:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        reprocessed = results.get("reprocessed", [])
        failed = results.get("failed", [])

        for event in reprocessed:
            p = event["payload_corrected"]
            try:
                cursor.execute("""
                    INSERT INTO listening_events
                        (user_id, track_id, timestamp, device_type,
                         geo_country, completed, event_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    p.get("user_id"),
                    p.get("track_id"),
                    p.get("timestamp"),
                    p.get("device_type", "unknown"),
                    p.get("geo_country", "unknown"),
                    p.get("completed", False),
                    "dlq_reprocessed",
                ))
                cursor.execute("""
                    UPDATE dead_letter_events
                    SET status = 'reprocessed', resolved_at = NOW()
                    WHERE id = %s
                """, (event["id"],))
            except Exception as e:
                logger.warning(f"Réinjection échouée pour {event['id']} : {e}")

        for event in failed:
            cursor.execute("""
                UPDATE dead_letter_events
                SET retry_count   = retry_count + 1,
                    last_retry_at = NOW(),
                    status = CASE
                        WHEN retry_count + 1 >= %s THEN 'abandoned'
                        ELSE 'pending'
                    END
                WHERE id = %s
            """, (MAX_RETRIES, event["id"]))

        conn.commit()
        cursor.close()
        conn.close()

        remaining = hook.get_first(
            "SELECT COUNT(*) FROM dead_letter_events WHERE status = 'pending'"
        )[0]

        stats = {
            "reprocessed":   len(reprocessed),
            "abandoned":     len([e for e in failed if e.get("retry_count", 0) + 1 >= MAX_RETRIES]),
            "still_pending": remaining,
        }
        logger.info(f"Bilan DLQ : {stats}")
        return stats

    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)
