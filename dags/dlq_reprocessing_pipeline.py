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

DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle
Retraite les événements défectueux isolés dans `dead_letter_events`.
Tente de corriger les erreurs et de réinjecter les events valides dans `listening_events`.

### Sources
- Table `dead_letter_events` où `status = 'pending'` et `retry_count < 3`

### Logique
1. Fetch les events `pending` par lots de BATCH_SIZE
2. Pour chaque event :
   - `missing_fields` avec user_id présent + track_id valide → réinjection
   - `unknown_track` → impossible, marquer `abandoned`
   - `validation` avec timestamp corrigeable → réinjection
   - Sinon : incrémenter retry_count, passer à `abandoned` à 3 tentatives
3. Si succès → INSERT dans `listening_events` + `status='reprocessed'`
4. Si échec → `retry_count + 1`, `abandoned` si >= MAX_RETRIES

### Injection de test
```sql
INSERT INTO dead_letter_events (payload, error_type, original_topic)
VALUES ('{\"user_id\": \"uuid\", \"track_id\": \"uuid\", \"timestamp\": \"2026-06-02T10:00:00Z\",
         \"duration_ms\": 45000, \"completed\": true, \"event_source\": \"p2p\"}',
        'schema_validation', 'listening_events');
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

REQUIRED_FIELDS = {"user_id", "track_id", "timestamp", "duration_ms"}


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
        """
        Récupère les événements en attente de retraitement (status='pending',
        retry_count < MAX_RETRIES) par lots de BATCH_SIZE.

        Returns:
            list[dict]: events à retraiter avec id, payload, error_type, retry_count
        """
        logger = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT id::text, payload, error_type, retry_count, original_topic
            FROM dead_letter_events
            WHERE status = 'pending'
              AND retry_count < %s
            ORDER BY created_at ASC
            LIMIT %s
        """, (MAX_RETRIES, BATCH_SIZE))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        events = [
            {
                "id":           r[0],
                "payload":      r[1] if isinstance(r[1], dict) else json.loads(r[1]),
                "error_type":   r[2],
                "retry_count":  r[3],
                "original_topic": r[4],
            }
            for r in rows
        ]
        logger.info("%d événements pending trouvés en DLQ", len(events))
        return events

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        """
        Tente de corriger et réinjecter chaque événement défectueux.

        Stratégies :
        - Champs manquants mais user_id + track_id présents → tentative de réinjection
        - unknown_track → impossible, marquer failed
        - timestamp absent → fallback sur NOW()
        - track_id invalide (FK) → failed

        Returns:
            dict: {"reprocessed": [...], "failed": [...]}
        """
        logger = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        # Charger les track_ids valides en mémoire pour vérification rapide
        cur.execute("SELECT id::text FROM tracks")
        valid_track_ids = {row[0] for row in cur.fetchall()}

        reprocessed = []
        failed      = []

        for event in pending_events:
            dlq_id  = event["id"]
            payload = event["payload"]
            error_t = event["error_type"]

            # Les events unknown_track ne peuvent pas être corrigés
            if error_t == "unknown_track":
                failed.append({"id": dlq_id, "reason": "unknown_track — non corrigeable"})
                continue

            # Vérifier les champs minimum
            user_id  = payload.get("user_id")
            track_id = payload.get("track_id")

            if not user_id:
                failed.append({"id": dlq_id, "reason": "user_id manquant — non corrigeable"})
                continue

            if not track_id or track_id not in valid_track_ids:
                failed.append({"id": dlq_id, "reason": f"track_id={track_id} invalide"})
                continue

            # Corriger le timestamp si absent ou invalide
            timestamp = payload.get("timestamp")
            if not timestamp:
                timestamp = datetime.utcnow().isoformat() + "Z"
                logger.info("DLQ %s : timestamp manquant — fallback NOW()", dlq_id)

            # Préparer l'event corrigé
            corrected = {
                "event_id":     payload.get("event_id", dlq_id),
                "user_id":      user_id,
                "track_id":     track_id,
                "timestamp":    timestamp,
                "duration_ms":  payload.get("duration_ms", 0),
                "device_type":  payload.get("device_type"),
                "geo_country":  payload.get("geo_country"),
                "completed":    bool(payload.get("completed", False)),
                "event_source": payload.get("event_source", "dlq_reprocessed"),
                "dlq_id":       dlq_id,
            }

            if corrected["duration_ms"] <= 0:
                failed.append({"id": dlq_id, "reason": "duration_ms invalide"})
                continue

            reprocessed.append(corrected)

        cur.close()
        conn.close()

        logger.info(
            "Retraitement : %d succès, %d échecs sur %d events",
            len(reprocessed), len(failed), len(pending_events),
        )
        return {"reprocessed": reprocessed, "failed": failed}

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict, **context) -> dict:
        """
        - Réinsère les events corrigés dans listening_events
        - Met à jour dead_letter_events : status='reprocessed' ou retry_count++/abandoned

        Returns:
            dict: {reprocessed_count, abandoned_count, still_pending_count}
        """
        logger = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        insert_sql = """
            INSERT INTO listening_events
                (id, user_id, track_id, timestamp, duration_ms,
                 device_type, geo_country, completed, event_source, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO NOTHING
        """
        mark_ok_sql = """
            UPDATE dead_letter_events
            SET status = 'reprocessed', resolved_at = NOW(), last_retry_at = NOW()
            WHERE id = %s
        """
        mark_fail_sql = """
            UPDATE dead_letter_events
            SET retry_count    = retry_count + 1,
                last_retry_at  = NOW(),
                status         = CASE
                                    WHEN retry_count + 1 >= %s THEN 'abandoned'
                                    ELSE 'pending'
                                 END
            WHERE id = %s
        """

        reprocessed_count = 0
        abandoned_count   = 0
        still_pending     = 0

        for event in results.get("reprocessed", []):
            try:
                cur.execute(insert_sql, (
                    event["event_id"],
                    event["user_id"],
                    event["track_id"],
                    event["timestamp"],
                    event["duration_ms"],
                    event.get("device_type"),
                    event.get("geo_country"),
                    event["completed"],
                    event["event_source"],
                ))
                cur.execute(mark_ok_sql, (event["dlq_id"],))
                reprocessed_count += 1
            except Exception as exc:
                logger.warning("Échec réinsertion event %s : %s", event["dlq_id"], exc)
                conn.rollback()
                cur.execute(mark_fail_sql, (MAX_RETRIES, event["dlq_id"]))
                conn.commit()

        for item in results.get("failed", []):
            cur.execute(mark_fail_sql, (MAX_RETRIES, item["id"]))
            # Vérifier si on atteint abandoned ou on reste pending
            cur.execute("SELECT status FROM dead_letter_events WHERE id = %s", (item["id"],))
            row = cur.fetchone()
            if row and row[0] == "abandoned":
                abandoned_count += 1
            else:
                still_pending += 1

        conn.commit()
        cur.close()

        stats = {
            "reprocessed_count": reprocessed_count,
            "abandoned_count":   abandoned_count,
            "still_pending":     still_pending,
        }
        logger.info(
            "DLQ bilan : %d retraités, %d abandonnés, %d encore pending",
            reprocessed_count, abandoned_count, still_pending,
        )
        return stats

    # ── Orchestration ─────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)
