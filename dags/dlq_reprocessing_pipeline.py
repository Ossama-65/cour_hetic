"""
DAG : dlq_reprocessing_pipeline
==================================
Retraite périodiquement les événements défectueux de la Dead Letter Queue.

Planification : toutes les heures
Catchup       : désactivé

Architecture :
    PostgreSQL dead_letter_events (status='pending')
        → fetch_pending_dlq()       ← récupérer les events en attente
        → reprocess_events()        ← tenter de retraiter
        → update_dlq_status()       ← marquer resolved ou increment retry_count
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle
Retraite les événements de la Dead Letter Queue (DLQ) toutes les heures.
Les événements échoués sont retentés jusqu'à 3 fois, puis marqués 'failed'.

### Source
- Table `dead_letter_events` (status='pending')

### Logique
- retry_count < 3 → retenter l'insertion dans listening_events
- retry_count >= 3 → marquer status='failed'
- Succès → marquer status='resolved'
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
    description="Retraitement horaire de la Dead Letter Queue",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "dlq", "resilience"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="fetch_pending_dlq")
    def fetch_pending_dlq(**context) -> list:
        """
        Récupère les événements en attente dans la DLQ.
        Limite à BATCH_SIZE pour éviter de surcharger le système.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import logging

        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, payload, error_type, retry_count
            FROM dead_letter_events
            WHERE status = 'pending'
              AND retry_count < %s
            ORDER BY created_at ASC
            LIMIT %s
        """, (MAX_RETRIES, BATCH_SIZE))

        rows = cursor.fetchall()
        cursor.close()

        events = [
            {
                "id": str(row[0]),
                "payload": row[1],
                "error_type": row[2],
                "retry_count": row[3],
            }
            for row in rows
        ]
        logging.info(f"DLQ : {len(events)} événements en attente")
        return events

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        """
        Tente de retraiter chaque événement de la DLQ.
        Les événements de type 'schema_validation' sont ignorés (irrécupérables).
        Les événements 'unknown_track' sont retentés si le track existe maintenant.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import logging

        if not pending_events:
            logging.info("Aucun événement à retraiter")
            return {"resolved": [], "failed": []}

        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cursor = conn.cursor()

        resolved = []
        failed = []

        for event in pending_events:
            event_id = event["id"]
            payload = event["payload"]
            error_type = event["error_type"]
            retry_count = event["retry_count"]

            try:
                if error_type == "schema_validation":
                    # Irrécupérable — marquer failed directement
                    failed.append(event_id)
                    continue

                if error_type == "unknown_track":
                    # Vérifier si le track existe maintenant
                    track_id = payload.get("track_id")
                    if track_id:
                        cursor.execute(
                            "SELECT id FROM tracks WHERE id = %s", (track_id,)
                        )
                        if cursor.fetchone():
                            # Track trouvé — tenter l'insertion
                            cursor.execute("""
                                INSERT INTO listening_events
                                    (id, user_id, track_id, timestamp, duration_ms,
                                     device_type, geo_country, completed, event_source)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (id) DO NOTHING
                            """, (
                                payload.get("event_id"),
                                payload.get("user_id"),
                                payload.get("track_id"),
                                payload.get("timestamp"),
                                payload.get("duration_ms"),
                                payload.get("device_type"),
                                payload.get("geo_country"),
                                payload.get("completed", False),
                                payload.get("event_source", "p2p"),
                            ))
                            resolved.append(event_id)
                            continue

                # Par défaut : incrémenter le retry_count
                if retry_count + 1 >= MAX_RETRIES:
                    failed.append(event_id)
                else:
                    # Sera retenté au prochain run
                    cursor.execute("""
                        UPDATE dead_letter_events
                        SET retry_count = retry_count + 1,
                            last_retry_at = NOW()
                        WHERE id = %s
                    """, (event_id,))

            except Exception as e:
                logging.warning(f"Erreur retraitement {event_id} : {e}")
                failed.append(event_id)

        conn.commit()
        cursor.close()

        logging.info(f"Retraitement : {len(resolved)} résolus, {len(failed)} échoués")
        return {"resolved": resolved, "failed": failed}

    @task(task_id="update_dlq_status")
    def update_dlq_status(reprocess_result: dict, **context) -> dict:
        """
        Met à jour le statut des événements dans la DLQ.
        - resolved → status='resolved', resolved_at=NOW()
        - failed   → status='failed'
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import logging

        resolved = reprocess_result.get("resolved", [])
        failed = reprocess_result.get("failed", [])

        if not resolved and not failed:
            return {"resolved": 0, "failed": 0}

        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cursor = conn.cursor()

        if resolved:
            cursor.execute("""
                UPDATE dead_letter_events
                SET status = 'resolved', resolved_at = NOW()
                WHERE id = ANY(%s)
            """, (resolved,))

        if failed:
            cursor.execute("""
                UPDATE dead_letter_events
                SET status = 'failed', last_retry_at = NOW()
                WHERE id = ANY(%s)
            """, (failed,))

        conn.commit()
        cursor.close()

        logging.info(f"DLQ mise à jour : {len(resolved)} résolus, {len(failed)} échoués")
        return {"resolved": len(resolved), "failed": len(failed)}

    # ── Orchestration ─────────────────────────────────────────
    pending        = fetch_pending_dlq()
    reprocessed    = reprocess_events(pending)
    update_dlq_status(reprocessed)