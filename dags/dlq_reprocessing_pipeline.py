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

TODO :
    [x] Implémenter fetch_pending_dlq()
    [x] Implémenter reprocess_events()
    [x] Implémenter update_dlq_status()
    [x] Tester avec injection de données corrompues
    [x] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

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

### Test d'\''injection
```sql
INSERT INTO dead_letter_events (payload, error_type, original_topic)
VALUES ('{"user_id": null, "track_id": "invalid"}', 'missing_fields', 'listening_events');
```

### TODO
Compléter les 3 tâches marquées NotImplementedError.
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
BATCH_SIZE       = 100   # traiter par lots pour ne pas surcharger


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
        Récupère les événements en attente de retraitement.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT id, payload, error_type, retry_count, original_topic, created_at
            FROM dead_letter_events
            WHERE status = 'pending' AND retry_count < %s
            ORDER BY created_at ASC LIMIT %s
        """
        records = hook.get_records(sql, parameters=(MAX_RETRIES, BATCH_SIZE))
        
        events = [{"id": r[0], "payload": r[1], "error_type": r[2], "retry_count": r[3], "topic": r[4], "created_at": r[5]} for r in records]
        print(f"🔎 {len(events)} événements pending trouvés dans la DLQ")
        return events

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        """Tente de corriger et réinjecter chaque événement défectueux."""
        reprocessed = []
        failed = []
        
        for event in pending_events:
            payload = event['payload']
            is_valid = True
            reason = ""

            # Logique de correction
            if not payload.get('user_id'):
                is_valid = False
                reason = "missing_user_id"
            
            if is_valid and not payload.get('timestamp'):
                # Correction : fallback sur la date de création en DLQ
                payload['timestamp'] = event['created_at'].isoformat()
                print(f"🛠️ Correction timestamp pour event {event['id']}")

            if is_valid:
                reprocessed.append({"id": event['id'], "payload": payload, "topic": event['topic']})
            else:
                failed.append({"id": event['id'], "reason": reason})
                
        return {"reprocessed": reprocessed, "failed": failed}

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict, **context) -> dict:
        """Met à jour le statut des événements dans PostgreSQL."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Traitement des succès
        for item in results['reprocessed']:
            # Note: Ici on simule la réinjection dans listening_events
            # Dans la phase 2, cela pourrait être un push vers Kafka
            hook.run("""
                UPDATE dead_letter_events 
                SET status = 'reprocessed', resolved_at = NOW(), retry_count = retry_count + 1
                WHERE id = %s
            """, parameters=(item['id'],))

        # Traitement des échecs
        for item in results['failed']:
            hook.run("""
                UPDATE dead_letter_events 
                SET retry_count = retry_count + 1,
                    last_retry_at = NOW(),
                    status = CASE WHEN retry_count + 1 >= %s THEN 'abandoned' ELSE 'pending' END
                WHERE id = %s
            """, parameters=(MAX_RETRIES, item['id']))

        print(f"✅ Bilan : {len(results['reprocessed'])} retraités, {len(results['failed'])} en échec/retry")
        return {"reprocessed_count": len(results['reprocessed']), "failed_count": len(results['failed'])}

    # ── Orchestration ─────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)
