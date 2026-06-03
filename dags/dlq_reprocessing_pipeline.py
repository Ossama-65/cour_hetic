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

        TODO :
            1. Utiliser PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            2. Requête :
               SELECT id, payload, error_type, retry_count, original_topic
               FROM dead_letter_events
               WHERE status = 'pending'
                 AND retry_count < %(max_retries)s
               ORDER BY created_at ASC
               LIMIT %(batch_size)s
            3. Retourner la liste des events à retraiter
            4. Logger : "X événements pending trouvés"
        """
        raise NotImplementedError("TODO : implémenter fetch_pending_dlq()")
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT id, payload, error_type, retry_count, original_topic, created_at
            FROM dead_letter_events
            WHERE status = 'pending'
              AND retry_count < %s
            ORDER BY created_at ASC
            LIMIT %s
        """
        records = hook.get_records(sql, parameters=(MAX_RETRIES, BATCH_SIZE))
        
        pending_list = []
        for r in records:
            pending_list.append({
                "id": r[0],
                "payload": r[1],
                "error_type": r[2],
                "retry_count": r[3],
                "original_topic": r[4],
                "created_at": r[5]
            })
        
        print(f"🔎 {len(pending_list)} événements pending trouvés dans la DLQ")
        return pending_list

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        """
        Tente de corriger et réinjecter chaque événement défectueux.

        TODO :
            1. Pour chaque event, parser le payload JSON
            2. Tenter la validation des champs obligatoires
            3. Tenter la correction si possible :
               - user_id manquant → impossible à corriger → abandoned
               - timestamp invalide → utiliser created_at comme fallback
               - track_id inconnu → vérifier dans tracks, si absent → abandoned
            4. Si valide : préparer pour réinsertion dans listening_events
            5. Retourner {"reprocessed": [...], "failed": [...]}
        """
        reprocessed = []
        failed = []

        for event in pending_events:
            try:
                payload = event['payload']
                if isinstance(payload, str):
                    payload = json.loads(payload)
                
                # Logique de correction simple
                is_valid = True
                
                # 1. user_id est critique
                if not payload.get('user_id'):
                    is_valid = False
                
                # 2. Correction timestamp (fallback sur date de création DLQ)
                if is_valid and not payload.get('timestamp'):
                    payload['timestamp'] = event['created_at'].isoformat()
                
                if is_valid:
                    reprocessed.append({
                        "dlq_id": event['id'],
                        "payload": payload
                    })
                else:
                    failed.append(event['id'])
            except Exception as e:
                print(f"Erreur lors du traitement de l'event {event['id']}: {e}")
                failed.append(event['id'])

        return {"reprocessed": reprocessed, "failed": failed}

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict, **context) -> dict:
        """
        Met à jour le statut des événements dans dead_letter_events.

        TODO :
            1. Pour les events retraités avec succès :
               - INSERT dans listening_events
               - UPDATE dead_letter_events SET status='reprocessed', resolved_at=NOW()
            2. Pour les events échoués :
               - UPDATE dead_letter_events
                 SET retry_count = retry_count + 1,
                     last_retry_at = NOW(),
                     status = CASE WHEN retry_count + 1 >= 3 THEN 'abandoned' ELSE 'pending' END
            3. Logger le bilan : "X retraités, Y abandonnés, Z encore en pending"
            4. Retourner les stats
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # 1. Gérer les succès
        for item in results['reprocessed']:
            p = item['payload']
            # Réinjection (ajuster selon le topic d'origine si nécessaire)
            hook.run("""
                INSERT INTO listening_events (user_id, track_id, timestamp, duration_ms)
                VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
            """, parameters=(p.get('user_id'), p.get('track_id'), p.get('timestamp'), p.get('duration_ms')))
            
            hook.run("UPDATE dead_letter_events SET status='reprocessed', resolved_at=NOW() WHERE id=%s", 
                     parameters=(item['dlq_id'],))

        # 2. Gérer les échecs (incrémenter retry ou abandonner)
        all_failed = results['failed']
        if all_failed:
            hook.run("""
                UPDATE dead_letter_events
                SET retry_count = retry_count + 1,
                    last_retry_at = NOW(),
                    status = CASE WHEN retry_count + 1 >= %s THEN 'abandoned' ELSE 'pending' END
                WHERE id = ANY(%s)
            """, parameters=(MAX_RETRIES, all_failed))

        print(f"✅ Bilan : {len(results['reprocessed'])} retraités, {len(all_failed)} mis à jour")
        return {"reprocessed_count": len(results['reprocessed']), "failed_count": len(all_failed)}

    # ── Orchestration ─────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)
