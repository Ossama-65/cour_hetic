"""
DAG : dlq_reprocessing_pipeline
================================

Retraite périodiquement les événements présents dans la Dead Letter Queue.

Objectif :
    - récupérer les événements DLQ en statut pending ;
    - tenter de corriger les erreurs simples ;
    - réinsérer les événements valides dans listening_events ;
    - mettre à jour le statut DLQ en reprocessed ou abandoned.

Architecture :
    PostgreSQL dead_letter_events
        → fetch_pending_dlq()
        → reprocess_events()
        → update_dlq_status()
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "spotify_postgres"
MAX_RETRIES = 3
BATCH_SIZE = 100


DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle

Ce DAG retraite les événements invalides stockés dans `dead_letter_events`.

### Logique

1. Lire les événements `pending` avec `retry_count < 3`.
2. Essayer de corriger les erreurs simples.
3. Si l'événement devient valide, le réinsérer dans `listening_events`.
4. Marquer l'événement comme `reprocessed`.
5. Si l'événement reste invalide, incrémenter `retry_count`.
6. Après 3 tentatives, passer l'événement en `abandoned`.

### Idempotence

La réinsertion dans `listening_events` utilise :

```sql
ON CONFLICT (id) DO NOTHING
```

Ainsi, un événement déjà retraité ne crée pas de doublon.
"""


DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}


def parse_payload(payload: Any) -> dict[str, Any]:
    """
    Convertit le payload DLQ en dictionnaire.

    PostgreSQL JSONB peut revenir sous forme de dict OU de string selon le hook.
    """
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        return json.loads(payload)
    raise ValueError(f"Payload DLQ non supporté : {type(payload)}")


def normalize_payload(payload: dict[str, Any], created_at: datetime) -> dict[str, Any]:
    """
    Corrige les erreurs simples dans un événement.

    Corrections possibles :
    - si timestamp manquant, utiliser created_at ;
    - si event_id absent mais id présent, utiliser id ;
    - si event_source absent, utiliser p2p ;
    - si completed absent, utiliser False.
    """
    corrected = dict(payload)

    if not corrected.get("event_id") and corrected.get("id"):
        corrected["event_id"] = corrected["id"]

    if not corrected.get("timestamp"):
        # created_at peut être un datetime (psycopg2) ou une string
        corrected["timestamp"] = (
            created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at)
        )

    if not corrected.get("event_source"):
        corrected["event_source"] = "p2p"

    if "completed" not in corrected:
        corrected["completed"] = False

    return corrected


def validate_listening_event(payload: dict[str, Any]) -> list[str]:
    """Retourne la liste des champs obligatoires manquants ou invalides."""
    required_fields = ["event_id", "user_id", "track_id", "timestamp", "duration_ms"]
    missing = [field for field in required_fields if not payload.get(field)]

    try:
        duration_ms = int(payload.get("duration_ms", 0))
        if duration_ms <= 0:
            missing.append("duration_ms_positive")
    except (TypeError, ValueError):
        missing.append("duration_ms_integer")

    return missing


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
    def fetch_pending_dlq() -> list[dict[str, Any]]:
        """Récupère les événements DLQ en attente de retraitement."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        records = hook.get_records(
            """
            SELECT id, original_topic, payload, error_type,
                   error_message, retry_count, created_at
            FROM dead_letter_events
            WHERE status = 'pending'
              AND retry_count < %s
            ORDER BY created_at ASC
            LIMIT %s
            """,
            parameters=(MAX_RETRIES, BATCH_SIZE),
        )

        events = [
            {
                "id": record[0],
                "original_topic": record[1],
                "payload": parse_payload(record[2]),
                "error_type": record[3],
                "error_message": record[4],
                "retry_count": record[5],
                "created_at": record[6],
            }
            for record in records
        ]

        print(f"🔎 Événements DLQ pending trouvés : {len(events)}")
        return events

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list[dict[str, Any]]) -> dict[str, Any]:
        """Tente de corriger les événements DLQ."""
        reprocessed: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []

        for event in pending_events:
            payload = normalize_payload(
                payload=event["payload"],
                created_at=event["created_at"],
            )
            topic = event.get("original_topic") or ""

            if topic == "listening_events":
                missing = validate_listening_event(payload)
                if missing:
                    failed.append(
                        {"id": event["id"], "reason": f"Champs invalides/manquants : {missing}"}
                    )
                    continue
                reprocessed.append({"id": event["id"], "topic": topic, "payload": payload})
            else:
                failed.append(
                    {"id": event["id"], "reason": f"Topic non retraitable automatiquement : {topic}"}
                )

        print(
            "🛠️ Retraitement DLQ terminé | "
            f"réinjectables={len(reprocessed)} échecs={len(failed)}"
        )
        return {"reprocessed": reprocessed, "failed": failed}

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict[str, Any]) -> dict[str, int]:
        """Réinsère les événements valides et met à jour le statut DLQ."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        reprocessed_count = 0
        failed_count = 0

        conn = hook.get_conn()
        with conn.cursor() as cursor:

            for item in results.get("reprocessed", []):
                payload = item["payload"]

                cursor.execute(
                    """
                    INSERT INTO listening_events (
                        id, user_id, track_id, source_peer_id, timestamp,
                        duration_ms, device_type, geo_country, completed, event_source
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        payload["event_id"],
                        payload["user_id"],
                        payload["track_id"],
                        payload.get("source_peer_id") or payload.get("source_peer"),
                        payload["timestamp"],
                        int(payload["duration_ms"]),
                        payload.get("device_type"),
                        payload.get("geo_country"),
                        bool(payload.get("completed", False)),
                        payload.get("event_source", "p2p"),
                    ),
                )

                cursor.execute(
                    """
                    UPDATE dead_letter_events
                    SET status = 'reprocessed',
                        retry_count = retry_count + 1,
                        last_retry_at = NOW(),
                        resolved_at = NOW(),
                        error_message = 'Successfully reprocessed into listening_events'
                    WHERE id = %s
                    """,
                    (item["id"],),
                )
                reprocessed_count += 1

            for item in results.get("failed", []):
                cursor.execute(
                    """
                    UPDATE dead_letter_events
                    SET retry_count = retry_count + 1,
                        last_retry_at = NOW(),
                        status = CASE
                            WHEN retry_count + 1 >= %s THEN 'abandoned'
                            ELSE 'pending'
                        END,
                        error_message = %s
                    WHERE id = %s
                    """,
                    (MAX_RETRIES, item["reason"], item["id"]),
                )
                failed_count += 1

        conn.commit()

        print(
            "✅ Mise à jour DLQ terminée | "
            f"réinsérés={reprocessed_count} échecs/retry={failed_count}"
        )
        return {"reprocessed_count": reprocessed_count, "failed_count": failed_count}

    # ── Orchestration ────────────────────────────────────────────────────────
    pending_events = fetch_pending_dlq()
    reprocessing_results = reprocess_events(pending_events)
    update_dlq_status(reprocessing_results)
