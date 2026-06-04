"""
DAG #20 - late_events_reprocessing
Retraite les événements tardifs (arrivés après la fenêtre de watermark)
depuis la DLQ vers listening_events
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="late_events_reprocessing",
    default_args=DEFAULT_ARGS,
    description="Retraitement des événements tardifs depuis la DLQ",
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["spotify", "phase-2", "late-events", "dlq"],
) as dag:

    @task(task_id="fetch_late_events")
    def fetch_late_events(**context) -> list:
        pg = PostgresHook(postgres_conn_id="spotify_postgres")
        rows = pg.get_records("""
            SELECT id, payload, created_at
            FROM dead_letter_events
            WHERE error_type = 'late_event'
            AND status = 'pending'
            AND created_at >= NOW() - INTERVAL '24 hours'
            ORDER BY created_at ASC
            LIMIT 1000
        """)
        events = [{"id": r[0], "payload": r[1], "created_at": str(r[2])} for r in rows]
        print(f"Événements tardifs trouvés: {len(events)}")
        return events

    @task(task_id="reprocess_late_events")
    def reprocess_late_events(events: list, **context) -> dict:
        if not events:
            print("Aucun événement tardif à retraiter")
            return {"reprocessed": 0, "failed": 0}

        pg = PostgresHook(postgres_conn_id="spotify_postgres")
        conn = pg.get_conn()
        cur = conn.cursor()
        reprocessed = 0
        failed = 0

        for event in events:
            try:
                payload = event["payload"] if isinstance(event["payload"], dict) else {}
                cur.execute("""
                    INSERT INTO listening_events
                        (user_id, track_id, listened_at, duration_ms, source)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    payload.get("user_id"),
                    payload.get("track_id"),
                    payload.get("timestamp", datetime.utcnow().isoformat()),
                    payload.get("duration_ms", 0),
                    "late_reprocessing",
                ))
                reprocessed += 1
            except Exception as e:
                print(f"Erreur retraitement event {event['id']}: {e}")
                failed += 1

        conn.commit()
        cur.close()
        print(f"Retraitement terminé: {reprocessed} OK, {failed} échecs")
        return {"reprocessed": reprocessed, "failed": failed}

    @task(task_id="update_dlq_status")
    def update_dlq_status(events: list, stats: dict, **context):
        if not events:
            return
        pg = PostgresHook(postgres_conn_id="spotify_postgres")
        event_ids = [e["id"] for e in events]
        placeholders = ",".join(["%s"] * len(event_ids))
        pg.run(f"""
            UPDATE dead_letter_events
            SET status = 'reprocessed', updated_at = NOW()
            WHERE id IN ({placeholders})
        """, parameters=event_ids)
        print(f"DLQ mise à jour: {len(event_ids)} entrées → status=reprocessed")
        print(f"Stats finales: {stats}")

    events = fetch_late_events()
    stats = reprocess_late_events(events)
    update_dlq_status(events, stats)
