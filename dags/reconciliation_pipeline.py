"""
DAG #19 - reconciliation_pipeline
Réconcilie les données entre Kafka (realtime_top_tracks) et PostgreSQL (listening_events)
Détecte les écarts et génère un rapport de cohérence
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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="reconciliation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Réconciliation données streaming vs batch PostgreSQL",
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["spotify", "phase-2", "reconciliation"],
) as dag:

    @task(task_id="count_realtime_events")
    def count_realtime_events(**context) -> dict:
        pg = PostgresHook(postgres_conn_id="spotify_postgres")
        result = pg.get_first("""
            SELECT
                COUNT(*) as total_windows,
                SUM(stream_count) as total_streams,
                MAX(window_end) as latest_window
            FROM realtime_top_tracks
            WHERE window_start >= NOW() - INTERVAL '1 hour'
        """)
        stats = {
            "total_windows": int(result[0]) if result[0] else 0,
            "total_streams": int(result[1]) if result[1] else 0,
            "latest_window": str(result[2]) if result[2] else None,
        }
        print(f"Realtime stats: {stats}")
        return stats

    @task(task_id="count_batch_events")
    def count_batch_events(**context) -> dict:
        pg = PostgresHook(postgres_conn_id="spotify_postgres")
        result = pg.get_first("""
            SELECT
                COUNT(*) as total_events,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT track_id) as unique_tracks
            FROM listening_events
            WHERE listened_at >= NOW() - INTERVAL '1 hour'
        """)
        stats = {
            "total_events": int(result[0]) if result[0] else 0,
            "unique_users": int(result[1]) if result[1] else 0,
            "unique_tracks": int(result[2]) if result[2] else 0,
        }
        print(f"Batch stats: {stats}")
        return stats

    @task(task_id="detect_missing_events")
    def detect_missing_events(**context) -> dict:
        pg = PostgresHook(postgres_conn_id="spotify_postgres")
        # Tracks dans realtime mais absentes de listening_events
        missing = pg.get_records("""
            SELECT DISTINCT r.track_id
            FROM realtime_top_tracks r
            WHERE r.window_start >= NOW() - INTERVAL '1 hour'
            AND NOT EXISTS (
                SELECT 1 FROM listening_events l
                WHERE l.track_id = r.track_id
                AND l.listened_at >= NOW() - INTERVAL '1 hour'
            )
            LIMIT 10
        """)
        missing_ids = [row[0] for row in missing]
        print(f"Tracks manquantes dans listening_events: {len(missing_ids)}")
        return {"missing_tracks": missing_ids, "count": len(missing_ids)}

    @task(task_id="compute_reconciliation_score")
    def compute_reconciliation_score(realtime: dict, batch: dict, missing: dict) -> dict:
        total_realtime = realtime.get("total_streams", 0)
        total_batch = batch.get("total_events", 0)

        if total_realtime == 0:
            score = 100.0
            status = "NO_DATA"
        else:
            ratio = min(total_batch, total_realtime) / max(total_batch, total_realtime) if max(total_batch, total_realtime) > 0 else 1.0
            score = round(ratio * 100, 2)
            status = "OK" if score >= 80 else "WARNING" if score >= 50 else "CRITICAL"

        report = {
            "reconciliation_score": score,
            "status": status,
            "realtime_streams": total_realtime,
            "batch_events": total_batch,
            "missing_tracks_count": missing.get("count", 0),
            "checked_at": datetime.utcnow().isoformat(),
        }
        print(f"Rapport réconciliation: {report}")
        return report

    @task(task_id="store_reconciliation_report")
    def store_reconciliation_report(report: dict, **context):
        pg = PostgresHook(postgres_conn_id="spotify_postgres")
        pg.run("""
            INSERT INTO dead_letter_events
                (original_topic, payload, error_type, error_message, created_at)
            VALUES (%s, %s::jsonb, %s, %s, NOW())
        """, parameters=[
            "reconciliation",
            str(report).replace("'", '"'),
            f"reconciliation_{report['status']}",
            f"Score: {report['reconciliation_score']}% | Realtime: {report['realtime_streams']} | Batch: {report['batch_events']}",
        ])
        print(f"Rapport stocké — status: {report['status']} score: {report['reconciliation_score']}%")

    realtime = count_realtime_events()
    batch = count_batch_events()
    missing = detect_missing_events()
    report = compute_reconciliation_score(realtime, batch, missing)
    store_reconciliation_report(report)
