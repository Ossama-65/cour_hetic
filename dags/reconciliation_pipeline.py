"""
DAG : reconciliation_pipeline
================================
Compare les agrégats batch (daily_streams) avec les agrégats streaming
(realtime_top_tracks) pour la même période.
Alerte si divergence > 5% pour un track.

Planification : quotidienne à 06:00 UTC (après aggregation_pipeline 04:00)
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## reconciliation_pipeline

### Rôle
Pont batch ↔ streaming : compare daily_streams (batch Airflow) avec
realtime_top_tracks (Spark Streaming) pour détecter les divergences.

### Logique
- Pour chaque track présent dans les deux tables sur la même date
- Calcule le taux de divergence : |batch - streaming| / batch
- Alerte (log + XCom) si divergence > DIVERGENCE_THRESHOLD (5%)

### Destinations
Table `reconciliation_reports` (créée si absente via migration)

### Planification
Quotidienne à 06:00 UTC (après aggregation_pipeline 04:00 + streaming 05:00)
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID     = "spotify_postgres"
DIVERGENCE_THRESHOLD = 0.05   # 5%
LOOKBACK_DAYS        = 1      # Comparer la veille


with DAG(
    dag_id="reconciliation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pont batch↔streaming : compare daily_streams vs realtime_top_tracks",
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-2", "reconciliation", "quality"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="ensure_reconciliation_table")
    def ensure_reconciliation_table():
        """Crée la table reconciliation_reports si elle n'existe pas."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run("""
            CREATE TABLE IF NOT EXISTS reconciliation_reports (
                id              SERIAL PRIMARY KEY,
                report_date     DATE NOT NULL,
                track_id        UUID,
                batch_count     BIGINT,
                streaming_count BIGINT,
                divergence_pct  FLOAT,
                is_alert        BOOLEAN DEFAULT FALSE,
                created_at      TIMESTAMP DEFAULT NOW()
            )
        """)

    @task(task_id="compare_aggregates")
    def compare_aggregates(**context) -> dict:
        """
        Compare daily_streams vs realtime_top_tracks sur les LOOKBACK_DAYS derniers jours.
        Retourne les tracks avec divergence + statistiques globales.
        """
        logger = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT
                ds.track_id::text,
                ds.date,
                ds.total_streams                                       AS batch_count,
                COALESCE(SUM(rt.stream_count), 0)                      AS streaming_count,
                CASE
                    WHEN ds.total_streams = 0 THEN 0
                    ELSE ABS(ds.total_streams - COALESCE(SUM(rt.stream_count), 0))::float
                         / ds.total_streams
                END                                                    AS divergence_pct
            FROM daily_streams ds
            LEFT JOIN realtime_top_tracks rt
                ON rt.track_id = ds.track_id
                AND rt.window_start::date = ds.date
            WHERE ds.date >= CURRENT_DATE - INTERVAL '%s days'
            GROUP BY ds.track_id, ds.date, ds.total_streams
            ORDER BY divergence_pct DESC
        """, (LOOKBACK_DAYS,))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        results = []
        alerts  = []

        for row in rows:
            track_id, date, batch_count, streaming_count, divergence_pct = row
            is_alert = float(divergence_pct or 0) > DIVERGENCE_THRESHOLD
            results.append({
                "track_id":        track_id,
                "date":            str(date),
                "batch_count":     int(batch_count or 0),
                "streaming_count": int(streaming_count or 0),
                "divergence_pct":  round(float(divergence_pct or 0), 4),
                "is_alert":        is_alert,
            })
            if is_alert:
                alerts.append(track_id)

        logger.info(
            "Réconciliation : %d tracks comparés, %d alertes (divergence > %.0f%%)",
            len(results), len(alerts), DIVERGENCE_THRESHOLD * 100,
        )

        if alerts:
            logger.warning(
                "ALERTE DIVERGENCE — tracks concernés : %s", alerts[:10]
            )

        return {
            "results":       results,
            "total_tracks":  len(results),
            "alert_count":   len(alerts),
            "max_divergence": max((r["divergence_pct"] for r in results), default=0),
        }

    @task(task_id="store_report")
    def store_report(report: dict, **context) -> dict:
        """Persiste le rapport de réconciliation dans PostgreSQL."""
        logger = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        sql = """
            INSERT INTO reconciliation_reports
                (report_date, track_id, batch_count, streaming_count, divergence_pct, is_alert)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        for r in report["results"]:
            cur.execute(sql, (
                r["date"], r["track_id"],
                r["batch_count"], r["streaming_count"],
                r["divergence_pct"], r["is_alert"],
            ))

        conn.commit()
        cur.close()

        logger.info(
            "Rapport stocké : %d tracks | alertes : %d | max divergence : %.1f%%",
            report["total_tracks"], report["alert_count"],
            report["max_divergence"] * 100,
        )
        context["ti"].xcom_push(key="alert_count", value=report["alert_count"])
        return report

    # ── Orchestration ─────────────────────────────────────────
    ensure_table = ensure_reconciliation_table()
    report       = compare_aggregates()
    ensure_table >> report
    store_report(report)
