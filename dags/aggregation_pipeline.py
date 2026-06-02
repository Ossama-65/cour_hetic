"""
DAG : aggregation_pipeline
============================
Calcule les agrégats quotidiens après la fin du streaming_events_pipeline.
Dépend de streaming_events_pipeline via ExternalTaskSensor.

Architecture :
    ExternalTaskSensor (attend le dernier run réussi de streaming_events_pipeline)
        → compute_top_tracks()      ← top 50 du jour → daily_streams
        → compute_artist_stats()    ← streams + unique_listeners → artist_stats
        → compute_p2p_metrics()     ← taux cache_hit, distribution device/pays
        → update_aggregates()       ← upserts PostgreSQL
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## aggregation_pipeline

### Rôle
Calcule les agrégats quotidiens (top tracks, stats artistes, métriques P2P)
après la fin du streaming_events_pipeline.

### Dépendances
Attend le dernier run réussi de `streaming_events_pipeline` via ExternalTaskSensor
(cross-schedule : utilise `execution_date_fn` pour trouver le run le plus récent).

### Destinations
- Table `daily_streams`  : top 50 tracks par jour
- Table `artist_stats`   : streams + unique listeners par artiste par jour

### Stratégie
Incrémentale : calcule uniquement pour `data_interval_start.date()`.
Idempotente  : INSERT ... ON CONFLICT (track_id, date) DO UPDATE SET ...
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"


def _get_latest_streaming_run(dt):
    """
    Retourne l'execution_date du dernier run réussi de streaming_events_pipeline.
    Utilisé par ExternalTaskSensor pour gérer la différence de schedule (5 min vs 1 jour).
    """
    from airflow.models import DagRun
    from airflow.utils.session import create_session
    from airflow.utils.state import State

    with create_session() as session:
        run = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == "streaming_events_pipeline",
                DagRun.state == State.SUCCESS,
                DagRun.execution_date <= dt,
            )
            .order_by(DagRun.execution_date.desc())
            .first()
        )
        return run.execution_date if run else dt


with DAG(
    dag_id="aggregation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Agrégats quotidiens : top tracks, stats artistes, métriques P2P",
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "aggregation"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_events = ExternalTaskSensor(
        task_id="wait_for_streaming_events",
        external_dag_id="streaming_events_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        execution_date_fn=_get_latest_streaming_run,
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="compute_top_tracks")
    def compute_top_tracks(**context) -> list:
        """
        Calcule le top 50 des tracks pour la date d'exécution (data_interval_start).
        Filtre sur completed=TRUE pour ne compter que les vraies écoutes.

        Returns:
            list[dict]: agrégats par track_id pour la date courante
        """
        logger = logging.getLogger(__name__)
        exec_date = context["data_interval_start"].date()

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT
                le.track_id::text,
                COUNT(*)                           AS total_streams,
                COUNT(DISTINCT le.user_id)         AS unique_listeners,
                COALESCE(SUM(le.duration_ms), 0)   AS total_duration_ms,
                ARRAY_AGG(DISTINCT le.geo_country) AS countries
            FROM listening_events le
            WHERE DATE(le.timestamp) = %s
              AND le.completed = TRUE
            GROUP BY le.track_id
            ORDER BY total_streams DESC
            LIMIT 50
        """, (exec_date,))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        result = [
            {
                "track_id":         r[0],
                "total_streams":    r[1],
                "unique_listeners": r[2],
                "total_duration_ms": r[3],
                "countries":        r[4] or [],
                "date":             str(exec_date),
            }
            for r in rows
        ]
        logger.info("Top tracks pour %s : %d tracks trouvés", exec_date, len(result))
        return result

    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list:
        """
        Calcule les statistiques par artiste pour la date d'exécution.
        Jointure listening_events × tracks pour récupérer l'artist_id.

        Returns:
            list[dict]: stats par artiste pour la date courante
        """
        logger = logging.getLogger(__name__)
        exec_date = context["data_interval_start"].date()

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT
                t.artist_id::text,
                COUNT(*)                   AS total_streams,
                COUNT(DISTINCT le.user_id) AS unique_listeners,
                (
                    SELECT le2.track_id::text
                    FROM listening_events le2
                    JOIN tracks t2 ON le2.track_id = t2.id
                    WHERE t2.artist_id = t.artist_id
                      AND DATE(le2.timestamp) = %s
                      AND le2.completed = TRUE
                    GROUP BY le2.track_id
                    ORDER BY COUNT(*) DESC
                    LIMIT 1
                ) AS top_track_id
            FROM listening_events le
            JOIN tracks t ON le.track_id = t.id
            WHERE DATE(le.timestamp) = %s
              AND le.completed = TRUE
            GROUP BY t.artist_id
            ORDER BY total_streams DESC
        """, (exec_date, exec_date))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        result = [
            {
                "artist_id":        r[0],
                "total_streams":    r[1],
                "unique_listeners": r[2],
                "top_track_id":     r[3],
                "date":             str(exec_date),
            }
            for r in rows
        ]
        logger.info("Artist stats pour %s : %d artistes", exec_date, len(result))
        return result

    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict:
        """
        Calcule les métriques du réseau P2P pour la date d'exécution :
        - Taux de cache_hit (event_source='cache' / total)
        - Nombre de peers uniques actifs (source_peer_id)
        - Distribution device_type et geo_country

        Returns:
            dict: métriques P2P agrégées
        """
        logger = logging.getLogger(__name__)
        exec_date = context["data_interval_start"].date()

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT
                COUNT(*)                                                    AS total_events,
                COUNT(*) FILTER (WHERE event_source = 'cache')             AS cache_hits,
                COUNT(*) FILTER (WHERE event_source = 'p2p')               AS p2p_events,
                COUNT(DISTINCT source_peer_id)                             AS active_peers,
                AVG(duration_ms)                                           AS avg_listen_ms,
                jsonb_object_agg(device_type, device_count) FILTER (WHERE device_type IS NOT NULL) AS device_distribution
            FROM (
                SELECT
                    event_source,
                    source_peer_id,
                    duration_ms,
                    device_type,
                    COUNT(*) OVER (PARTITION BY device_type) AS device_count
                FROM listening_events
                WHERE DATE(timestamp) = %s
            ) sub
        """, (exec_date,))

        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row or row[0] == 0:
            metrics = {
                "date": str(exec_date),
                "total_events": 0,
                "cache_hit_rate": 0.0,
                "p2p_rate": 0.0,
                "active_peers": 0,
                "avg_listen_ms": 0.0,
            }
        else:
            total = row[0] or 1
            metrics = {
                "date":           str(exec_date),
                "total_events":   row[0],
                "cache_hit_rate": round((row[1] or 0) / total, 4),
                "p2p_rate":       round((row[2] or 0) / total, 4),
                "active_peers":   row[3] or 0,
                "avg_listen_ms":  round(float(row[4] or 0), 2),
            }

        logger.info("P2P metrics pour %s : %s", exec_date, metrics)
        return metrics

    @task(task_id="update_aggregates")
    def update_aggregates(top_tracks: list, artist_stats: list, p2p_metrics: dict, **context):
        """
        Écrit les agrégats dans PostgreSQL via upserts idempotents.
        - daily_streams  : ON CONFLICT (track_id, date) DO UPDATE
        - artist_stats   : ON CONFLICT (artist_id, date) DO UPDATE

        Returns:
            dict: stats d'insertion
        """
        logger = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        # ── daily_streams ────────────────────────────────────
        ds_sql = """
            INSERT INTO daily_streams
                (track_id, date, total_streams, unique_listeners, total_duration_ms, countries, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (track_id, date) DO UPDATE SET
                total_streams     = EXCLUDED.total_streams,
                unique_listeners  = EXCLUDED.unique_listeners,
                total_duration_ms = EXCLUDED.total_duration_ms,
                countries         = EXCLUDED.countries,
                updated_at        = NOW()
        """
        for t in top_tracks:
            cur.execute(ds_sql, (
                t["track_id"], t["date"],
                t["total_streams"], t["unique_listeners"],
                t["total_duration_ms"], t["countries"],
            ))

        # ── artist_stats ─────────────────────────────────────
        as_sql = """
            INSERT INTO artist_stats
                (artist_id, date, total_streams, unique_listeners, top_track_id, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (artist_id, date) DO UPDATE SET
                total_streams    = EXCLUDED.total_streams,
                unique_listeners = EXCLUDED.unique_listeners,
                top_track_id     = EXCLUDED.top_track_id,
                updated_at       = NOW()
        """
        for a in artist_stats:
            cur.execute(as_sql, (
                a["artist_id"], a["date"],
                a["total_streams"], a["unique_listeners"], a["top_track_id"],
            ))

        conn.commit()
        cur.close()

        stats = {
            "tracks_aggregated":  len(top_tracks),
            "artists_aggregated": len(artist_stats),
            "p2p_cache_hit_rate": p2p_metrics.get("cache_hit_rate", 0),
            "date":               p2p_metrics.get("date"),
        }

        if top_tracks:
            logger.info(
                "Top track du jour : track_id=%s avec %d streams",
                top_tracks[0]["track_id"], top_tracks[0]["total_streams"],
            )
        logger.info("Agrégats écrits : %s", stats)
        return stats

    # ── Orchestration ─────────────────────────────────────────
    top_tracks   = compute_top_tracks()
    artist_stats = compute_artist_stats()
    p2p_metrics  = compute_p2p_metrics()

    wait_for_events >> [top_tracks, artist_stats, p2p_metrics]
    update_aggregates(top_tracks, artist_stats, p2p_metrics)
