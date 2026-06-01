"""
DAG : aggregation_pipeline
============================
Calcule les agrégats quotidiens après la fin du streaming_events_pipeline.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor

DAG_DOC = """
## aggregation_pipeline

### Rôle
Calcule les agrégats quotidiens (top tracks, stats artistes, métriques P2P)
après la fin du streaming_events_pipeline.

### Destinations
- Table `daily_streams` : top 50 tracks par jour
- Table `artist_stats` : streams + unique listeners par artiste par jour

### Idempotence
INSERT ... ON CONFLICT (track_id, date) DO UPDATE SET ...
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
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="compute_top_tracks")
    def compute_top_tracks(**context) -> list:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        from datetime import date as date_type
        date = date_type.today()

        rows = pg.get_records("""
            SELECT
                track_id::text,
                COUNT(*)                        AS total_streams,
                COUNT(DISTINCT user_id)         AS unique_listeners,
                SUM(duration_ms)                AS total_duration_ms,
                ARRAY_AGG(DISTINCT geo_country) AS countries
            FROM listening_events
            WHERE DATE(timestamp) = %(date)s
              AND completed = TRUE
            GROUP BY track_id
            ORDER BY total_streams DESC
            LIMIT 50
        """, parameters={"date": str(date)})

        result = [
            {
                "track_id":         r[0],
                "total_streams":    r[1],
                "unique_listeners": r[2],
                "total_duration_ms": r[3],
                "countries":        list(r[4]) if r[4] else [],
                "date":             str(date),
            }
            for r in rows
        ]
        print(f"✅ Top tracks calculé : {len(result)} tracks pour {date}")
        return result

    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        from datetime import date as date_type
        date = date_type.today()

        rows = pg.get_records("""
            SELECT
                t.artist_id::text,
                COUNT(le.id)                AS total_streams,
                COUNT(DISTINCT le.user_id)  AS unique_listeners,
                MODE() WITHIN GROUP (ORDER BY le.track_id::text) AS top_track_id
            FROM listening_events le
            JOIN tracks t ON le.track_id = t.id
            WHERE DATE(le.timestamp) = %(date)s
            GROUP BY t.artist_id
            ORDER BY total_streams DESC
        """, parameters={"date": str(date)})

        result = [
            {
                "artist_id":        r[0],
                "total_streams":    r[1],
                "unique_listeners": r[2],
                "top_track_id":     r[3],
                "date":             str(date),
            }
            for r in rows
        ]
        print(f"✅ Artist stats calculé : {len(result)} artistes pour {date}")
        return result

    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        from datetime import date as date_type
        date = date_type.today()

        # Taux de cache_hit
        total = pg.get_first("""
            SELECT COUNT(*) FROM listening_events
            WHERE DATE(timestamp) = %(date)s
        """, parameters={"date": str(date)})[0] or 1

        cache_hits = pg.get_first("""
            SELECT COUNT(*) FROM listening_events
            WHERE DATE(timestamp) = %(date)s AND event_source = 'cache'
        """, parameters={"date": str(date)})[0] or 0

        p2p_count = pg.get_first("""
            SELECT COUNT(*) FROM listening_events
            WHERE DATE(timestamp) = %(date)s AND event_source = 'p2p'
        """, parameters={"date": str(date)})[0] or 0

        # Distribution par device_type
        devices = pg.get_records("""
            SELECT device_type, COUNT(*) FROM listening_events
            WHERE DATE(timestamp) = %(date)s
            GROUP BY device_type
        """, parameters={"date": str(date)})

        # Distribution par pays
        countries = pg.get_records("""
            SELECT geo_country, COUNT(*) FROM listening_events
            WHERE DATE(timestamp) = %(date)s
            GROUP BY geo_country ORDER BY COUNT(*) DESC LIMIT 10
        """, parameters={"date": str(date)})

        metrics = {
            "date":            str(date),
            "total_events":    total,
            "cache_hit_rate":  round(cache_hits / total, 4),
            "p2p_rate":        round(p2p_count / total, 4),
            "by_device":       {r[0]: r[1] for r in devices if r[0]},
            "top_countries":   {r[0]: r[1] for r in countries if r[0]},
        }

        print(f"✅ P2P metrics : cache_hit={metrics['cache_hit_rate']:.1%}, "
              f"p2p={metrics['p2p_rate']:.1%}")
        return metrics

    @task(task_id="update_aggregates")
    def update_aggregates(top_tracks: list, artist_stats: list, p2p_metrics: dict, **context):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cur  = conn.cursor()

        # Upsert daily_streams
        for t in top_tracks:
            cur.execute("""
                INSERT INTO daily_streams
                    (track_id, date, total_streams, unique_listeners, total_duration_ms, countries)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (track_id, date) DO UPDATE SET
                    total_streams     = EXCLUDED.total_streams,
                    unique_listeners  = EXCLUDED.unique_listeners,
                    total_duration_ms = EXCLUDED.total_duration_ms,
                    countries         = EXCLUDED.countries,
                    updated_at        = NOW()
            """, (
                t["track_id"], t["date"], t["total_streams"],
                t["unique_listeners"], t["total_duration_ms"], t["countries"]
            ))

        # Upsert artist_stats
        for a in artist_stats:
            cur.execute("""
                INSERT INTO artist_stats
                    (artist_id, date, total_streams, unique_listeners, top_track_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (artist_id, date) DO UPDATE SET
                    total_streams    = EXCLUDED.total_streams,
                    unique_listeners = EXCLUDED.unique_listeners,
                    top_track_id     = EXCLUDED.top_track_id,
                    updated_at       = NOW()
            """, (
                a["artist_id"], a["date"], a["total_streams"],
                a["unique_listeners"], a.get("top_track_id")
            ))

        conn.commit()
        cur.close()

        print(f"✅ Agrégats mis à jour : {len(top_tracks)} tracks, {len(artist_stats)} artistes")
        print(f"   Métriques P2P : {p2p_metrics}")

    # ── Orchestration ─────────────────────────────────────────
    top_tracks   = compute_top_tracks()
    artist_stats = compute_artist_stats()
    p2p_metrics  = compute_p2p_metrics()

    wait_for_events >> [top_tracks, artist_stats, p2p_metrics]
    update_aggregates(top_tracks, artist_stats, p2p_metrics)