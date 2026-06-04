"""
DAG : aggregation_pipeline
==========================

Calcule les agrégats quotidiens à partir des événements d'écoute.

Objectif issue #7 :
    - attendre la fin de streaming_events_pipeline (ExternalTaskSensor) ;
    - calculer le top 50 des tracks du jour ;
    - calculer les statistiques journalières par artiste ;
    - calculer quelques métriques P2P de suivi ;
    - écrire les résultats dans PostgreSQL de manière idempotente.

Architecture :
    wait_for_streaming_events (ExternalTaskSensor)
        → compute_top_tracks()
        → compute_artist_stats()
        → compute_p2p_metrics()
        → update_aggregates()

Destinations :
    - daily_streams
    - artist_stats
"""

from __future__ import annotations

from datetime import datetime, timedelta, date
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor


POSTGRES_CONN_ID = "spotify_postgres"


DAG_DOC = """
## aggregation_pipeline

### Rôle

Ce DAG calcule les agrégats quotidiens à partir de la table `listening_events`.
Il attend d'abord la fin de `streaming_events_pipeline` via un `ExternalTaskSensor`.

### Sources
- `listening_events`
- `tracks`

### Destinations
- `daily_streams` (top 50 tracks du jour)
- `artist_stats`

### Idempotence

Les insertions utilisent :

```sql
ON CONFLICT (...) DO UPDATE
```

Le DAG peut donc être relancé sans créer de doublons.

### Démo

Pour agréger un jour précis sans dépendre du planning, déclencher avec une conf :

```json
{ "target_date": "2026-06-04" }
```
"""


DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def get_target_date(context: dict[str, Any]) -> date:
    """
    Détermine la date métier à agréger.

    Priorité :
    1. dag_run.conf["target_date"] si fourni manuellement ;
    2. logical_date / execution_date pour les tests Airflow ;
    3. date UTC du jour en dernier recours.

    Remarque :
    On évite d'utiliser data_interval_start ici, car avec un DAG planifié à 04:00,
    Airflow peut produire une date d'intervalle différente de la date métier testée.
    """
    dag_run = context.get("dag_run")

    if dag_run and dag_run.conf and dag_run.conf.get("target_date"):
        return datetime.strptime(dag_run.conf["target_date"], "%Y-%m-%d").date()

    logical_date = context.get("logical_date") or context.get("execution_date")

    if logical_date:
        return logical_date.date()

    return datetime.utcnow().date()


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

    # Attend la fin du DAGRun complet de streaming_events_pipeline.
    # ⚠️ Le sensor matche sur le MÊME logical_date. En run planifié à 04:00,
    #    un run streaming à 04:00 existe → OK. En déclenchement MANUEL (démo),
    #    les dates ne coïncident pas : faire "Mark Success" sur ce sensor,
    #    ou abaisser timeout, ou déclencher les deux DAGs ensemble.
    wait_for_events = ExternalTaskSensor(
        task_id="wait_for_streaming_events",
        external_dag_id="streaming_events_pipeline",
        external_task_id=None,        # attend la fin du DAGRun complet
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="compute_top_tracks")
    def compute_top_tracks(**context) -> list[dict[str, Any]]:
        """Top 50 tracks du jour → alimente daily_streams."""
        target_date = get_target_date(context)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        sql = """
            SELECT
                le.track_id::text AS track_id,
                DATE(le.timestamp) AS stream_date,
                COUNT(*)::bigint AS total_streams,
                COUNT(DISTINCT le.user_id)::bigint AS unique_listeners,
                COALESCE(SUM(le.duration_ms), 0)::bigint AS total_duration_ms,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT le.geo_country), NULL) AS countries
            FROM listening_events le
            WHERE DATE(le.timestamp) = %s
              AND le.completed = TRUE
            GROUP BY le.track_id, DATE(le.timestamp)
            ORDER BY total_streams DESC
            LIMIT 50
        """
        records = hook.get_records(sql, parameters=(target_date,))

        results = [
            {
                "track_id": row[0],
                "date": row[1].isoformat(),
                "total_streams": int(row[2]),
                "unique_listeners": int(row[3]),
                "total_duration_ms": int(row[4]),
                "countries": row[5] or [],
            }
            for row in records
        ]
        print(f"✅ compute_top_tracks | date={target_date} tracks={len(results)}")
        return results

    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list[dict[str, Any]]:
        """
        Stats journalières par artiste : total_streams, unique_listeners, top_track.

        unique_listeners est calculé en COUNT(DISTINCT user_id) AU NIVEAU ARTISTE
        (et non en sommant les distincts par track, ce qui surcompterait les
        auditeurs ayant écouté plusieurs titres du même artiste).
        """
        target_date = get_target_date(context)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        sql = """
            WITH artist_track_streams AS (
                SELECT
                    t.artist_id,
                    le.track_id,
                    DATE(le.timestamp) AS stream_date,
                    COUNT(*)::bigint AS track_streams
                FROM listening_events le
                JOIN tracks t ON t.id = le.track_id
                WHERE DATE(le.timestamp) = %(d)s
                  AND le.completed = TRUE
                GROUP BY t.artist_id, le.track_id, DATE(le.timestamp)
            ),
            ranked_tracks AS (
                SELECT
                    artist_id, track_id, stream_date, track_streams,
                    ROW_NUMBER() OVER (
                        PARTITION BY artist_id, stream_date
                        ORDER BY track_streams DESC, track_id
                    ) AS rank_in_artist
                FROM artist_track_streams
            ),
            artist_totals AS (
                SELECT
                    t.artist_id,
                    DATE(le.timestamp) AS stream_date,
                    COUNT(*)::bigint AS total_streams,
                    COUNT(DISTINCT le.user_id)::bigint AS unique_listeners
                FROM listening_events le
                JOIN tracks t ON t.id = le.track_id
                WHERE DATE(le.timestamp) = %(d)s
                  AND le.completed = TRUE
                GROUP BY t.artist_id, DATE(le.timestamp)
            )
            SELECT
                a.artist_id::text,
                a.stream_date,
                a.total_streams,
                a.unique_listeners,
                r.track_id::text AS top_track_id
            FROM artist_totals a
            JOIN ranked_tracks r
              ON r.artist_id = a.artist_id
             AND r.stream_date = a.stream_date
             AND r.rank_in_artist = 1
            ORDER BY a.total_streams DESC
        """
        records = hook.get_records(sql, parameters={"d": target_date})

        results = [
            {
                "artist_id": row[0],
                "date": row[1].isoformat(),
                "total_streams": int(row[2]),
                "unique_listeners": int(row[3]),
                "top_track_id": row[4],
            }
            for row in records
        ]
        print(f"✅ compute_artist_stats | date={target_date} artistes={len(results)}")
        return results

    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict[str, Any]:
        """
        Métriques de suivi P2P (loggées : pas de table dédiée dans le schéma).
        """
        target_date = get_target_date(context)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        row = hook.get_first(
            """
            SELECT
                COUNT(*)::bigint,
                COUNT(*) FILTER (WHERE event_source = 'cache')::bigint,
                COUNT(*) FILTER (WHERE event_source = 'p2p')::bigint,
                COUNT(DISTINCT source_peer_id)::bigint,
                COUNT(DISTINCT user_id)::bigint
            FROM listening_events
            WHERE DATE(timestamp) = %s
            """,
            parameters=(target_date,),
        )

        total_events = int(row[0] or 0)
        cache_events = int(row[1] or 0)
        p2p_events = int(row[2] or 0)
        active_peers = int(row[3] or 0)
        unique_users = int(row[4] or 0)

        by_device = {
            r[0]: int(r[1])
            for r in hook.get_records(
                """
                SELECT COALESCE(device_type, 'unknown'), COUNT(*)::bigint
                FROM listening_events
                WHERE DATE(timestamp) = %s
                GROUP BY COALESCE(device_type, 'unknown')
                ORDER BY 2 DESC
                """,
                parameters=(target_date,),
            )
        }

        metrics = {
            "date": target_date.isoformat(),
            "total_events": total_events,
            "cache_events": cache_events,
            "p2p_events": p2p_events,
            "active_peers": active_peers,
            "unique_users": unique_users,
            "cache_hit_rate": round(cache_events / total_events, 4) if total_events else 0.0,
            "p2p_rate": round(p2p_events / total_events, 4) if total_events else 0.0,
            "by_device": by_device,
        }
        print(f"✅ compute_p2p_metrics | {metrics}")
        return metrics

    @task(task_id="update_aggregates")
    def update_aggregates(
        top_tracks: list[dict[str, Any]],
        artist_stats: list[dict[str, Any]],
        p2p_metrics: dict[str, Any],
    ) -> dict[str, int]:
        """Écrit les agrégats dans PostgreSQL de façon idempotente."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        daily_rows = [
            (
                item["track_id"],
                item["date"],
                item["total_streams"],
                item["unique_listeners"],
                item["total_duration_ms"],
                item["countries"],
            )
            for item in top_tracks
        ]

        artist_rows = [
            (
                item["artist_id"],
                item["date"],
                item["total_streams"],
                item["unique_listeners"],
                item["top_track_id"],
            )
            for item in artist_stats
        ]

        conn = hook.get_conn()
        with conn.cursor() as cursor:
            if daily_rows:
                cursor.executemany(
                    """
                    INSERT INTO daily_streams (
                        track_id, date, total_streams,
                        unique_listeners, total_duration_ms, countries
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (track_id, date) DO UPDATE SET
                        total_streams = EXCLUDED.total_streams,
                        unique_listeners = EXCLUDED.unique_listeners,
                        total_duration_ms = EXCLUDED.total_duration_ms,
                        countries = EXCLUDED.countries,
                        updated_at = NOW()
                    """,
                    daily_rows,
                )

            if artist_rows:
                cursor.executemany(
                    """
                    INSERT INTO artist_stats (
                        artist_id, date, total_streams,
                        unique_listeners, top_track_id
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (artist_id, date) DO UPDATE SET
                        total_streams = EXCLUDED.total_streams,
                        unique_listeners = EXCLUDED.unique_listeners,
                        top_track_id = EXCLUDED.top_track_id,
                        updated_at = NOW()
                    """,
                    artist_rows,
                )

        conn.commit()

        print(
            "✅ update_aggregates | "
            f"daily_streams={len(daily_rows)} artist_stats={len(artist_rows)} "
            f"p2p_total_events={p2p_metrics.get('total_events')}"
        )
        return {
            "daily_streams_upserted": len(daily_rows),
            "artist_stats_upserted": len(artist_rows),
        }

    # ── Orchestration ────────────────────────────────────────────────────────
    top_tracks = compute_top_tracks()
    artist_stats = compute_artist_stats()
    p2p_metrics = compute_p2p_metrics()

    wait_for_events >> [top_tracks, artist_stats, p2p_metrics]
    update_aggregates(top_tracks, artist_stats, p2p_metrics)
