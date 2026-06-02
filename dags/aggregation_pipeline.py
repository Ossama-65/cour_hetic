"""
DAG : aggregation_pipeline
============================
Calcule les agrégats quotidiens après la fin du streaming_events_pipeline.
Dépend de streaming_events_pipeline via ExternalTaskSensor.

Architecture :
    ExternalTaskSensor (attend streaming_events_pipeline)
        → compute_top_tracks()      ← top 50 du jour → daily_streams
        → compute_artist_stats()    ← streams + unique_listeners → artist_stats
        → compute_p2p_metrics()     ← taux cache_hit, latence moyenne (MinIO)
        → update_aggregates()       ← écriture PostgreSQL (upsert idempotent)
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

### Dépendances
Attend la fin de `streaming_events_pipeline` via ExternalTaskSensor
(`external_task_id=None` → attend le DAGRun complet).

### Destinations
- Table `daily_streams` : top 50 tracks par jour
- Table `artist_stats` : streams + unique listeners par artiste par jour
- Les métriques P2P (cache_hit, latence) sont calculées et loguées
  (pas de table dédiée en Phase 1).

### Stratégie
Incrémentale : calcule uniquement pour le jour courant
(`data_interval_start`, ou `dag_run.conf['date']` si fourni).
Idempotente : INSERT ... ON CONFLICT (track_id, date) DO UPDATE SET ...

### Source des métriques P2P
Les `p2p_network_events` (cache_hit / cache_miss / latency_ms) ne sont pas
en base : ils sont lus depuis les Parquet MinIO écrits par
streaming_events_pipeline (`s3://spotify-parquet/p2p_network_events/date=.../`).
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
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
PARQUET_BUCKET   = "spotify-parquet"


def _target_date(context) -> str:
    """
    Détermine le jour à agréger (stratégie incrémentale).
    Priorité au paramètre manuel `dag_run.conf['date']`, sinon
    le début de l'intervalle de données du run.
    """
    dag_run = context.get("dag_run")
    if dag_run is not None and dag_run.conf and dag_run.conf.get("date"):
        return dag_run.conf["date"]
    return context["data_interval_start"].strftime("%Y-%m-%d")


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
        external_task_id=None,     # attend la fin du DAGRun complet
        allowed_states=["success"],
        failed_states=["failed"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="compute_top_tracks")
    def compute_top_tracks(**context) -> list:
        """
        Calcule le top 50 des tracks pour le jour courant.
        """
        import logging
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        logger = logging.getLogger(__name__)
        date   = _target_date(context)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT
                track_id,
                COUNT(*)                                   AS total_streams,
                COUNT(DISTINCT user_id)                    AS unique_listeners,
                COALESCE(SUM(duration_ms), 0)              AS total_duration_ms,
                ARRAY_AGG(DISTINCT geo_country)
                    FILTER (WHERE geo_country IS NOT NULL) AS countries
            FROM listening_events
            WHERE DATE(timestamp) = %(date)s
              AND completed = TRUE
            GROUP BY track_id
            ORDER BY total_streams DESC
            LIMIT 50
        """, {"date": date})

        rows = cur.fetchall()
        cur.close()
        conn.close()

        top = [
            {
                "track_id":          str(r[0]),
                "total_streams":     int(r[1]),
                "unique_listeners":  int(r[2]),
                "total_duration_ms": int(r[3]),
                "countries":         list(r[4]) if r[4] else [],
            }
            for r in rows
        ]

        logger.info(f"Top tracks ({date}) — {len(top)} tracks calculés")
        return top

    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list:
        """
        Calcule les statistiques par artiste pour le jour courant :
        total_streams, unique_listeners et top_track_id.
        """
        import logging
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        logger = logging.getLogger(__name__)
        date   = _target_date(context)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        cur.execute("""
            WITH track_counts AS (
                SELECT t.artist_id, le.track_id, COUNT(*) AS streams
                FROM listening_events le
                JOIN tracks t ON le.track_id = t.id
                WHERE DATE(le.timestamp) = %(date)s
                  AND le.completed = TRUE
                GROUP BY t.artist_id, le.track_id
            ),
            artist_totals AS (
                SELECT t.artist_id,
                       COUNT(*)                    AS total_streams,
                       COUNT(DISTINCT le.user_id)  AS unique_listeners
                FROM listening_events le
                JOIN tracks t ON le.track_id = t.id
                WHERE DATE(le.timestamp) = %(date)s
                  AND le.completed = TRUE
                GROUP BY t.artist_id
            ),
            top_per_artist AS (
                SELECT DISTINCT ON (artist_id)
                       artist_id, track_id AS top_track_id
                FROM track_counts
                ORDER BY artist_id, streams DESC
            )
            SELECT a.artist_id, a.total_streams, a.unique_listeners, tp.top_track_id
            FROM artist_totals a
            LEFT JOIN top_per_artist tp ON a.artist_id = tp.artist_id
            ORDER BY a.total_streams DESC
        """, {"date": date})

        rows = cur.fetchall()
        cur.close()
        conn.close()

        stats = [
            {
                "artist_id":        str(r[0]),
                "total_streams":    int(r[1]),
                "unique_listeners": int(r[2]),
                "top_track_id":     str(r[3]) if r[3] else None,
            }
            for r in rows
        ]

        logger.info(f"Stats artistes ({date}) — {len(stats)} artistes calculés")
        return stats

    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict:
        """
        Calcule les métriques du réseau P2P pour le jour courant.

        Les p2p_network_events ne sont pas en base : on lit les Parquet
        écrits par streaming_events_pipeline sur MinIO. On y trouve les
        types cache_hit / cache_miss et le champ latency_ms.
        Complété par la distribution event_source des listening_events.
        """
        import logging
        import io

        logger = logging.getLogger(__name__)
        date   = _target_date(context)

        metrics = {
            "date":            date,
            "cache_hit_rate":  0.0,
            "avg_latency_ms":  0.0,
            "active_peers":    0,
            "p2p_event_count": 0,
            "event_type_distribution": {},
            "listening_source_distribution": {},
        }

        # ── 1. Métriques depuis les Parquet P2P sur MinIO ──────────
        try:
            import boto3
            import pandas as pd

            s3 = boto3.client(
                "s3",
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                region_name="us-east-1",
            )

            prefix  = f"p2p_network_events/date={date}/"
            paginator = s3.get_paginator("list_objects_v2")
            frames = []
            for page in paginator.paginate(Bucket=PARQUET_BUCKET, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if not obj["Key"].endswith(".parquet"):
                        continue
                    body = s3.get_object(Bucket=PARQUET_BUCKET, Key=obj["Key"])["Body"].read()
                    frames.append(pd.read_parquet(io.BytesIO(body)))

            if frames:
                df = pd.concat(frames, ignore_index=True)
                metrics["p2p_event_count"] = int(len(df))

                if "event_type" in df.columns:
                    metrics["event_type_distribution"] = {
                        str(k): int(v) for k, v in df["event_type"].value_counts().items()
                    }
                    hits   = int((df["event_type"] == "cache_hit").sum())
                    misses = int((df["event_type"] == "cache_miss").sum())
                    if hits + misses > 0:
                        metrics["cache_hit_rate"] = round(hits / (hits + misses), 4)

                if "latency_ms" in df.columns:
                    lat = df["latency_ms"].dropna()
                    if len(lat) > 0:
                        metrics["avg_latency_ms"] = round(float(lat.mean()), 2)

                if "peer_id" in df.columns:
                    metrics["active_peers"] = int(df["peer_id"].nunique())
            else:
                logger.info(f"Aucun Parquet P2P trouvé sous s3://{PARQUET_BUCKET}/{prefix}")
        except Exception as e:
            logger.warning(f"Lecture métriques P2P MinIO impossible : {e}")

        # ── 2. Distribution event_source des listening_events ──────
        try:
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT event_source, COUNT(*)
                FROM listening_events
                WHERE DATE(timestamp) = %(date)s
                GROUP BY event_source
            """, {"date": date})
            metrics["listening_source_distribution"] = {
                str(src): int(cnt) for src, cnt in cur.fetchall()
            }
            cur.close()
            conn.close()
        except Exception as e:
            logger.warning(f"Distribution event_source impossible : {e}")

        logger.info(
            f"Métriques P2P ({date}) — cache_hit_rate={metrics['cache_hit_rate']}, "
            f"avg_latency_ms={metrics['avg_latency_ms']}, active_peers={metrics['active_peers']}"
        )
        return metrics

    @task(task_id="update_aggregates")
    def update_aggregates(top_tracks: list, artist_stats: list,
                          p2p_metrics: dict, **context):
        """
        Écrit les agrégats dans PostgreSQL de façon idempotente.
        """
        import logging
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        logger = logging.getLogger(__name__)
        date   = _target_date(context)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        # ── daily_streams ──────────────────────────────────────────
        for t in top_tracks:
            cur.execute("""
                INSERT INTO daily_streams
                    (track_id, date, total_streams, unique_listeners,
                     total_duration_ms, countries, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (track_id, date) DO UPDATE SET
                    total_streams     = EXCLUDED.total_streams,
                    unique_listeners  = EXCLUDED.unique_listeners,
                    total_duration_ms = EXCLUDED.total_duration_ms,
                    countries         = EXCLUDED.countries,
                    updated_at        = NOW()
            """, (
                t["track_id"], date, t["total_streams"], t["unique_listeners"],
                t["total_duration_ms"], t["countries"],
            ))

        # ── artist_stats ───────────────────────────────────────────
        for a in artist_stats:
            cur.execute("""
                INSERT INTO artist_stats
                    (artist_id, date, total_streams, unique_listeners,
                     top_track_id, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (artist_id, date) DO UPDATE SET
                    total_streams    = EXCLUDED.total_streams,
                    unique_listeners = EXCLUDED.unique_listeners,
                    top_track_id     = EXCLUDED.top_track_id,
                    updated_at       = NOW()
            """, (
                a["artist_id"], date, a["total_streams"],
                a["unique_listeners"], a["top_track_id"],
            ))

        conn.commit()
        cur.close()
        conn.close()

        if top_tracks:
            best = top_tracks[0]
            logger.info(
                f"✅ Agrégats {date} — {len(top_tracks)} tracks, "
                f"{len(artist_stats)} artistes. "
                f"Top track {best['track_id']} : {best['total_streams']} streams"
            )
        else:
            logger.info(f"Aucun stream à agréger pour {date}")

        logger.info(f"📡 Métriques P2P {date} : {p2p_metrics}")

        return {
            "date":           date,
            "tracks_written": len(top_tracks),
            "artists_written": len(artist_stats),
            "cache_hit_rate": p2p_metrics.get("cache_hit_rate"),
            "avg_latency_ms": p2p_metrics.get("avg_latency_ms"),
        }

    # ── Orchestration ─────────────────────────────────────────
    top_tracks   = compute_top_tracks()
    artist_stats = compute_artist_stats()
    p2p_metrics  = compute_p2p_metrics()

    wait_for_events >> [top_tracks, artist_stats, p2p_metrics]
    update_aggregates(top_tracks, artist_stats, p2p_metrics)
