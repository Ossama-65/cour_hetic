"""
DAG : global_aggregation_pipeline
=====================================
Publie les métriques locales dans `global_metrics` Kafka,
consomme les métriques de tous les groupes,
et produit le Top 50 Global stocké dans Redis (top50:global).

Planification : quotidienne à 08:00 UTC
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## global_aggregation_pipeline

### Rôle
Phase 3 — Calcule le Top 50 Global SPOTIFY en agrégeant les métriques
de tous les groupes participants via le topic Kafka `global_metrics`.

### Sources
- Table `daily_streams` locale → publication dans `global_metrics`
- Topic `global_metrics` → consommation des autres groupes

### Destinations
- Redis clé `top50:global` → Top 50 tracks agrégés tous groupes
- Redis clé `metrics:{groupe}` → métriques par groupe
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID = "spotify_postgres"
KAFKA_BOOTSTRAP  = "kafka-1:9092"
GLOBAL_TOPIC     = "global_metrics"
SOURCE_GROUP     = "groupe-c"
REDIS_HOST       = "redis"
REDIS_PORT       = 6379
REDIS_DB         = 1
TTL_SECONDS      = 86_400


with DAG(
    dag_id="global_aggregation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Phase 3 : publie métriques locales + agrège Top 50 Global via Kafka",
    schedule_interval="0 8 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-3", "top50", "inter-groupes"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="publish_local_metrics")
    def publish_local_metrics(**context) -> dict:
        """
        Publie les métriques locales dans `global_metrics`.
        Format : global_metrics_schema.json v1.0.
        """
        logger = logging.getLogger(__name__)
        from confluent_kafka import Producer

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        # Top 50 du jour depuis daily_streams
        cur.execute("""
            SELECT ds.track_id::text, a.name, t.title, t.genre,
                   ds.total_streams, ds.unique_listeners
            FROM daily_streams ds
            JOIN tracks t ON ds.track_id = t.id
            JOIN artists a ON t.artist_id = a.id
            WHERE ds.date = CURRENT_DATE - 1
            ORDER BY ds.total_streams DESC
            LIMIT 50
        """)
        top_tracks = [
            {
                "track_id":        r[0],
                "artist_name":     r[1],
                "track_title":     r[2],
                "genre":           r[3],
                "stream_count":    int(r[4] or 0),
                "unique_listeners":int(r[5] or 0),
            }
            for r in cur.fetchall()
        ]

        # Métriques globales
        cur.execute("""
            SELECT COALESCE(SUM(total_streams), 0),
                   COALESCE(AVG(COALESCE((
                       SELECT cache_hit_rate FROM reconciliation_reports
                       WHERE report_date = CURRENT_DATE - 1 LIMIT 1
                   ), 0)), 0)
            FROM daily_streams WHERE date = CURRENT_DATE - 1
        """)
        row = cur.fetchone()
        total_streams = int(row[0] or 0) if row else 0
        cur.close()
        conn.close()

        metrics_msg = {
            "source_group":   SOURCE_GROUP,
            "date":           (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "published_at":   datetime.utcnow().isoformat() + "Z",
            "top_tracks":     top_tracks,
            "total_streams":  total_streams,
            "active_peers":   0,
            "cache_hit_rate": 0.0,
            "schema_version": "1.0",
        }

        producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all"})
        producer.produce(GLOBAL_TOPIC, key=SOURCE_GROUP,
                         value=json.dumps(metrics_msg))
        producer.flush()

        logger.info("Métriques locales publiées : %d top tracks, %d streams",
                    len(top_tracks), total_streams)
        return {"top_tracks": len(top_tracks), "total_streams": total_streams}

    @task(task_id="aggregate_global_top50")
    def aggregate_global_top50(**context) -> dict:
        """
        Consomme `global_metrics` de tous les groupes,
        agrège les streams par track_id,
        et stocke le Top 50 Global dans Redis (top50:global).
        """
        logger = logging.getLogger(__name__)
        import redis as redis_lib

        try:
            from kafka import KafkaConsumer
        except ImportError:
            logger.warning("kafka-python non installé — return")
            return {"top50_size": 0}

        consumer = KafkaConsumer(
            GLOBAL_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=10_000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id=f"airflow-global-agg-{SOURCE_GROUP}",
        )

        # Agréger toutes les métriques
        track_totals: dict = {}
        group_metrics: dict = {}

        for msg in consumer:
            metrics = msg.value
            group = metrics.get("source_group", "unknown")
            group_metrics[group] = {
                "total_streams": metrics.get("total_streams", 0),
                "date":          metrics.get("date"),
            }
            for track in metrics.get("top_tracks", []):
                tid = track["track_id"]
                if tid not in track_totals:
                    track_totals[tid] = {
                        "track_id":    tid,
                        "artist_name": track.get("artist_name", ""),
                        "track_title": track.get("track_title", ""),
                        "genre":       track.get("genre"),
                        "stream_count": 0,
                        "groups":      [],
                    }
                track_totals[tid]["stream_count"] += track.get("stream_count", 0)
                if group not in track_totals[tid]["groups"]:
                    track_totals[tid]["groups"].append(group)

        consumer.commit()
        consumer.close()

        # Top 50 global
        top50 = sorted(track_totals.values(),
                       key=lambda x: x["stream_count"], reverse=True)[:50]

        # Stocker dans Redis
        r = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        r.setex("top50:global", TTL_SECONDS, json.dumps(top50))

        for group, gmeta in group_metrics.items():
            r.setex(f"metrics:{group}", TTL_SECONDS, json.dumps(gmeta))

        logger.info("Top 50 Global : %d tracks depuis %d groupes → Redis top50:global",
                    len(top50), len(group_metrics))
        return {"top50_size": len(top50), "groups": list(group_metrics.keys())}

    # ── Orchestration ─────────────────────────────────────────
    published = publish_local_metrics()
    published >> aggregate_global_top50()
