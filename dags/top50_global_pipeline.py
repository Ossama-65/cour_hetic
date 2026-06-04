"""
DAG #24 - top50_global_pipeline
Publie nos agregats dans le topic Kafka partage `global_metrics`
et consomme les metriques de tous les groupes pour produire le Top 50 Global.
Stocke dans Redis : cle `top50:global`
"""
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=20),
}

KAFKA_BOOTSTRAP  = "kafka-1:9092"
GLOBAL_TOPIC     = "global_metrics"
SOURCE_GROUP     = "groupe-d"
REDIS_URL        = "redis://redis:6379/0"
TOP50_KEY        = "top50:global"
TOP50_TTL        = 300  # 5 minutes

POSTGRES_CONN_ID = "spotify_postgres"


with DAG(
    dag_id="top50_global_pipeline",
    default_args=DEFAULT_ARGS,
    description="Top 50 Global cross-groupes via Kafka + Redis",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-3", "top50", "global"],
) as dag:

    @task(task_id="publish_our_metrics")
    def publish_our_metrics(**context) -> int:
        from kafka import KafkaProducer

        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        rows = pg.get_records("""
            SELECT track_id::text, SUM(stream_count) as total, SUM(unique_listeners) as listeners
            FROM realtime_top_tracks
            WHERE window_start >= NOW() - INTERVAL '10 minutes'
            GROUP BY track_id
            ORDER BY total DESC
            LIMIT 50
        """)

        if not rows:
            logger.info("Pas de donnees realtime a publier")
            return 0

        payload = {
            "group_id":       SOURCE_GROUP,
            "timestamp":      datetime.utcnow().isoformat() + "Z",
            "window_minutes": 10,
            "schema_version": "1.0",
            "top_tracks": [
                {
                    "track_id":         row[0],
                    "stream_count":     int(row[1]),
                    "unique_listeners": int(row[2]),
                }
                for row in rows
            ],
        }

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send(GLOBAL_TOPIC, value=payload)
        producer.flush()
        producer.close()

        logger.info(f"{len(rows)} tracks publiees dans {GLOBAL_TOPIC}")
        return len(rows)

    @task(task_id="consume_all_groups_metrics")
    def consume_all_groups_metrics(**context) -> list:
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            GLOBAL_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="earliest",
            consumer_timeout_ms=20000,
            group_id=f"{SOURCE_GROUP}-top50-consumer",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

        all_metrics = []
        for msg in consumer:
            all_metrics.append(msg.value)

        consumer.close()
        logger.info(f"{len(all_metrics)} messages recus depuis {GLOBAL_TOPIC}")
        return all_metrics

    @task(task_id="compute_top50_global")
    def compute_top50_global(all_metrics: list, **context) -> list:
        scores = defaultdict(lambda: {"stream_count": 0, "unique_listeners": 0, "groups": []})

        for msg in all_metrics:
            group_id = msg.get("group_id", "unknown")
            for track in msg.get("top_tracks", []):
                track_id = track.get("track_id")
                if not track_id:
                    continue
                scores[track_id]["stream_count"]     += track.get("stream_count", 0)
                scores[track_id]["unique_listeners"] += track.get("unique_listeners", 0)
                if group_id not in scores[track_id]["groups"]:
                    scores[track_id]["groups"].append(group_id)

        top50 = sorted(
            [{"track_id": tid, **data} for tid, data in scores.items()],
            key=lambda x: x["stream_count"],
            reverse=True
        )[:50]

        logger.info(f"Top 50 Global calculé: {len(top50)} tracks de {len(set(m.get('group_id') for m in all_metrics))} groupes")
        return top50

    @task(task_id="store_top50_redis")
    def store_top50_redis(top50: list, **context):
        import redis

        r = redis.from_url(REDIS_URL)
        payload = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "track_count":  len(top50),
            "tracks":       top50,
        }
        r.setex(TOP50_KEY, TOP50_TTL, json.dumps(payload))
        logger.info(f"top50:global stocké dans Redis ({len(top50)} tracks, TTL={TOP50_TTL}s)")

        # verification
        raw = r.get(TOP50_KEY)
        if raw:
            data = json.loads(raw)
            logger.info(f"Verification Redis OK — {data['track_count']} tracks")

    published    = publish_our_metrics()
    all_metrics  = consume_all_groups_metrics()
    top50        = compute_top50_global(all_metrics)
    published >> top50
    store_top50_redis(top50)
