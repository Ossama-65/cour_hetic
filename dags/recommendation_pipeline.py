"""
DAG : recommendation_pipeline
=============================

Génère des recommandations musicales personnalisées à partir des écoutes,
du catalogue et des agrégats journaliers.

Stratégie :
    1. Identifier les genres et artistes écoutés par chaque utilisateur.
    2. Proposer des tracks du même genre ou du même artiste.
    3. Exclure les tracks déjà écoutées.
    4. Favoriser les tracks populaires dans daily_streams.
    5. Insérer les recommandations dans PostgreSQL et les stocker dans Redis.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

import redis
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor


logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_URL = "redis://redis:6379/1"

RECO_TTL_SECONDS = 86400
TOP_N_RECO = 10


DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
}


DAG_DOC = """
## recommendation_pipeline

Ce DAG génère des recommandations musicales personnalisées.

### Logique

- lire les préférences utilisateur depuis `listening_events` et `tracks` ;
- identifier les genres/artistes préférés ;
- proposer des tracks du catalogue non encore écoutées ;
- pondérer avec la popularité issue de `daily_streams` ;
- insérer dans `recommendations` avec un upsert idempotent ;
- stocker aussi les recommandations dans Redis avec une TTL de 24h.
"""


with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Generate music recommendations from listening events and daily aggregates",
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "recommendation", "ml"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_aggregation = ExternalTaskSensor(
        task_id="wait_for_aggregation",
        external_dag_id="aggregation_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="generate_recommendations")
    def generate_recommendations() -> list[dict[str, Any]]:
        """
        Génère des recommandations robustes, même avec peu d'événements.

        La requête :
        - construit les préférences par genre ;
        - construit les préférences par artiste ;
        - exclut les morceaux déjà écoutés ;
        - score les candidats avec genre + artiste + popularité.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        query = """
            WITH active_users AS (
                SELECT DISTINCT user_id
                FROM listening_events
                WHERE completed = TRUE
            ),
            listened_tracks AS (
                SELECT DISTINCT user_id, track_id
                FROM listening_events
                WHERE completed = TRUE
            ),
            user_genres AS (
                SELECT
                    le.user_id,
                    t.genre,
                    COUNT(*)::float AS genre_count
                FROM listening_events le
                JOIN tracks t ON t.id = le.track_id
                WHERE le.completed = TRUE
                  AND t.genre IS NOT NULL
                GROUP BY le.user_id, t.genre
            ),
            user_artists AS (
                SELECT
                    le.user_id,
                    t.artist_id,
                    COUNT(*)::float AS artist_count
                FROM listening_events le
                JOIN tracks t ON t.id = le.track_id
                WHERE le.completed = TRUE
                GROUP BY le.user_id, t.artist_id
            ),
            track_popularity AS (
                SELECT
                    track_id,
                    SUM(total_streams)::float AS popularity
                FROM daily_streams
                GROUP BY track_id
            ),
            candidates AS (
                SELECT
                    au.user_id,
                    tr.id AS track_id,
                    (
                        0.50 * COALESCE(ug.genre_count, 0)
                        + 0.30 * COALESCE(ua.artist_count, 0)
                        + 0.20 * LEAST(COALESCE(tp.popularity, 0), 100)
                        + 0.01 * random()
                    ) AS score
                FROM active_users au
                JOIN tracks tr ON TRUE
                LEFT JOIN user_genres ug
                    ON ug.user_id = au.user_id
                   AND ug.genre = tr.genre
                LEFT JOIN user_artists ua
                    ON ua.user_id = au.user_id
                   AND ua.artist_id = tr.artist_id
                LEFT JOIN track_popularity tp
                    ON tp.track_id = tr.id
                LEFT JOIN listened_tracks lt
                    ON lt.user_id = au.user_id
                   AND lt.track_id = tr.id
                WHERE lt.track_id IS NULL
                  AND (
                        ug.genre_count IS NOT NULL
                     OR ua.artist_count IS NOT NULL
                     OR tp.popularity IS NOT NULL
                  )
            ),
            ranked AS (
                SELECT
                    user_id::text,
                    track_id::text,
                    ROUND(score::numeric, 6)::float AS score,
                    ROW_NUMBER() OVER (
                        PARTITION BY user_id
                        ORDER BY score DESC, track_id
                    ) AS rank
                FROM candidates
            )
            SELECT user_id, track_id, score
            FROM ranked
            WHERE rank <= %s
            ORDER BY user_id, score DESC
        """

        rows = hook.get_records(query, parameters=(TOP_N_RECO,))

        recommendations = [
            {
                "user_id": row[0],
                "track_id": row[1],
                "score": float(row[2]),
            }
            for row in rows
        ]

        logger.info("Recommendations generated: total=%s", len(recommendations))
        return recommendations

    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: list[dict[str, Any]]) -> dict[str, int]:
        """
        Stocke les recommandations dans PostgreSQL et Redis.
        """
        if not recommendations:
            logger.warning("No recommendations to store")
            return {
                "users_with_recos": 0,
                "total_recommendations": 0,
            }

        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        insert_query = """
            INSERT INTO recommendations (user_id, track_id, score, generated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (user_id, track_id)
            DO UPDATE SET
                score = EXCLUDED.score,
                generated_at = NOW()
        """

        by_user: dict[str, list[dict[str, Any]]] = {}

        for reco in recommendations:
            by_user.setdefault(reco["user_id"], []).append(
                {
                    "track_id": reco["track_id"],
                    "score": reco["score"],
                }
            )

        conn = hook.get_conn()

        try:
            with conn.cursor() as cursor:
                for reco in recommendations:
                    cursor.execute(
                        insert_query,
                        (
                            reco["user_id"],
                            reco["track_id"],
                            reco["score"],
                        ),
                    )

                conn.commit()

            for user_id, recos in by_user.items():
                redis_client.setex(
                    f"reco:{user_id}",
                    RECO_TTL_SECONDS,
                    json.dumps(recos, ensure_ascii=False),
                )

        except Exception:
            conn.rollback()
            logger.exception("Error while storing recommendations")
            raise

        finally:
            conn.close()

        logger.info(
            "Recommendations stored: users=%s, total=%s",
            len(by_user),
            len(recommendations),
        )

        return {
            "users_with_recos": len(by_user),
            "total_recommendations": len(recommendations),
        }

    recos = generate_recommendations()
    wait_for_aggregation >> recos
    store_recommendations(recos)
