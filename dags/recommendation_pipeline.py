"""
DAG : recommendation_pipeline
=============================

Genere des recommandations musicales personnalisees a partir des ecoutes
utilisateurs et les stocke dans Redis + PostgreSQL.

Destination PostgreSQL :
    recommendations(user_id, track_id, score, generated_at)
"""

import json
import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta

import redis
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor


logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_URL = "redis://redis:6379/1"

RECO_TTL_SECONDS = 86400
TOP_N_RECO = 10
LOOKBACK_DAYS = 7


DAG_DOC = """
## recommendation_pipeline

Ce DAG genere des recommandations musicales personnalisees.

### Logique
1. Lire les ecoutes recentes dans `listening_events`
2. Construire une matrice user -> track -> nombre d'ecoutes
3. Calculer une similarite simple entre utilisateurs
4. Recommander des morceaux ecoutes par des utilisateurs proches
5. Eviter de recommander un morceau deja ecoute
6. Inserer les recommandations dans PostgreSQL avec un upsert idempotent
7. Stocker aussi les recommandations dans Redis avec une TTL de 24h
"""


def cosine_similarity(user_a: dict, user_b: dict) -> float:
    """
    Calcule une similarite cosinus simple entre deux profils utilisateurs.
    Chaque profil est un dict {track_id: play_count}.
    """
    common_tracks = set(user_a.keys()) & set(user_b.keys())

    if not common_tracks:
        return 0.0

    dot_product = sum(user_a[track] * user_b[track] for track in common_tracks)
    norm_a = math.sqrt(sum(count * count for count in user_a.values()))
    norm_b = math.sqrt(sum(count * count for count in user_b.values()))

    if norm_a == 0 or norm_b == 0:
        return 0.0

    return dot_product / (norm_a * norm_b)


with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Generate music recommendations from listening events",
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

    @task(task_id="build_user_track_matrix")
    def build_user_track_matrix() -> dict:
        """
        Construit la matrice user -> track -> play_count a partir des ecoutes.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        query = f"""
            SELECT
                user_id,
                track_id,
                COUNT(*) AS play_count
            FROM listening_events
            WHERE "timestamp" >= NOW() - INTERVAL '{LOOKBACK_DAYS} days'
              AND completed = TRUE
            GROUP BY user_id, track_id
        """

        rows = hook.get_records(query)

        matrix = defaultdict(dict)

        for user_id, track_id, play_count in rows:
            matrix[str(user_id)][str(track_id)] = int(play_count)

        filtered_matrix = {
            user_id: tracks
            for user_id, tracks in matrix.items()
            if len(tracks) >= 3
        }

        logger.info(
            "User-track matrix built: users=%s, filtered_users=%s, rows=%s",
            len(matrix),
            len(filtered_matrix),
            len(rows),
        )

        return {
            "matrix": filtered_matrix,
            "users": list(filtered_matrix.keys()),
        }

    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict) -> dict:
        """
        Calcule les recommandations par similarite entre utilisateurs.
        """
        matrix = matrix_data.get("matrix", {})
        users = matrix_data.get("users", [])

        recommendations = {}

        if not users:
            logger.warning("No active users found for recommendations")
            return recommendations

        for user_id in users:
            user_profile = matrix[user_id]
            listened_tracks = set(user_profile.keys())

            neighbor_scores = []

            for other_user_id in users:
                if other_user_id == user_id:
                    continue

                similarity = cosine_similarity(user_profile, matrix[other_user_id])

                if similarity > 0:
                    neighbor_scores.append((other_user_id, similarity))

            neighbor_scores.sort(key=lambda item: item[1], reverse=True)
            top_neighbors = neighbor_scores[:TOP_N_RECO]

            candidate_scores = defaultdict(float)

            for neighbor_id, similarity in top_neighbors:
                neighbor_profile = matrix[neighbor_id]

                for track_id, play_count in neighbor_profile.items():
                    if track_id not in listened_tracks:
                        candidate_scores[track_id] += similarity * play_count

            ranked_tracks = sorted(
                candidate_scores.items(),
                key=lambda item: item[1],
                reverse=True,
            )

            recommendations[user_id] = [
                {
                    "track_id": track_id,
                    "score": round(float(score), 6),
                }
                for track_id, score in ranked_tracks[:TOP_N_RECO]
            ]

        total_recommendations = sum(len(items) for items in recommendations.values())

        logger.info(
            "Recommendations computed: users=%s, total_recommendations=%s",
            len(recommendations),
            total_recommendations,
        )

        return recommendations

    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict) -> dict:
        """
        Stocke les recommandations dans Redis et PostgreSQL.
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

        total_recommendations = 0

        conn = hook.get_conn()

        try:
            with conn.cursor() as cursor:
                for user_id, recos in recommendations.items():
                    if not recos:
                        continue

                    redis_key = f"reco:{user_id}"
                    redis_payload = json.dumps(recos)

                    redis_client.setex(
                        redis_key,
                        RECO_TTL_SECONDS,
                        redis_payload,
                    )

                    for reco in recos:
                        cursor.execute(
                            insert_query,
                            (
                                user_id,
                                reco["track_id"],
                                reco["score"],
                            ),
                        )
                        total_recommendations += 1

            conn.commit()

        except Exception:
            conn.rollback()
            logger.exception("Error while storing recommendations")
            raise

        finally:
            conn.close()

        users_with_recos = sum(
            1
            for recos in recommendations.values()
            if len(recos) > 0
        )

        logger.info(
            "Recommendations stored: users_with_recos=%s, total=%s",
            users_with_recos,
            total_recommendations,
        )

        return {
            "users_with_recos": users_with_recos,
            "total_recommendations": total_recommendations,
        }

    matrix = build_user_track_matrix()
    recommendations = compute_recommendations(matrix)

    wait_for_aggregation >> matrix
    store_recommendations(recommendations)