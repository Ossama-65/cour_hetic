"""
DAG : recommendation_pipeline
================================
Génère les recommandations personnalisées via collaborative filtering
et les stocke dans Redis + PostgreSQL.

Dépend de aggregation_pipeline via ExternalTaskSensor.
"""

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

logger = logging.getLogger(__name__)

DAG_DOC = """
## recommendation_pipeline

### Rôle
Génère un top-10 de recommandations par utilisateur actif
via collaborative filtering (similarité cosinus entre profils d'écoute).

### Dépendances
Attend la fin de `aggregation_pipeline` via ExternalTaskSensor.

### Destinations
- Redis : clé `reco:{user_id}` → liste de track_ids (TTL 24h)
- PostgreSQL : table `recommendations`

### Algorithme
Collaborative filtering simplifié :
1. Construire la matrice user x track (écoutes des 7 derniers jours)
2. Calculer la similarité cosinus entre utilisateurs
3. Pour chaque user, recommander les tracks aimés par ses voisins
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_URL        = "redis://redis:6379/1"
RECO_TTL_SECONDS = 86400
TOP_N_RECO       = 10
LOOKBACK_DAYS    = 7


with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Collaborative filtering → recommandations Redis + PostgreSQL",
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
    def build_user_track_matrix(**context) -> dict:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT user_id::text, track_id::text, COUNT(*) as play_count
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '%s days'
              AND completed = TRUE
            GROUP BY user_id, track_id
        """ % LOOKBACK_DAYS)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        # {user_id: {track_id: play_count}}
        matrix = defaultdict(dict)
        for user_id, track_id, play_count in rows:
            matrix[str(user_id)][str(track_id)] = play_count

        # garder uniquement les users avec >= 3 écoutes distinctes
        active_users = {
            uid: tracks
            for uid, tracks in matrix.items()
            if len(tracks) >= 3
        }

        logger.info(f"{len(active_users)} utilisateurs actifs trouvés")
        return {"matrix": active_users, "users": list(active_users.keys())}

    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict, **context) -> dict:
        import numpy as np
        from sklearn.metrics.pairwise import cosine_similarity

        matrix = matrix_data.get("matrix", {})
        users  = list(matrix.keys())

        if len(users) < 2:
            logger.info("Pas assez d'utilisateurs pour calculer des recommandations")
            return {}

        # index des tracks
        all_tracks = list({t for tracks in matrix.values() for t in tracks})
        track_index = {t: i for i, t in enumerate(all_tracks)}

        # construire la matrice numpy
        mat = np.zeros((len(users), len(all_tracks)))
        for i, user in enumerate(users):
            for track, count in matrix[user].items():
                mat[i][track_index[track]] = count

        sim = cosine_similarity(mat)

        recommendations = {}
        for i, user in enumerate(users):
            user_tracks = set(matrix[user].keys())
            scores = defaultdict(float)

            # top 5 voisins les plus similaires (hors lui-même)
            neighbor_indices = np.argsort(sim[i])[::-1][1:6]
            for j in neighbor_indices:
                neighbor = users[j]
                weight   = sim[i][j]
                for track, count in matrix[neighbor].items():
                    if track not in user_tracks:
                        scores[track] += count * weight

            top_tracks = sorted(scores, key=scores.get, reverse=True)[:TOP_N_RECO]
            if top_tracks:
                recommendations[user] = top_tracks

        logger.info(f"Recommandations calculées pour {len(recommendations)} utilisateurs")
        return recommendations

    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict, **context) -> dict:
        import redis

        r = redis.from_url(REDIS_URL)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        total = 0
        for user_id, track_ids in recommendations.items():
            # Redis
            r.setex(f"reco:{user_id}", RECO_TTL_SECONDS, json.dumps(track_ids))

            # PostgreSQL
            for rank, track_id in enumerate(track_ids):
                score = 1.0 - (rank / TOP_N_RECO)
                cursor.execute("""
                    INSERT INTO recommendations (user_id, track_id, score, generated_at)
                    VALUES (%s::uuid, %s::uuid, %s, NOW())
                    ON CONFLICT (user_id, track_id)
                    DO UPDATE SET score = EXCLUDED.score, generated_at = NOW()
                """, (user_id, track_id, score))
                total += 1

        conn.commit()
        cursor.close()
        conn.close()

        stats = {
            "users_with_recos":    len(recommendations),
            "total_recommendations": total,
        }
        logger.info(f"Stockage terminé : {stats}")
        return stats

    # Orchestration
    matrix          = build_user_track_matrix()
    recommendations = compute_recommendations(matrix)

    wait_for_aggregation >> matrix
    store_recommendations(recommendations)
