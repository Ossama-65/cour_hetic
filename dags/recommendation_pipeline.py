"""
DAG : recommendation_pipeline
================================
Génère les recommandations personnalisées via collaborative filtering
et les stocke dans Redis + PostgreSQL.

Dépend de aggregation_pipeline via ExternalTaskSensor.

Architecture :
    ExternalTaskSensor (attend aggregation_pipeline)
        → build_user_track_matrix()   ← matrice user×track des 7 derniers jours
        → compute_recommendations()   ← similarité cosinus entre utilisateurs
        → store_recommendations()     ← Redis (TTL 24h) + PostgreSQL
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
1. Construire la matrice user × track (écoutes des 7 derniers jours)
2. Calculer la similarité cosinus entre utilisateurs (sklearn)
3. Pour chaque user, recommander les tracks aimés par ses voisins
   qu'il n'a pas encore écoutés (top TOP_N_RECO)
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
RECO_TTL_SECONDS = 86_400   # 24 heures
TOP_N_RECO       = 10
TOP_N_NEIGHBORS  = 5
LOOKBACK_DAYS    = 7
MIN_LISTENS      = 3        # écoutes minimum pour être inclus


def _get_latest_aggregation_run(dt):
    """Retourne l'execution_date du dernier run réussi de aggregation_pipeline."""
    from airflow.models import DagRun
    from airflow.utils.session import create_session
    from airflow.utils.state import State

    with create_session() as session:
        run = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == "aggregation_pipeline",
                DagRun.state == State.SUCCESS,
                DagRun.execution_date <= dt,
            )
            .order_by(DagRun.execution_date.desc())
            .first()
        )
        return run.execution_date if run else dt


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
        execution_date_fn=_get_latest_aggregation_run,
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="build_user_track_matrix")
    def build_user_track_matrix(**context) -> dict:
        """
        Construit la matrice user × track des écoutes des LOOKBACK_DAYS derniers jours.
        Ne retient que les utilisateurs avec au moins MIN_LISTENS écoutes distinctes.

        Returns:
            dict: {
                "matrix": {user_id: {track_id: play_count}},
                "active_users": [user_id, ...],
                "all_tracks":   [track_id, ...]
            }
        """
        logger = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT user_id::text, track_id::text, COUNT(*) AS play_count
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '%s days'
              AND completed = TRUE
            GROUP BY user_id, track_id
        """, (LOOKBACK_DAYS,))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Construire {user_id: {track_id: play_count}}
        matrix: dict = {}
        for user_id, track_id, play_count in rows:
            matrix.setdefault(user_id, {})[track_id] = int(play_count)

        # Filtrer les utilisateurs avec assez d'écoutes distinctes
        active_users = [u for u, tracks in matrix.items() if len(tracks) >= MIN_LISTENS]
        all_tracks   = list({t for tracks in matrix.values() for t in tracks})

        logger.info(
            "Matrice user×track : %d users actifs, %d tracks uniques",
            len(active_users), len(all_tracks),
        )
        return {
            "matrix":       matrix,
            "active_users": active_users,
            "all_tracks":   all_tracks,
        }

    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict, **context) -> dict:
        """
        Calcule les recommandations par similarité cosinus entre utilisateurs.
        Pour chaque user actif : top-10 tracks non écoutés aimés par ses voisins.

        Returns:
            dict: {user_id: [track_id, ...]}
        """
        import numpy as np
        from sklearn.metrics.pairwise import cosine_similarity

        logger = logging.getLogger(__name__)

        matrix    = matrix_data["matrix"]
        users     = matrix_data["active_users"]
        tracks    = matrix_data["all_tracks"]

        if not users or not tracks:
            logger.info("Pas assez de données pour calculer des recommandations.")
            return {}

        # Index pour la matrice numpy
        user_idx  = {u: i for i, u in enumerate(users)}
        track_idx = {t: i for i, t in enumerate(tracks)}

        # Construire la matrice numpy (users × tracks)
        mat = np.zeros((len(users), len(tracks)), dtype=np.float32)
        for user_id in users:
            for track_id, count in matrix[user_id].items():
                if track_id in track_idx:
                    mat[user_idx[user_id], track_idx[track_id]] = count

        # Similarité cosinus
        sim = cosine_similarity(mat)

        recommendations: dict = {}

        for user_id in users:
            u_idx = user_idx[user_id]
            already_listened = set(matrix[user_id].keys())

            # Top voisins (hors soi-même)
            neighbor_scores = sim[u_idx].copy()
            neighbor_scores[u_idx] = -1  # exclure soi-même
            top_neighbors = np.argsort(neighbor_scores)[::-1][:TOP_N_NEIGHBORS]

            # Score agrégé pour chaque track non écouté
            track_scores: dict = {}
            for n_idx in top_neighbors:
                neighbor_id = users[n_idx]
                weight = float(sim[u_idx, n_idx])
                if weight <= 0:
                    continue
                for t_id, count in matrix.get(neighbor_id, {}).items():
                    if t_id not in already_listened:
                        track_scores[t_id] = track_scores.get(t_id, 0.0) + weight * count

            # Top-N recommandations
            top_tracks = sorted(track_scores, key=track_scores.get, reverse=True)[:TOP_N_RECO]
            if top_tracks:
                recommendations[user_id] = top_tracks

        logger.info(
            "Recommandations calculées pour %d / %d utilisateurs actifs",
            len(recommendations), len(users),
        )
        return recommendations

    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict, **context) -> dict:
        """
        Stocke les recommandations dans Redis (TTL 24h) et PostgreSQL (upsert).

        Returns:
            dict: {"users_with_recos": N, "total_recommendations": M}
        """
        import redis as redis_lib

        logger = logging.getLogger(__name__)

        if not recommendations:
            logger.info("Aucune recommandation à stocker.")
            return {"users_with_recos": 0, "total_recommendations": 0}

        # ── Redis ────────────────────────────────────────────
        r = redis_lib.from_url(REDIS_URL, decode_responses=True)
        pipe = r.pipeline()
        for user_id, track_ids in recommendations.items():
            key = f"reco:{user_id}"
            pipe.setex(key, RECO_TTL_SECONDS, json.dumps(track_ids))
        pipe.execute()

        # ── PostgreSQL ───────────────────────────────────────
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        sql = """
            INSERT INTO recommendations (user_id, track_id, score, generated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (user_id, track_id) DO UPDATE SET
                score        = EXCLUDED.score,
                generated_at = NOW()
        """
        total = 0
        for user_id, track_ids in recommendations.items():
            for rank, track_id in enumerate(track_ids):
                score = round(1.0 - rank * 0.1, 2)  # score décroissant
                cur.execute(sql, (user_id, track_id, score))
                total += 1

        conn.commit()
        cur.close()

        stats = {
            "users_with_recos":    len(recommendations),
            "total_recommendations": total,
        }
        logger.info("Recommandations stockées : %s", stats)
        return stats

    # ── Orchestration ─────────────────────────────────────────
    matrix          = build_user_track_matrix()
    recommendations = compute_recommendations(matrix)

    wait_for_aggregation >> matrix
    store_recommendations(recommendations)
