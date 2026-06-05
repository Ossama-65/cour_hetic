"""
DAG : recommendation_pipeline
================================
Génère les recommandations personnalisées via collaborative filtering
et les stocke dans Redis + PostgreSQL.

Dépend de aggregation_pipeline via ExternalTaskSensor.

TODO :
    [x] Implémenter build_user_track_matrix()
    [x] Implémenter compute_recommendations()
    [x] Implémenter store_recommendations()
    [x] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor

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
1. Construire la matrice user × track (écoutes des 7 derniers jours)
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
RECO_TTL_SECONDS = 86400   # 24 heures
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
        """
        Construit la matrice user × track des écoutes des 7 derniers jours.

        TODO :
            1. Requête SQL :
               SELECT user_id, track_id, COUNT(*) as play_count
               FROM listening_events
               WHERE timestamp >= NOW() - INTERVAL '7 days'
                 AND completed = TRUE
               GROUP BY user_id, track_id
            2. Construire un dict {user_id: {track_id: play_count}}
            3. Ne garder que les utilisateurs avec >= 3 écoutes distinctes
            4. Retourner la matrice + la liste des users actifs
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import logging

        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT user_id, track_id, COUNT(*) as play_count
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '7 days'
              AND completed = TRUE
            GROUP BY user_id, track_id
        """)
        rows = cursor.fetchall()
        cursor.close()

        # Construire la matrice {user_id: {track_id: play_count}}
        matrix = {}
        for user_id, track_id, play_count in rows:
            user_id = str(user_id)
            track_id = str(track_id)
            if user_id not in matrix:
                matrix[user_id] = {}
            matrix[user_id][track_id] = play_count

        # Garder uniquement les users avec >= 3 écoutes distinctes
        matrix = {u: tracks for u, tracks in matrix.items() if len(tracks) >= 3}

        logging.info(f"Matrice construite : {len(matrix)} utilisateurs actifs")
        return {"matrix": matrix, "users": list(matrix.keys())}

    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict, **context) -> dict:
        """
        Calcule les recommandations par similarité cosinus.

        TODO :
            1. Convertir la matrice en numpy array ou DataFrame sparse
            2. Calculer la similarité cosinus entre utilisateurs
            3. Pour chaque user : trouver ses TOP_N voisins les plus similaires
            4. Recommander les tracks que ses voisins ont aimés mais qu'il n'a pas écoutés
            5. Retourner {user_id: [track_id_1, track_id_2, ...]} (top TOP_N_RECO)
        """
        import numpy as np
        import logging

        matrix = matrix_data.get("matrix", {})
        users = matrix_data.get("users", [])

        if not users:
            logging.info("Aucun utilisateur actif, pas de recommandations")
            return {}

        # Construire la liste de tous les tracks
        all_tracks = list({track for tracks in matrix.values() for track in tracks})
        track_idx = {t: i for i, t in enumerate(all_tracks)}
        user_idx = {u: i for i, u in enumerate(users)}

        # Construire la matrice numpy
        mat = np.zeros((len(users), len(all_tracks)))
        for user, tracks in matrix.items():
            for track, count in tracks.items():
                mat[user_idx[user]][track_idx[track]] = count

        # Similarité cosinus
        norms = np.linalg.norm(mat, axis=1, keepdims=True)
        norms[norms == 0] = 1
        mat_norm = mat / norms
        similarity = np.dot(mat_norm, mat_norm.T)

        recommendations = {}
        for user in users:
            ui = user_idx[user]
            # Trouver les voisins les plus similaires (hors lui-même)
            sim_scores = similarity[ui].copy()
            sim_scores[ui] = 0
            top_neighbors = np.argsort(sim_scores)[::-1][:5]

            # Tracks déjà écoutés par l'utilisateur
            already_listened = set(matrix[user].keys())

            # Recommander les tracks des voisins
            scores = {}
            for ni in top_neighbors:
                neighbor = users[ni]
                weight = sim_scores[ni]
                if weight <= 0:
                    continue
                for track, count in matrix[neighbor].items():
                    if track not in already_listened:
                        scores[track] = scores.get(track, 0) + weight * count

            # Top N recommandations
            top_tracks = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:TOP_N_RECO]
            if top_tracks:
                recommendations[user] = [{"track_id": t, "score": float(s)} for t, s in top_tracks]

        logging.info(f"Recommandations calculées pour {len(recommendations)} utilisateurs")
        return recommendations

    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict, **context) -> dict:
        """
        Stocke les recommandations dans Redis et PostgreSQL.

        TODO :
            1. Redis : pour chaque user_id :
               redis.setex(f'reco:{user_id}', RECO_TTL_SECONDS, json.dumps(track_ids))
            2. PostgreSQL : UPSERT dans recommendations
            3. Retourner {"users_with_recos": N, "total_recommendations": M}
        """
        import redis
        import json
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import logging

        if not recommendations:
            logging.info("Aucune recommandation à stocker")
            return {"users_with_recos": 0, "total_recommendations": 0}

        r = redis.from_url(REDIS_URL, decode_responses=True)
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cursor = conn.cursor()

        total = 0
        for user_id, tracks in recommendations.items():
            track_ids = [t["track_id"] for t in tracks]

            # Stocker dans Redis
            r.setex(f"reco:{user_id}", RECO_TTL_SECONDS, json.dumps(track_ids))

            # Stocker dans PostgreSQL
            for item in tracks:
                cursor.execute("""
                    INSERT INTO recommendations (user_id, track_id, score, generated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (user_id, track_id) DO UPDATE SET
                        score        = EXCLUDED.score,
                        generated_at = NOW()
                """, (user_id, item["track_id"], item["score"]))
                total += 1

        conn.commit()
        cursor.close()

        logging.info(f"Recommandations stockées : {len(recommendations)} users, {total} entrées")
        return {"users_with_recos": len(recommendations), "total_recommendations": total}

    # ── Orchestration ─────────────────────────────────────────
    matrix          = build_user_track_matrix()
    recommendations = compute_recommendations(matrix)

    wait_for_aggregation >> matrix
    store_recommendations(recommendations)