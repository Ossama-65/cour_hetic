"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.

Planification : quotidienne à 02:00 UTC
Catchup       : activé (permet le backfill historique)

Architecture :
    MinIO (labels/*.json)
        → extract_from_minio()
        → validate_schema()
        → transform_catalog()        ← normalisation, dédoublonnage
        → load_to_postgres()         ← upsert avec ON CONFLICT
        → notify_success()

TODO :
    [x] Implémenter extract_from_minio() — lire les JSONs depuis MinIO
    [x] Implémenter validate_schema() — vérifier les champs obligatoires
    [x] Implémenter transform_catalog() — normaliser les noms d'artistes, déduplication
    [x] Implémenter load_to_postgres() — upsert avec gestion des conflits
    [x] Configurer retry_delay et retries sur les tâches réseau
    [x] Ajouter un on_failure_callback pour alerting
    [x] Activer le doc_md sur ce DAG (voir variable DAG_DOC ci-dessous)
"""

from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ─────────────────────────────────────────────────────────────
# CALLBACKS
# ─────────────────────────────────────────────────────────────

def on_failure_alert(context):
    """Alerte en cas d'échec d'une tâche du pipeline."""
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    err = context.get('exception')
    print(f"❌ ALERTE : Échec du DAG {dag_id} sur la tâche {task_id}. Erreur : {err}")

# ─────────────────────────────────────────────────────────────
# DOCUMENTATION DU DAG (obligatoire pour la note)
# ─────────────────────────────────────────────────────────────

DAG_DOC = """
## catalog_ingestion_pipeline

### Rôle
Ingère les métadonnées musicales depuis les fichiers JSON de 3 labels
(SunSet Records, NightWave Music, Urban Pulse) stockés dans MinIO.

### Sources
- `s3://labels-raw/sunset_records.json`
- `s3://labels-raw/nightwave_music.json`
- `s3://labels-raw/urban_pulse.json`

### Destinations
- Table `artists` (upsert)
- Table `albums` (upsert)
- Table `tracks` (upsert)

### Idempotence
Le pipeline est idempotent : relancer plusieurs fois le même DAGrun
produit le même résultat grâce aux upserts ON CONFLICT DO UPDATE.

### Gestion des erreurs
- Schéma invalide → événement en DLQ (`dead_letter_events`)
- MinIO indisponible → retry x3 avec backoff exponentiel

### Monitoring
- XCom `tracks_inserted` : nombre de tracks insérées/mises à jour
- XCom `errors_count` : nombre d'entrées envoyées en DLQ
"""

# ─────────────────────────────────────────────────────────────
# CONFIGURATION PAR DÉFAUT
# ─────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":                 "spotify-team",
    "depends_on_past":       False,
    "start_date":            datetime(2025, 1, 1),
    "email_on_failure":      False,
    "email_on_retry":        False,
    "retries":               3,
    "retry_delay":           timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":     timedelta(minutes=30),
    "on_failure_callback":   on_failure_alert,
}

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_CONN_ID    = "spotify_minio"
MINIO_BUCKET     = "labels-raw"
LABEL_FILES      = ["sunset_records.json", "nightwave_music.json", "urban_pulse.json"]


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion quotidienne du catalogue musical depuis MinIO vers PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion", "catalogue"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="extract_from_minio")
    def extract_from_minio(**context) -> list[dict]:
        """
        Télécharge les fichiers JSON des labels depuis MinIO.

        TODO :
            1. Se connecter à MinIO via AwsBaseHook ou boto3
               (endpoint_url = http://minio:9000)
            2. Pour chaque fichier dans LABEL_FILES, télécharger et parser le JSON
            3. Retourner une liste de catalogues : [catalog_label_a, catalog_label_b, ...]
            4. Si un fichier est manquant : logger un warning et continuer
               (pas de crash — on traite ce qu'on a)

        Returns:
            list[dict] : catalogues bruts des labels
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        raw_catalogs = []
        
        for file_name in LABEL_FILES:
            try:
                # L'endpoint_url est configuré dans la connexion Airflow 'spotify_minio'
                file_content = s3_hook.read_key(key=file_name, bucket_name=MINIO_BUCKET)
                raw_catalogs.append(json.loads(file_content))
                print(f"Extraction réussie pour : {file_name}")
            except Exception as e:
                print(f"Attention : Impossible d'extraire {file_name} depuis MinIO : {e}")
        
        return raw_catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list[dict]) -> dict:
        """
        Valide le schéma de chaque catalogue et isole les entrées invalides.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        valid_entries = {"artists": [], "albums": [], "tracks": []}
        errors_count = 0
        
        for catalog in raw_catalogs:
            # Validation Artistes
            for artist in catalog.get("artists", []):
                if all(k in artist for k in ("id", "name", "label")):
                    valid_entries["artists"].append(artist)
                else:
                    errors_count += 1
                    hook.run("INSERT INTO dead_letter_events (payload, error_type, original_topic) VALUES (%s, %s, %s)", 
                             parameters=(json.dumps(artist), "schema_validation_artist", "catalog_ingestion"))

            # Validation Albums
            for album in catalog.get("albums", []):
                if all(k in album for k in ("id", "artist_id", "title")):
                    valid_entries["albums"].append(album)
                else:
                    errors_count += 1
                    hook.run("INSERT INTO dead_letter_events (payload, error_type, original_topic) VALUES (%s, %s, %s)", 
                             parameters=(json.dumps(album), "schema_validation_album", "catalog_ingestion"))

            # Validation Tracks
            for track in catalog.get("tracks", []):
                if all(k in track for k in ("id", "artist_id", "title", "duration_ms")):
                    valid_entries["tracks"].append(track)
                else:
                    errors_count += 1
                    hook.run("INSERT INTO dead_letter_events (payload, error_type, original_topic) VALUES (%s, %s, %s)", 
                             parameters=(json.dumps(track), "schema_validation_track", "catalog_ingestion"))
                             
        return {"valid": valid_entries, "errors_count": errors_count}

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        valid_data = validated["valid"]
        
        # Normalisation Artistes
        unique_artists = []
        seen = set()
        for a in valid_data["artists"]:
            norm_name = a["name"].strip().title()
            key = (norm_name, a["label"])
            if key not in seen:
                a["name"] = norm_name
                unique_artists.append(a)
                seen.add(key)
        
        # Validation durées Tracks
        valid_tracks = [t for t in valid_data["tracks"] if 0 < t["duration_ms"] < 3_600_000]
                
        return {
            "artists": unique_artists,
            "albums": valid_data["albums"],
            "tracks": valid_tracks
        }

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Préparation des données pour executemany (plus performant)
        artists_data = [(a['id'], a['name'], a.get('country'), a['label']) for a in transformed['artists']]
        albums_data = [(alb['id'], alb['artist_id'], alb['title'], alb.get('release_year')) for alb in transformed['albums']]
        tracks_data = [
            (t['id'], t.get('album_id'), t['artist_id'], t['title'], t['duration_ms'], t.get('genre')) 
            for t in transformed['tracks']
        ]

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                # Upsert Artists
                if artists_data:
                    cur.executemany("""
                        INSERT INTO artists (id, name, country, label) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (name, label) DO UPDATE SET updated_at = NOW()
                    """, artists_data)

                # Upsert Albums
                if albums_data:
                    cur.executemany("""
                        INSERT INTO albums (id, artist_id, title, release_year) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title
                    """, albums_data)

                # Upsert Tracks
                if tracks_data:
                    cur.executemany("""
                        INSERT INTO tracks (id, album_id, artist_id, title, duration_ms, genre) 
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET updated_at = NOW()
                    """, tracks_data)
                
                conn.commit()

        stats = {
            "artists_inserted": len(transformed['artists']),
            "albums_inserted": len(transformed['albums']),
            "tracks_inserted": len(transformed['tracks']),
            "errors_count": context['ti'].xcom_pull(task_ids='validate_schema').get('errors_count', 0)
        }
        
        context['ti'].xcom_push(key='tracks_inserted', value=stats['tracks_inserted'])
        return stats

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        """
        Log de succès avec statistiques d'ingestion.
        Optionnel : envoyer une notification (webhook Slack simulé).
        """
        dag_run = context["dag_run"]
        print(f"""
        ✅ catalog_ingestion_pipeline terminé
        DAGRun : {dag_run.run_id}
        Tracks insérées  : {stats.get('tracks_inserted', 0)}
        Artists insérés  : {stats.get('artists_inserted', 0)}
        Erreurs DLQ      : {stats.get('errors_count', 0)}
        """)

    # ── Orchestration des tâches ──────────────────────────────
    raw       = extract_from_minio()
    validated = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats     = load_to_postgres(transformed)
    notify_success(stats)
