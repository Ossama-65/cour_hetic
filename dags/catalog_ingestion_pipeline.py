"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.

Planification : quotidienne à 02:00 UTC
Catchup       : activé (permet le backfill historique)
"""

from datetime import datetime, timedelta
import os
import json
import boto3

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# DOCUMENTATION DU DAG
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
}

POSTGRES_CONN_ID = "spotify_postgres"
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
        """Télécharge et extrait les fichiers JSON des labels depuis MinIO."""
        s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin'
        )
        
        catalogues = []
        for file_key in LABEL_FILES:
            try:
                print(f"Extraction de : {file_key}")
                response = s3_client.get_object(Bucket=MINIO_BUCKET, Key=file_key)
                data = json.loads(response['Body'].read().decode('utf-8'))
                catalogues.append(data)
            except Exception as e:
                print(f"⚠️ Fichier manquant ou corrompu ({file_key}) : {e}. On continue...")
                
        return catalogues

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list[dict]) -> dict:
        """Valide la présence des champs requis et isole les erreurs en DLQ."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        valid_data = {"artists": [], "albums": [], "tracks": []}
        errors_count = 0
        
        dlq_query = """
            INSERT INTO dead_letter_events (event_type, payload, error_type, created_at)
            VALUES (%s, %s, %s, NOW())
        """
        
        for catalog in raw_catalogs:
            label_name = catalog.get("label", "Unknown")
            
            # 1. Validation Artistes
            for artist in catalog.get("artists", []):
                if all(k in artist for k in ("id", "name")):
                    artist["label"] = label_name
                    valid_data["artists"].append(artist)
                else:
                    pg_hook.run(dlq_query, parameters=("artist", json.dumps(artist), "schema_validation"))
                    errors_count += 1
                    
            # 2. Validation Albums
            for album in catalog.get("albums", []):
                if all(k in album for k in ("id", "artist_id", "title")):
                    valid_data["albums"].append(album)
                else:
                    pg_hook.run(dlq_query, parameters=("album", json.dumps(album), "schema_validation"))
                    errors_count += 1
                    
            # 3. Validation Morceaux (Tracks)
            for track in catalog.get("tracks", []):
                if all(k in track for k in ("id", "artist_id", "title", "duration_ms")):
                    valid_data["tracks"].append(track)
                else:
                    pg_hook.run(dlq_query, parameters=("track", json.dumps(track), "schema_validation"))
                    errors_count += 1
                    
        return {"valid": valid_data, "errors_count": errors_count}

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        """Nettoie, normalise et déduplique les données."""
        data = validated["valid"]
        
        transformed_artists = {}
        transformed_albums = []
        transformed_tracks = []
        
        # 1. Transformation Artistes & Suppression Doublons
        for artist in data["artists"]:
            clean_name = artist["name"].strip().title()
            artist_id = artist["id"]
            transformed_artists[artist_id] = (
                artist_id,
                clean_name,
                artist["label"],
                artist.get("genres", []),
                artist.get("monthly_listeners", 0)
            )
            
        # 2. Transformation Albums (release_date supprimée pour correspondre au schéma PG)
        for album in data["albums"]:
            transformed_albums.append((
                album["id"],
                album["artist_id"],
                album["title"].strip()
            ))
            
        # 3. Transformation Tracks
        for track in data["tracks"]:
            duration = track["duration_ms"]
            if 0 < duration < 3600000:
                transformed_tracks.append((
                    track["id"],
                    track["artist_id"],
                    track["title"].strip(),
                    duration,
                    track.get("explicit", False)
                ))
            else:
                print(f"⚠️ Track {track['id']} écartée car durée invalide : {duration}ms")
                
        return {
            "artists": list(transformed_artists.values()),
            "albums": transformed_albums,
            "tracks": transformed_tracks,
            "errors_count": validated["errors_count"]
        }

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        """Insère les données de façon idempotente (Upsert) avec de hautes performances."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        
        try:
            # 1. Upsert Artistes
            artist_query = """
                INSERT INTO artists (id, name, label, genres, monthly_listeners)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) 
                DO UPDATE SET 
                    name = EXCLUDED.name,
                    monthly_listeners = EXCLUDED.monthly_listeners,
                    updated_at = NOW();
            """
            if transformed["artists"]:
                cursor.executemany(artist_query, transformed["artists"])
                
            # 2. Upsert Albums (Simplifié avec DO NOTHING car pas de colonnes updated_at ou release_date)
            album_query = """
                INSERT INTO albums (id, artist_id, title)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) 
                DO NOTHING;
            """
            if transformed["albums"]:
                clean_albums = [(a[0], a[1], a[2]) for a in transformed["albums"]]
                cursor.executemany(album_query, clean_albums)
                
            # 3. Upsert Tracks
            track_query = """
                INSERT INTO tracks (id, artist_id, title, duration_ms, explicit)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) 
                DO UPDATE SET 
                    title = EXCLUDED.title,
                    duration_ms = EXCLUDED.duration_ms,
                    updated_at = NOW();
            """
            if transformed["tracks"]:
                cursor.executemany(track_query, transformed["tracks"])
                
            connection.commit()
            
            stats = {
                "artists_inserted": len(transformed["artists"]),
                "albums_inserted": len(transformed["albums"]),
                "tracks_inserted": len(transformed["tracks"]),
                "errors_count": transformed["errors_count"]
            }
            
            for key, value in stats.items():
                context['ti'].xcom_push(key=key, value=value)
                
            return stats
            
        except Exception as e:
            connection.rollback()
            raise e
        finally:
            cursor.close()
            connection.close()

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        """Affiche les statistiques de succès dans la console Airflow."""
        dag_run = context["dag_run"]
        print(f"""
        ✅ catalog_ingestion_pipeline terminé avec succès !
        DAGRun : {dag_run.run_id}
        --------------------------------──────────────────
        Artists insérés/mis à jour : {stats.get('artists_inserted', 0)}
        Albums insérés/mis à jour  : {stats.get('albums_inserted', 0)}
        Tracks insérées/mises à jour  : {stats.get('tracks_inserted', 0)}
        Erreurs envoyées en DLQ       : {stats.get('errors_count', 0)}
        --------------------------------──────────────────
        """)

    # ── Orchestration des tâches ──────────────────────────────
    raw          = extract_from_minio()
    validated    = validate_schema(raw)
    transformed  = transform_catalog(validated)
    stats        = load_to_postgres(transformed)
    notify_success(stats)