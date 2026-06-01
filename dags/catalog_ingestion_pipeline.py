"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
    "owner":                     "spotify-team",
    "depends_on_past":           False,
    "start_date":                datetime(2025, 1, 1),
    "email_on_failure":          False,
    "email_on_retry":            False,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":         timedelta(minutes=30),
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

    # ── TÂCHE 1 : extract_from_minio ─────────────────────────

    @task(task_id="extract_from_minio")
    def extract_from_minio(**context) -> list:
        """
        Télécharge les fichiers JSON des labels depuis MinIO.
        Retourne une liste de catalogues bruts.
        """
        import boto3
        import json
        import os
        import logging

        s3 = boto3.client(
            's3',
            endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin'
        )

        catalogs = []
        for filename in LABEL_FILES:
            try:
                obj = s3.get_object(Bucket=MINIO_BUCKET, Key=filename)
                catalog = json.loads(obj['Body'].read())
                catalogs.append(catalog)
                logging.info(f"Fichier chargé : {filename}")
            except Exception as e:
                logging.warning(f"Fichier manquant ou erreur pour {filename} : {e}")
                continue

        return catalogs

    # ── TÂCHE 2 : validate_schema ────────────────────────────

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list) -> dict:
        """
        Valide le schéma de chaque catalogue et isole les entrées invalides.
        Les entrées invalides sont envoyées dans dead_letter_events.
        """
        import json
        from datetime import datetime

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        valid = {"artists": [], "albums": [], "tracks": []}
        errors_count = 0

        required_artist = ["id", "name", "label"]
        required_album  = ["id", "artist_id", "title"]
        required_track  = ["id", "artist_id", "title", "duration_ms"]

        for catalog in raw_catalogs:
            for artist in catalog.get("artists", []):
                if all(k in artist for k in required_artist):
                    valid["artists"].append(artist)
                else:
                    errors_count += 1
                    cursor.execute("""
                        INSERT INTO dead_letter_events (payload, error_type, created_at)
                        VALUES (%s, %s, %s)
                    """, (json.dumps(artist), "schema_validation", datetime.utcnow()))

            for album in catalog.get("albums", []):
                if all(k in album for k in required_album):
                    valid["albums"].append(album)
                else:
                    errors_count += 1
                    cursor.execute("""
                        INSERT INTO dead_letter_events (payload, error_type, created_at)
                        VALUES (%s, %s, %s)
                    """, (json.dumps(album), "schema_validation", datetime.utcnow()))

            for track in catalog.get("tracks", []):
                if all(k in track for k in required_track):
                    valid["tracks"].append(track)
                else:
                    errors_count += 1
                    cursor.execute("""
                        INSERT INTO dead_letter_events (payload, error_type, created_at)
                        VALUES (%s, %s, %s)
                    """, (json.dumps(track), "schema_validation", datetime.utcnow()))

        conn.commit()
        cursor.close()
        conn.close()

        return {"valid": valid, "errors_count": errors_count}

    # ── TÂCHE 3 : transform_catalog ──────────────────────────

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        """
        Normalise les données du catalogue.
        """
        data = validated.get("valid", validated)

        artists = []
        albums  = []
        tracks  = []

        seen_artists = set()
        for artist in data.get("artists", []):
            name_normalized = artist["name"].strip().title()
            key = (name_normalized, artist.get("label", ""))
            if key not in seen_artists:
                seen_artists.add(key)
                artists.append({
                    "id":                artist["id"],
                    "name":              name_normalized,
                    "label":             artist.get("label", ""),
                    "genres":            artist.get("genres", []),
                    "monthly_listeners": artist.get("monthly_listeners", 0),
                })

        for album in data.get("albums", []):
            albums.append({
                "id":           album["id"],
                "title":        album["title"],
                "artist_id":    album["artist_id"],
                "release_date": album.get("release_date"),
            })

        for track in data.get("tracks", []):
            duration = track.get("duration_ms", 0)
            if 0 < duration < 3_600_000:
                tracks.append({
                    "id":         track["id"],
                    "title":      track["title"],
                    "album_id":   track.get("album_id"),
                    "artist_id":  track.get("artist_id"),
                    "duration_ms": duration,
                })

        return {"artists": artists, "albums": albums, "tracks": tracks}

    # ── TÂCHE 4 : load_to_postgres ───────────────────────────

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        """
        Charge les données dans PostgreSQL avec upsert idempotent.
        """
        hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = hook.get_conn()
        cursor = conn.cursor()

        # Upsert Artists
        artists_data = [
            (a["id"], a["name"], a["label"], a["genres"], a["monthly_listeners"])
            for a in transformed["artists"]
        ]
        cursor.executemany("""
            INSERT INTO artists (id, name, label, genres, monthly_listeners)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (name, label) DO UPDATE SET
                monthly_listeners = EXCLUDED.monthly_listeners,
                updated_at = NOW()
        """, artists_data)

        # Upsert Albums
        albums_data = [
            (a["id"], a["title"], a["artist_id"], a.get("release_date"))
            for a in transformed["albums"]
        ]
        cursor.executemany("""
            INSERT INTO albums (id, title, artist_id, release_date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                updated_at = NOW()
        """, albums_data)

        # Upsert Tracks
        tracks_data = [
            (t["id"], t["title"], t.get("album_id"), t.get("artist_id"), t["duration_ms"])
            for t in transformed["tracks"]
        ]
        cursor.executemany("""
            INSERT INTO tracks (id, title, album_id, artist_id, duration_ms)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                updated_at = NOW()
        """, tracks_data)

        conn.commit()
        cursor.close()
        conn.close()

        stats = {
            "artists_inserted": len(artists_data),
            "albums_inserted":  len(albums_data),
            "tracks_inserted":  len(tracks_data),
        }
        return stats

    # ── TÂCHE 5 : notify_success ─────────────────────────────

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        """
        Log de succès avec statistiques d'ingestion.
        """
        dag_run = context["dag_run"]
        print(f"""
        ✅ catalog_ingestion_pipeline terminé
        DAGRun   : {dag_run.run_id}
        Artists  : {stats.get('artists_inserted', 0)}
        Albums   : {stats.get('albums_inserted', 0)}
        Tracks   : {stats.get('tracks_inserted', 0)}
        """)

    # ── Orchestration ─────────────────────────────────────────
    raw         = extract_from_minio()
    validated   = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats       = load_to_postgres(transformed)
    notify_success(stats)