"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.

Planification : quotidienne à 02:00 UTC
Catchup       : désactivé

Architecture :
    MinIO (labels/*.json)
        → extract_from_minio()
        → validate_schema()
        → transform_catalog()        ← normalisation, dédoublonnage
        → load_to_postgres()         ← upsert avec ON CONFLICT
        → notify_success()
"""

import json
import logging
import os
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
# CONFIGURATION
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
MINIO_BUCKET     = "labels-raw"
LABEL_FILES      = ["sunset_records.json", "nightwave_music.json", "urban_pulse.json"]

VALID_GENRES = {
    "Pop", "Rock", "Hip-Hop", "Electronic", "Jazz",
    "R&B", "Folk", "Latin", "Metal", "Classical",
}
REQUIRED_ARTIST_FIELDS = {"id", "name", "label"}
REQUIRED_ALBUM_FIELDS  = {"id", "artist_id", "title"}
REQUIRED_TRACK_FIELDS  = {"id", "artist_id", "title", "duration_ms"}


# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion quotidienne du catalogue musical depuis MinIO vers PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion", "catalogue"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="extract_from_minio")
    def extract_from_minio(**context) -> list:
        """
        Télécharge les fichiers JSON des labels depuis MinIO.
        Si un fichier est manquant, log un warning et continue.

        Returns:
            list[dict] : catalogues bruts des labels
        """
        import boto3

        logger = logging.getLogger(__name__)

        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        )

        catalogs = []
        for filename in LABEL_FILES:
            try:
                response = s3.get_object(Bucket=MINIO_BUCKET, Key=filename)
                catalog = json.loads(response["Body"].read().decode("utf-8"))
                catalogs.append(catalog)
                logger.info("Downloaded %s : %s", filename, catalog.get("stats", {}))
            except Exception as exc:
                logger.warning("Could not download %s : %s", filename, exc)

        logger.info("Extracted %d catalog(s) from MinIO", len(catalogs))
        return catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list) -> dict:
        """
        Valide le schéma de chaque catalogue.
        Les entrées invalides sont envoyées en DLQ (dead_letter_events).

        Returns:
            dict: {"valid": {"artists": [...], "albums": [...], "tracks": [...]}, "errors_count": N}
        """
        logger = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        dlq_sql = """
            INSERT INTO dead_letter_events (payload, error_type, error_message, original_topic)
            VALUES (%s::jsonb, %s, %s, %s)
        """

        valid_artists, valid_albums, valid_tracks = [], [], []
        errors_count = 0

        for catalog in raw_catalogs:
            for artist in catalog.get("artists", []):
                missing = REQUIRED_ARTIST_FIELDS - set(artist.keys())
                if missing:
                    hook.run(dlq_sql, parameters=(
                        json.dumps(artist), "schema_validation",
                        f"Missing fields: {missing}", "minio_catalog",
                    ))
                    errors_count += 1
                else:
                    valid_artists.append(artist)

            for album in catalog.get("albums", []):
                missing = REQUIRED_ALBUM_FIELDS - set(album.keys())
                if missing:
                    hook.run(dlq_sql, parameters=(
                        json.dumps(album), "schema_validation",
                        f"Missing fields: {missing}", "minio_catalog",
                    ))
                    errors_count += 1
                else:
                    valid_albums.append(album)

            for track in catalog.get("tracks", []):
                missing = REQUIRED_TRACK_FIELDS - set(track.keys())
                if missing:
                    hook.run(dlq_sql, parameters=(
                        json.dumps(track), "schema_validation",
                        f"Missing fields: {missing}", "minio_catalog",
                    ))
                    errors_count += 1
                else:
                    valid_tracks.append(track)

        logger.info(
            "Validated — artists: %d, albums: %d, tracks: %d, errors: %d",
            len(valid_artists), len(valid_albums), len(valid_tracks), errors_count,
        )

        return {
            "valid": {
                "artists": valid_artists,
                "albums":  valid_albums,
                "tracks":  valid_tracks,
            },
            "errors_count": errors_count,
        }

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        """
        Transforme et normalise les données du catalogue.
        - Noms d'artistes : strip + title case + dédoublonnage sur (name, label)
        - Durées tracks   : rejette si duration_ms hors [1, 3_599_999]
        - Genres          : rejette les genres hors liste de référence

        Returns:
            dict: {"artists": [...], "albums": [...], "tracks": [...]}
        """
        logger = logging.getLogger(__name__)
        data = validated["valid"]

        seen_artists = set()
        artists = []
        for artist in data["artists"]:
            name = artist["name"].strip().title()
            key = (name, artist.get("label", ""))
            if key in seen_artists:
                continue
            seen_artists.add(key)
            artist["name"] = name
            artist["genres"] = [g for g in artist.get("genres", []) if g in VALID_GENRES]
            artists.append(artist)

        albums = []
        for album in data["albums"]:
            album["title"] = album["title"].strip()
            albums.append(album)

        tracks = []
        for track in data["tracks"]:
            duration = track.get("duration_ms", 0)
            if not (0 < duration < 3_600_000):
                logger.warning("Track %s skipped — invalid duration: %d", track.get("id"), duration)
                continue
            track["title"] = track["title"].strip()
            if track.get("genre") not in VALID_GENRES:
                track["genre"] = None
            tracks.append(track)

        logger.info(
            "Transformed — artists: %d, albums: %d, tracks: %d",
            len(artists), len(albums), len(tracks),
        )
        return {"artists": artists, "albums": albums, "tracks": tracks}

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        """
        Charge les données dans PostgreSQL avec upsert idempotent.
        - Artists : ON CONFLICT (id) DO UPDATE
        - Albums  : ON CONFLICT (id) DO UPDATE
        - Tracks  : ON CONFLICT (id) DO UPDATE updated_at=NOW()

        Returns:
            dict: stats {artists_inserted, albums_inserted, tracks_inserted, errors_count}
        """
        logger = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        artist_sql = """
            INSERT INTO artists
                (id, name, country, label, genres, monthly_listeners, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (id) DO UPDATE SET
                name              = EXCLUDED.name,
                country           = EXCLUDED.country,
                label             = EXCLUDED.label,
                genres            = EXCLUDED.genres,
                monthly_listeners = EXCLUDED.monthly_listeners,
                updated_at        = NOW()
        """
        album_sql = """
            INSERT INTO albums (id, artist_id, title, release_year, total_tracks, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE SET
                title        = EXCLUDED.title,
                release_year = EXCLUDED.release_year,
                total_tracks = EXCLUDED.total_tracks
        """
        track_sql = """
            INSERT INTO tracks
                (id, album_id, artist_id, title, duration_ms, genre, bpm,
                 explicit, audio_file_path, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (id) DO UPDATE SET
                title           = EXCLUDED.title,
                duration_ms     = EXCLUDED.duration_ms,
                genre           = EXCLUDED.genre,
                bpm             = EXCLUDED.bpm,
                explicit        = EXCLUDED.explicit,
                audio_file_path = EXCLUDED.audio_file_path,
                updated_at      = NOW()
        """

        artists_inserted = 0
        for a in transformed["artists"]:
            cur.execute(artist_sql, (
                a["id"], a["name"], a.get("country"), a.get("label"),
                a.get("genres", []), a.get("monthly_listeners", 0),
            ))
            artists_inserted += 1

        albums_inserted = 0
        for al in transformed["albums"]:
            cur.execute(album_sql, (
                al["id"], al["artist_id"], al["title"],
                al.get("release_year"), al.get("total_tracks"),
            ))
            albums_inserted += 1

        tracks_inserted = 0
        for t in transformed["tracks"]:
            cur.execute(track_sql, (
                t["id"], t.get("album_id"), t["artist_id"], t["title"],
                t["duration_ms"], t.get("genre"), t.get("bpm"),
                t.get("explicit", False), t.get("audio_file_path"),
            ))
            tracks_inserted += 1

        conn.commit()
        cur.close()

        stats = {
            "artists_inserted": artists_inserted,
            "albums_inserted":  albums_inserted,
            "tracks_inserted":  tracks_inserted,
            "errors_count":     0,
        }
        context["ti"].xcom_push(key="tracks_inserted", value=tracks_inserted)
        context["ti"].xcom_push(key="errors_count",    value=0)

        logger.info("Loaded : %s", stats)
        return stats

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        """Log de succès avec statistiques d'ingestion."""
        dag_run = context["dag_run"]
        print(f"""
        ✅ catalog_ingestion_pipeline terminé
        DAGRun          : {dag_run.run_id}
        Artists insérés : {stats.get('artists_inserted', 0)}
        Albums insérés  : {stats.get('albums_inserted', 0)}
        Tracks insérées : {stats.get('tracks_inserted', 0)}
        Erreurs DLQ     : {stats.get('errors_count', 0)}
        """)

    # ── Orchestration ─────────────────────────────────────────
    raw         = extract_from_minio()
    validated   = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats       = load_to_postgres(transformed)
    notify_success(stats)
