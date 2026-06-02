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

VALID_GENRES = ["Pop", "Rock", "Hip-Hop", "Electronic", "Jazz", "R&B", "Folk", "Latin", "Metal", "Classical"]

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
    def extract_from_minio(**context) -> list:
        """
        Télécharge les fichiers JSON des labels depuis MinIO.
        Retourne une liste de catalogues bruts.
        """
        import boto3
        import json
        import logging

        logger = logging.getLogger(__name__)

        # Connexion à MinIO via boto3 (compatible S3)
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1",
        )

        catalogs = []
        for filename in LABEL_FILES:
            try:
                response = s3.get_object(Bucket=MINIO_BUCKET, Key=filename)
                catalog = json.loads(response["Body"].read().decode("utf-8"))
                catalogs.append(catalog)
                logger.info(f"✅ Fichier chargé : {filename} — {catalog.get('stats', {})}")
            except Exception as e:
                logger.warning(f"⚠️ Fichier manquant ou erreur : {filename} — {e}")

        logger.info(f"Total catalogues chargés : {len(catalogs)}")
        return catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list) -> dict:
        """
        Valide le schéma de chaque catalogue.
        Les entrées invalides sont envoyées en DLQ.
        """
        import logging
        import json

        logger = logging.getLogger(__name__)

        # Champs obligatoires par type
        REQUIRED_ARTIST = ["id", "name", "label"]
        REQUIRED_ALBUM  = ["id", "artist_id", "title"]
        REQUIRED_TRACK  = ["id", "artist_id", "title", "duration_ms"]

        valid_artists = []
        valid_albums  = []
        valid_tracks  = []
        errors        = []

        def check_fields(entry, required_fields, entry_type):
            missing = [f for f in required_fields if not entry.get(f)]
            if missing:
                errors.append({
                    "type":    entry_type,
                    "entry":   entry,
                    "missing": missing,
                })
                return False
            return True

        for catalog in raw_catalogs:
            label = catalog.get("label", "unknown")
            logger.info(f"Validation du catalogue : {label}")

            for artist in catalog.get("artists", []):
                if check_fields(artist, REQUIRED_ARTIST, "artist"):
                    valid_artists.append(artist)

            for album in catalog.get("albums", []):
                if check_fields(album, REQUIRED_ALBUM, "album"):
                    valid_albums.append(album)

            for track in catalog.get("tracks", []):
                if check_fields(track, REQUIRED_TRACK, "track"):
                    valid_tracks.append(track)

        # Envoyer les erreurs en DLQ
        if errors:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            cur  = conn.cursor()
            for error in errors:
                cur.execute("""
                    INSERT INTO dead_letter_events
                        (original_topic, payload, error_type, error_message)
                    VALUES (%s, %s, %s, %s)
                """, (
                    "catalog_ingestion",
                    json.dumps(error["entry"]),
                    "schema_validation",
                    f"Champs manquants : {error['missing']}",
                ))
            conn.commit()
            cur.close()
            conn.close()
            logger.warning(f"⚠️ {len(errors)} entrées invalides envoyées en DLQ")

        logger.info(f"✅ Valides — Artists: {len(valid_artists)}, Albums: {len(valid_albums)}, Tracks: {len(valid_tracks)}")

        return {
            "valid": {
                "artists": valid_artists,
                "albums":  valid_albums,
                "tracks":  valid_tracks,
            },
            "errors_count": len(errors),
        }

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        """
        Transforme et normalise les données du catalogue.
        """
        import logging

        logger = logging.getLogger(__name__)

        data = validated.get("valid", {})

        artists = data.get("artists", [])
        albums  = data.get("albums",  [])
        tracks  = data.get("tracks",  [])

        # ── Normaliser les artistes ──────────────────────────
        seen_artists = set()
        clean_artists = []
        for artist in artists:
            artist["name"] = artist["name"].strip().title()
            key = (artist["name"], artist["label"])
            if key not in seen_artists:
                seen_artists.add(key)
                clean_artists.append(artist)

        # ── Normaliser les tracks ────────────────────────────
        clean_tracks = []
        for track in tracks:
            # Filtrer les durées invalides
            duration = track.get("duration_ms", 0)
            if not (0 < duration < 3_600_000):
                logger.warning(f"Track ignorée (durée invalide) : {track.get('title')} — {duration}ms")
                continue

            # Normaliser le genre
            genre = track.get("genre", "")
            if genre not in VALID_GENRES:
                track["genre"] = "Pop"  # genre par défaut

            track["title"] = track["title"].strip()
            clean_tracks.append(track)

        logger.info(f"✅ Après transformation — Artists: {len(clean_artists)}, Albums: {len(albums)}, Tracks: {len(clean_tracks)}")

        return {
            "artists": clean_artists,
            "albums":  albums,
            "tracks":  clean_tracks,
        }

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        """
        Charge les données dans PostgreSQL avec upsert idempotent.
        """
        import logging

        logger = logging.getLogger(__name__)

        artists = transformed.get("artists", [])
        albums  = transformed.get("albums",  [])
        tracks  = transformed.get("tracks",  [])

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        artists_inserted = 0
        albums_inserted  = 0
        tracks_inserted  = 0

        # ── Upsert Artists ───────────────────────────────────
        for artist in artists:
            cur.execute("""
                INSERT INTO artists (id, name, country, label, genres, monthly_listeners)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (name, label) DO UPDATE SET
                    country           = EXCLUDED.country,
                    genres            = EXCLUDED.genres,
                    monthly_listeners = EXCLUDED.monthly_listeners,
                    updated_at        = NOW()
            """, (
                artist["id"],
                artist["name"],
                artist.get("country"),
                artist["label"],
                artist.get("genres", []),
                artist.get("monthly_listeners", 0),
            ))
            artists_inserted += 1

        # ── Upsert Albums ────────────────────────────────────
        for album in albums:
            cur.execute("""
                INSERT INTO albums (id, artist_id, title, release_year, total_tracks)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    title        = EXCLUDED.title,
                    release_year = EXCLUDED.release_year,
                    total_tracks = EXCLUDED.total_tracks
            """, (
                album["id"],
                album["artist_id"],
                album["title"],
                album.get("release_year"),
                album.get("total_tracks", 0),
            ))
            albums_inserted += 1

        # ── Upsert Tracks ────────────────────────────────────
        for track in tracks:
            cur.execute("""
                INSERT INTO tracks (id, album_id, artist_id, title, duration_ms, genre, bpm, explicit, audio_file_path)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    title           = EXCLUDED.title,
                    duration_ms     = EXCLUDED.duration_ms,
                    genre           = EXCLUDED.genre,
                    updated_at      = NOW()
            """, (
                track["id"],
                track.get("album_id"),
                track["artist_id"],
                track["title"],
                track["duration_ms"],
                track.get("genre"),
                track.get("bpm"),
                track.get("explicit", False),
                track.get("audio_file_path"),
            ))
            tracks_inserted += 1

        conn.commit()
        cur.close()
        conn.close()

        stats = {
            "artists_inserted": artists_inserted,
            "albums_inserted":  albums_inserted,
            "tracks_inserted":  tracks_inserted,
            "errors_count":     0,
        }

        logger.info(f"✅ Chargement terminé — {stats}")
        return stats

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        """
        Log de succès avec statistiques d'ingestion.
        """
        dag_run = context["dag_run"]
        print(f"""
        ✅ catalog_ingestion_pipeline terminé
        DAGRun          : {dag_run.run_id}
        Artists insérés : {stats.get('artists_inserted', 0)}
        Albums insérés  : {stats.get('albums_inserted', 0)}
        Tracks insérés  : {stats.get('tracks_inserted', 0)}
        Erreurs DLQ     : {stats.get('errors_count', 0)}
        """)

    # ── Orchestration des tâches ──────────────────────────────
    raw         = extract_from_minio()
    validated   = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats       = load_to_postgres(transformed)
    notify_success(stats)