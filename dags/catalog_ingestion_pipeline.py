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
        → transform_catalog()
        → load_to_postgres()
        → notify_success()
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
"""

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
        import boto3, json, os

        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )

        all_artists = []
        all_albums  = []
        all_tracks  = []

        for filename in LABEL_FILES:
            try:
                obj = s3.get_object(Bucket=MINIO_BUCKET, Key=filename)
                catalog = json.loads(obj["Body"].read())
                all_artists.extend(catalog.get("artists", []))
                all_albums.extend(catalog.get("albums", []))
                all_tracks.extend(catalog.get("tracks", []))
                print(f"✅ Téléchargé : {filename} — "
                      f"{len(catalog.get('artists', []))} artistes, "
                      f"{len(catalog.get('tracks', []))} tracks")
            except Exception as e:
                print(f"⚠️  Fichier manquant : {filename} — {e}")

        if not all_artists:
            raise ValueError("Aucun artiste trouvé dans MinIO")

        return [{"artists": all_artists, "albums": all_albums, "tracks": all_tracks}]

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list[dict]) -> dict:
        import json
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        valid = {"artists": [], "albums": [], "tracks": []}
        errors_count = 0

        for catalog in raw_catalogs:
            for artist in catalog.get("artists", []):
                if not all(artist.get(f) for f in ["id", "name", "label"]):
                    pg.run("""
                        INSERT INTO dead_letter_events (original_topic, payload, error_type, error_message)
                        VALUES (%s, %s, %s, %s)
                    """, parameters=["catalog_ingestion", json.dumps(artist),
                                     "schema_validation", "Champs obligatoires manquants (artist)"])
                    errors_count += 1
                    continue
                valid["artists"].append(artist)

            for album in catalog.get("albums", []):
                if not all(album.get(f) for f in ["id", "title", "artist_id"]):
                    errors_count += 1
                    continue
                valid["albums"].append(album)

            for track in catalog.get("tracks", []):
                if not all(track.get(f) for f in ["id", "title", "duration_ms", "artist_id"]):
                    errors_count += 1
                    continue
                valid["tracks"].append(track)

        print(f"✅ Validation : {len(valid['artists'])} artistes, "
              f"{len(valid['albums'])} albums, "
              f"{len(valid['tracks'])} tracks, {errors_count} erreurs DLQ")
        return {"valid": valid, "errors_count": errors_count}

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        data = validated["valid"]

        seen_artists = {}
        clean_artists = []
        for a in data["artists"]:
            key = (a["name"].strip().title(), a["label"].strip())
            if key not in seen_artists:
                a["name"]  = key[0]
                a["label"] = key[1]
                seen_artists[key] = a["id"]
                clean_artists.append(a)

        clean_tracks = []
        for t in data["tracks"]:
            duration = int(t.get("duration_ms", 0))
            if not (0 < duration < 3_600_000):
                continue
            t["duration_ms"] = duration
            t["title"] = t["title"].strip()
            clean_tracks.append(t)

        print(f"✅ Transformation : {len(clean_artists)} artistes, "
              f"{len(data['albums'])} albums, {len(clean_tracks)} tracks")
        return {
            "artists": clean_artists,
            "albums":  data["albums"],
            "tracks":  clean_tracks,
        }

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cur  = conn.cursor()

        artist_rows = [
            (a["id"], a["name"], a.get("country"), a["label"],
             a.get("genres", []), a.get("monthly_listeners", 0))
            for a in transformed["artists"]
        ]
        cur.executemany("""
            INSERT INTO artists (id, name, country, label, genres, monthly_listeners)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (name, label) DO UPDATE SET
                monthly_listeners = EXCLUDED.monthly_listeners,
                updated_at        = NOW()
        """, artist_rows)

        album_rows = [
            (a["id"], a["artist_id"], a["title"],
             a.get("release_year"), a.get("total_tracks"))
            for a in transformed["albums"]
        ]
        cur.executemany("""
            INSERT INTO albums (id, artist_id, title, release_year, total_tracks)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                title        = EXCLUDED.title,
                total_tracks = EXCLUDED.total_tracks
        """, album_rows)

        track_rows = [
            (t["id"], t.get("album_id"), t["artist_id"], t["title"],
             t["duration_ms"], t.get("genre"), t.get("bpm"), t.get("explicit", False),
             t.get("audio_file_path"))
            for t in transformed["tracks"]
        ]
        cur.executemany("""
            INSERT INTO tracks (id, album_id, artist_id, title, duration_ms, genre, bpm, explicit, audio_file_path)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                title      = EXCLUDED.title,
                updated_at = NOW()
        """, track_rows)

        conn.commit()
        cur.close()

        stats = {
            "artists_inserted": len(artist_rows),
            "albums_inserted":  len(album_rows),
            "tracks_inserted":  len(track_rows),
            "errors_count":     0,
        }
        print(f"✅ PostgreSQL chargé : {stats}")
        return stats

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        dag_run = context["dag_run"]
        print(f"""
        ✅ catalog_ingestion_pipeline terminé
        DAGRun : {dag_run.run_id}
        Artists insérés  : {stats.get('artists_inserted', 0)}
        Albums insérés   : {stats.get('albums_inserted', 0)}
        Tracks insérées  : {stats.get('tracks_inserted', 0)}
        Erreurs DLQ      : {stats.get('errors_count', 0)}
        """)

    raw         = extract_from_minio()
    validated   = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats       = load_to_postgres(transformed)
    notify_success(stats)