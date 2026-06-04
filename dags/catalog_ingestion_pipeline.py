"""
DAG : catalog_ingestion_pipeline
=================================

Ingère le catalogue musical depuis les fichiers JSON des labels
stockés dans MinIO, valide les données, les transforme, puis les charge
dans PostgreSQL.

Architecture :
    MinIO / bucket labels-raw
        → extract_from_minio()
        → validate_schema()
        → transform_catalog()
        → load_to_postgres()
        → notify_success()

Objectifs de l'issue #4 :
    [x] Lire les JSON depuis MinIO
    [x] Valider les champs obligatoires
    [x] Envoyer les entrées invalides en DLQ
    [x] Normaliser les artistes et les genres
    [x] Dédupliquer les artistes
    [x] Charger artists, albums et tracks dans PostgreSQL
    [x] Utiliser des upserts idempotents
    [x] Ajouter doc_md et callback d'échec
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_CONN_ID = "spotify_minio"
MINIO_BUCKET = "labels-raw"

LABEL_FILES = [
    "sunset_records.json",
    "nightwave_music.json",
    "urban_pulse.json",
]

VALID_GENRES = {
    "Pop",
    "Rock",
    "Hip-Hop",
    "Electronic",
    "Jazz",
    "Classical",
    "R&B",
    "Metal",
    "Folk",
    "Latin",
}


# ─────────────────────────────────────────────────────────────
# CALLBACKS
# ─────────────────────────────────────────────────────────────

def on_failure_alert(context: dict[str, Any]) -> None:
    """Callback appelé lorsqu'une tâche Airflow échoue."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exception = context.get("exception")

    print(
        f"""
        ❌ ALERTE AIRFLOW
        DAG    : {dag_id}
        Tâche  : {task_id}
        Erreur : {exception}
        """
    )


# ─────────────────────────────────────────────────────────────
# DOCUMENTATION AIRFLOW
# ─────────────────────────────────────────────────────────────

DAG_DOC = """
## catalog_ingestion_pipeline

### Rôle

Ce DAG ingère les métadonnées musicales depuis trois catalogues JSON stockés
dans MinIO, puis charge les tables PostgreSQL du catalogue musical.

### Sources

- `s3://labels-raw/sunset_records.json`
- `s3://labels-raw/nightwave_music.json`
- `s3://labels-raw/urban_pulse.json`

### Destinations

- `artists`
- `albums`
- `tracks`
- `dead_letter_events` pour les entrées invalides

### Idempotence

Le DAG peut être relancé plusieurs fois sans créer de doublons grâce aux
instructions `INSERT ... ON CONFLICT DO UPDATE`.

### Gestion des erreurs

- Fichier MinIO manquant : warning et poursuite du traitement.
- Entrée invalide : insertion dans `dead_letter_events`.
- Erreur réseau : retries Airflow configurés.
"""

DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": on_failure_alert,
}


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def normalize_artist_name(name: str) -> str:
    """Normalise le nom d'un artiste."""
    return " ".join(name.strip().split()).title()


def normalize_genre(genre: str | None) -> str | None:
    """
    Normalise un genre musical.

    Si le genre n'appartient pas à la liste de référence, on retourne None
    pour éviter d'insérer une valeur incohérente.
    """
    if not genre:
        return None

    cleaned = genre.strip()

    for valid_genre in VALID_GENRES:
        if cleaned.lower() == valid_genre.lower():
            return valid_genre

    return None


def insert_dlq(
    hook: PostgresHook,
    payload: dict[str, Any],
    error_type: str,
    error_message: str,
    original_topic: str = "catalog_ingestion",
) -> None:
    """Insère une entrée invalide dans la Dead Letter Queue."""
    hook.run(
        """
        INSERT INTO dead_letter_events (
            original_topic,
            payload,
            error_type,
            error_message,
            status
        )
        VALUES (%s, %s::jsonb, %s, %s, 'pending')
        """,
        parameters=(
            original_topic,
            json.dumps(payload, ensure_ascii=False),
            error_type,
            error_message,
        ),
    )


# ─────────────────────────────────────────────────────────────
# DAG
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
    def extract_from_minio() -> list[dict[str, Any]]:
        """
        Télécharge et parse les catalogues JSON stockés dans MinIO.

        Si un fichier est absent, on logge un warning et on continue.
        """
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        raw_catalogs: list[dict[str, Any]] = []

        for file_name in LABEL_FILES:
            try:
                file_content = s3_hook.read_key(
                    key=file_name,
                    bucket_name=MINIO_BUCKET,
                )

                if not file_content:
                    print(f"⚠️ Fichier vide ignoré : {file_name}")
                    continue

                catalog = json.loads(file_content)
                raw_catalogs.append(catalog)

                stats = catalog.get("stats", {})
                print(
                    f"✅ Catalogue extrait : {file_name} "
                    f"artists={stats.get('artists')} "
                    f"albums={stats.get('albums')} "
                    f"tracks={stats.get('tracks')}"
                )

            except Exception as exc:
                print(f"⚠️ Impossible d'extraire {file_name} depuis MinIO : {exc}")

        print(f"📦 Nombre de catalogues extraits : {len(raw_catalogs)}")
        return raw_catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Valide les catalogues.

        Entrées attendues :
        - artiste : id, name, label
        - album   : id, artist_id, title
        - track   : id, artist_id, title, duration_ms
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        valid_entries: dict[str, list[dict[str, Any]]] = {
            "artists": [],
            "albums": [],
            "tracks": [],
        }

        errors_count = 0

        required_artist_fields = ("id", "name", "label")
        required_album_fields = ("id", "artist_id", "title")
        required_track_fields = ("id", "artist_id", "title", "duration_ms")

        for catalog in raw_catalogs:
            label = catalog.get("label", "unknown_label")

            for artist in catalog.get("artists", []):
                missing = [field for field in required_artist_fields if not artist.get(field)]
                if missing:
                    errors_count += 1
                    insert_dlq(
                        hook=hook,
                        payload=artist,
                        error_type="schema_validation_artist",
                        error_message=f"Champs artiste manquants : {missing}",
                    )
                    continue

                artist.setdefault("label", label)
                valid_entries["artists"].append(artist)

            for album in catalog.get("albums", []):
                missing = [field for field in required_album_fields if not album.get(field)]
                if missing:
                    errors_count += 1
                    insert_dlq(
                        hook=hook,
                        payload=album,
                        error_type="schema_validation_album",
                        error_message=f"Champs album manquants : {missing}",
                    )
                    continue

                valid_entries["albums"].append(album)

            for track in catalog.get("tracks", []):
                missing = [field for field in required_track_fields if not track.get(field)]
                if missing:
                    errors_count += 1
                    insert_dlq(
                        hook=hook,
                        payload=track,
                        error_type="schema_validation_track",
                        error_message=f"Champs track manquants : {missing}",
                    )
                    continue

                valid_entries["tracks"].append(track)

        print(
            f"""
            ✅ Validation terminée
            Artistes valides : {len(valid_entries['artists'])}
            Albums valides   : {len(valid_entries['albums'])}
            Tracks valides   : {len(valid_entries['tracks'])}
            Erreurs DLQ      : {errors_count}
            """
        )

        return {
            "valid": valid_entries,
            "errors_count": errors_count,
        }

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict[str, Any]) -> dict[str, Any]:
        """
        Normalise et prépare les données pour PostgreSQL.

        Transformations :
        - normalisation des noms d'artistes ;
        - déduplication des artistes par couple (name, label) ;
        - mapping des anciens artist_id vers les IDs conservés ;
        - normalisation des genres ;
        - filtrage des tracks avec durée invalide.
        """
        valid_data = validated["valid"]

        unique_artists: list[dict[str, Any]] = []
        seen_artist_keys: dict[tuple[str, str], str] = {}
        artist_id_mapping: dict[str, str] = {}

        for artist in valid_data["artists"]:
            original_artist_id = artist["id"]
            normalized_name = normalize_artist_name(artist["name"])
            label = artist["label"].strip()
            key = (normalized_name, label)

            if key not in seen_artist_keys:
                artist["name"] = normalized_name
                artist["label"] = label
                artist["genres"] = [
                    genre
                    for genre in (normalize_genre(g) for g in artist.get("genres", []))
                    if genre is not None
                ]
                artist["monthly_listeners"] = int(artist.get("monthly_listeners", 0) or 0)

                unique_artists.append(artist)
                seen_artist_keys[key] = artist["id"]

            artist_id_mapping[original_artist_id] = seen_artist_keys[key]

        valid_artist_ids = {artist["id"] for artist in unique_artists}

        transformed_albums: list[dict[str, Any]] = []
        valid_album_ids: set[str] = set()

        for album in valid_data["albums"]:
            original_artist_id = album["artist_id"]
            canonical_artist_id = artist_id_mapping.get(original_artist_id, original_artist_id)

            if canonical_artist_id not in valid_artist_ids:
                continue

            album["artist_id"] = canonical_artist_id
            album["release_year"] = album.get("release_year")
            album["total_tracks"] = int(album.get("total_tracks", 0) or 0)

            transformed_albums.append(album)
            valid_album_ids.add(album["id"])

        transformed_tracks: list[dict[str, Any]] = []

        for track in valid_data["tracks"]:
            duration_ms = int(track.get("duration_ms", 0) or 0)

            if duration_ms <= 0 or duration_ms >= 3_600_000:
                continue

            original_artist_id = track["artist_id"]
            canonical_artist_id = artist_id_mapping.get(original_artist_id, original_artist_id)

            if canonical_artist_id not in valid_artist_ids:
                continue

            album_id = track.get("album_id")
            if album_id and album_id not in valid_album_ids:
                continue

            track["artist_id"] = canonical_artist_id
            track["duration_ms"] = duration_ms
            track["genre"] = normalize_genre(track.get("genre"))
            track["bpm"] = track.get("bpm")
            track["explicit"] = bool(track.get("explicit", False))
            track["audio_file_path"] = track.get("audio_file_path")

            transformed_tracks.append(track)

        print(
            f"""
            ✅ Transformation terminée
            Artistes après déduplication : {len(unique_artists)}
            Albums transformés           : {len(transformed_albums)}
            Tracks transformés           : {len(transformed_tracks)}
            """
        )

        return {
            "artists": unique_artists,
            "albums": transformed_albums,
            "tracks": transformed_tracks,
            "errors_count": validated.get("errors_count", 0),
        }

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict[str, Any], **context) -> dict[str, int]:
        """
        Charge les données dans PostgreSQL avec des upserts idempotents.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        artists_data = [
            (
                artist["id"],
                artist["name"],
                artist.get("country"),
                artist["label"],
                artist.get("genres", []),
                int(artist.get("monthly_listeners", 0) or 0),
            )
            for artist in transformed["artists"]
        ]

        albums_data = [
            (
                album["id"],
                album["artist_id"],
                album["title"],
                album.get("release_year"),
                int(album.get("total_tracks", 0) or 0),
            )
            for album in transformed["albums"]
        ]

        tracks_data = [
            (
                track["id"],
                track.get("album_id"),
                track["artist_id"],
                track["title"],
                int(track["duration_ms"]),
                track.get("genre"),
                track.get("bpm"),
                bool(track.get("explicit", False)),
                track.get("audio_file_path"),
            )
            for track in transformed["tracks"]
        ]

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                if artists_data:
                    cur.executemany(
                        """
                        INSERT INTO artists (
                            id,
                            name,
                            country,
                            label,
                            genres,
                            monthly_listeners
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (name, label) DO UPDATE SET
                            country = EXCLUDED.country,
                            genres = EXCLUDED.genres,
                            monthly_listeners = EXCLUDED.monthly_listeners,
                            updated_at = NOW()
                        """,
                        artists_data,
                    )

                if albums_data:
                    cur.executemany(
                        """
                        INSERT INTO albums (
                            id,
                            artist_id,
                            title,
                            release_year,
                            total_tracks
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            artist_id = EXCLUDED.artist_id,
                            title = EXCLUDED.title,
                            release_year = EXCLUDED.release_year,
                            total_tracks = EXCLUDED.total_tracks
                        """,
                        albums_data,
                    )

                if tracks_data:
                    cur.executemany(
                        """
                        INSERT INTO tracks (
                            id,
                            album_id,
                            artist_id,
                            title,
                            duration_ms,
                            genre,
                            bpm,
                            explicit,
                            audio_file_path
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            album_id = EXCLUDED.album_id,
                            artist_id = EXCLUDED.artist_id,
                            title = EXCLUDED.title,
                            duration_ms = EXCLUDED.duration_ms,
                            genre = EXCLUDED.genre,
                            bpm = EXCLUDED.bpm,
                            explicit = EXCLUDED.explicit,
                            audio_file_path = EXCLUDED.audio_file_path,
                            updated_at = NOW()
                        """,
                        tracks_data,
                    )

                conn.commit()

        stats = {
            "artists_inserted": len(artists_data),
            "albums_inserted": len(albums_data),
            "tracks_inserted": len(tracks_data),
            "errors_count": int(transformed.get("errors_count", 0) or 0),
        }

        ti = context["ti"]
        ti.xcom_push(key="artists_inserted", value=stats["artists_inserted"])
        ti.xcom_push(key="albums_inserted", value=stats["albums_inserted"])
        ti.xcom_push(key="tracks_inserted", value=stats["tracks_inserted"])
        ti.xcom_push(key="errors_count", value=stats["errors_count"])

        print(
            f"""
            ✅ Chargement PostgreSQL terminé
            Artists : {stats['artists_inserted']}
            Albums  : {stats['albums_inserted']}
            Tracks  : {stats['tracks_inserted']}
            DLQ     : {stats['errors_count']}
            """
        )

        return stats

    @task(task_id="notify_success")
    def notify_success(stats: dict[str, int], **context) -> None:
        """
        Log de succès avec les statistiques d'ingestion.
        """
        dag_run = context["dag_run"]

        print(
            f"""
            ✅ catalog_ingestion_pipeline terminé

            DAGRun            : {dag_run.run_id}
            Artists chargés   : {stats.get('artists_inserted', 0)}
            Albums chargés    : {stats.get('albums_inserted', 0)}
            Tracks chargés    : {stats.get('tracks_inserted', 0)}
            Erreurs DLQ       : {stats.get('errors_count', 0)}
            """
        )

    # ─────────────────────────────────────────────────────────
    # ORCHESTRATION
    # ─────────────────────────────────────────────────────────

    raw_catalogs = extract_from_minio()
    validated_catalogs = validate_schema(raw_catalogs)
    transformed_catalog = transform_catalog(validated_catalogs)
    ingestion_stats = load_to_postgres(transformed_catalog)
    notify_success(ingestion_stats)
