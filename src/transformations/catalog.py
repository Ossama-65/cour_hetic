"""
Fonctions de transformation du catalogue musical SPOTIFY.
Utilisées dans catalog_ingestion_pipeline et testées dans tests/unit/.
"""

from typing import Optional


VALID_GENRES = {
    "Pop", "Rock", "Hip-Hop", "Electronic", "Jazz",
    "R&B", "Folk", "Latin", "Metal", "Classical",
}

REQUIRED_TRACK_FIELDS = {"id", "artist_id", "title", "duration_ms"}


def normalize_artist_name(name: Optional[str]) -> Optional[str]:
    """
    Normalise un nom d'artiste : supprime les espaces superflus et applique le title case.
    Retourne None si name est None ou vide.
    """
    if name is None:
        return None
    stripped = name.strip()
    return stripped.title() if stripped else None


def validate_track_schema(track: dict) -> list:
    """
    Valide le schéma d'un track.
    Retourne une liste d'erreurs (vide si le track est valide).

    Règles :
    - Champs obligatoires : id, artist_id, title, duration_ms
    - 0 < duration_ms < 3_600_000 (1 heure max)
    """
    errors = []

    for field in REQUIRED_TRACK_FIELDS:
        if field not in track or track[field] is None:
            errors.append(f"Champ manquant : {field}")

    duration = track.get("duration_ms")
    if duration is not None:
        if not isinstance(duration, (int, float)):
            errors.append("duration_ms doit être un entier")
        elif duration <= 0:
            errors.append(f"duration_ms doit être > 0 (reçu : {duration})")
        elif duration >= 3_600_000:
            errors.append(f"duration_ms doit être < 3 600 000 (reçu : {duration})")

    return errors


def deduplicate_artists(artists: list) -> list:
    """
    Supprime les artistes dupliqués selon la clé (name normalisé, label).
    Conserve la première occurrence.
    """
    seen: set = set()
    result = []
    for artist in artists:
        name = normalize_artist_name(artist.get("name", ""))
        label = artist.get("label", "")
        key = (name, label)
        if key not in seen:
            seen.add(key)
            result.append(artist)
    return result
