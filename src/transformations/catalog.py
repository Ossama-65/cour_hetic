"""
src/transformations/catalog.py
Fonctions de transformation du catalogue musical.
"""
from __future__ import annotations


def normalize_artist_name(name: str | None) -> str | None:
    """
    Normalise le nom d'un artiste :
    - Supprime les espaces en debut et fin
    - Applique le title case (gere les caracteres Unicode)
    Retourne None si l'entree est None.
    """
    if name is None:
        return None
    return name.strip().title()


def validate_track_schema(track: dict) -> list[str]:
    """
    Valide le schema d'un track.
    Retourne une liste d'erreurs (vide si le track est valide).

    Champs obligatoires : id, artist_id, title, duration_ms, genre
    Contraintes :
      - duration_ms > 0
      - duration_ms <= 36_000_000 (10 heures maximum)
    """
    errors = []
    required_fields = ["id", "artist_id", "title", "duration_ms", "genre"]

    for field in required_fields:
        if field not in track:
            errors.append(f"Champ manquant : {field}")

    if "duration_ms" in track:
        duration = track["duration_ms"]
        if duration <= 0:
            errors.append(f"duration_ms invalide : {duration} (doit etre > 0)")
        elif duration > 36_000_000:
            errors.append(f"duration_ms invalide : {duration} (depasse 10 heures)")

    return errors


def deduplicate_artists(artists: list[dict]) -> list[dict]:
    """
    Supprime les doublons d'artistes.
    Un doublon est defini par (nom normalise, label identique).
    En cas de doublon, le premier element rencontre est conserve.
    Des artistes avec le meme nom mais des labels differents sont conserves.
    """
    seen: set[tuple[str, str]] = set()
    result = []

    for artist in artists:
        key = (normalize_artist_name(artist["name"]), artist["label"])
        if key not in seen:
            seen.add(key)
            result.append(artist)

    return result
