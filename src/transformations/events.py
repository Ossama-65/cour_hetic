"""
Fonctions de validation et transformation des événements d'écoute SPOTIFY.
Utilisées dans streaming_events_pipeline et testées dans tests/unit/.
"""

from datetime import datetime, timezone
from typing import Optional

REQUIRED_EVENT_FIELDS = {"event_id", "user_id", "track_id", "timestamp", "duration_ms"}
BOT_DURATION_THRESHOLD_MS = 5_000   # < 5 secondes = pattern bot


def is_valid_listening_event(event: dict) -> bool:
    """
    Vérifie qu'un événement d'écoute est valide.

    Règles :
    - Champs obligatoires : event_id, user_id, track_id, timestamp, duration_ms
    - duration_ms > 0
    - timestamp parseable et non dans le futur (> NOW + 1 min tolérance)
    - Pattern bot : duration_ms < BOT_DURATION_THRESHOLD_MS → invalide

    Returns:
        bool: True si l'event est valide
    """
    # Champs obligatoires
    for field in REQUIRED_EVENT_FIELDS:
        if field not in event or event[field] is None:
            return False

    # duration_ms positif
    duration = event.get("duration_ms")
    if not isinstance(duration, (int, float)) or duration <= 0:
        return False

    # Pattern bot : écoute trop courte
    if duration < BOT_DURATION_THRESHOLD_MS:
        return False

    # Timestamp valide et non dans le futur (tolérance 60s)
    try:
        from datetime import timedelta
        ts_str = event["timestamp"]
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        ts = datetime.fromisoformat(ts_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts > datetime.now(timezone.utc) + timedelta(seconds=60):
            return False
    except (ValueError, TypeError, OverflowError):
        return False

    return True


def enrich_listening_event(event: dict, catalog: dict) -> Optional[dict]:
    """
    Enrichit un événement d'écoute avec les métadonnées du catalogue.

    Args:
        event:   événement d'écoute (doit avoir track_id)
        catalog: dict {track_id: {"title": ..., "artist_id": ..., "genre": ...}}

    Returns:
        dict enrichi ou None si track_id inconnu
    """
    track_id   = event.get("track_id")
    track_info = catalog.get(track_id)
    if track_info is None:
        return None

    enriched = dict(event)
    enriched["track_title"] = track_info.get("title")
    enriched["artist_id"]   = track_info.get("artist_id")
    enriched["genre"]       = track_info.get("genre")
    return enriched
