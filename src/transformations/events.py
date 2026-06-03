"""
src/transformations/events.py
Fonctions de transformation et validation des evenements d'ecoute.
"""
from __future__ import annotations
from datetime import datetime, timezone


REQUIRED_EVENT_FIELDS = [
    "event_id",
    "user_id",
    "track_id",
    "source_peer",
    "timestamp",
    "duration_ms",
    "completed",
    "device_type",
    "geo_country",
    "event_source",
]

BOT_DURATION_THRESHOLD_MS = 5_000


def is_valid_listening_event(event: dict) -> bool:
    """
    Valide un evenement d'ecoute.
    Retourne False si :
      - un champ obligatoire est manquant
      - le timestamp est dans le futur
      - duration_ms < 5000 et completed is False (pattern bot)
    """
    for field in REQUIRED_EVENT_FIELDS:
        if field not in event:
            return False

    try:
        ts_str = event["timestamp"].rstrip("Z")
        ts = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
        now = datetime.now(tz=timezone.utc)
        if ts > now:
            return False
    except (ValueError, AttributeError):
        return False

    if event.get("duration_ms", 0) < BOT_DURATION_THRESHOLD_MS and not event.get("completed", True):
        return False

    return True
