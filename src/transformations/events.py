from datetime import datetime, timezone


def is_valid_listening_event(event):
    # Champs obligatoires
    required_fields = [
        "event_id", "user_id", "track_id",
        "timestamp", "duration_ms", "completed",
        "device_type", "geo_country", "event_source"
    ]
    for field in required_fields:
        if field not in event:
            return False

    # Timestamp ne doit pas etre dans le futur
    try:
        ts = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
        if ts > datetime.now(timezone.utc):
            return False
    except Exception:
        return False

    # duration_ms trop courte = pattern bot
    if event["duration_ms"] < 5000:
        return False

    return True


def enrich_listening_event(event, tracks):
    enriched = event.copy()
    track = tracks.get(event.get("track_id"))
    if track:
        enriched["track_title"] = track.get("title")
        enriched["artist_id"] = track.get("artist_id")
        enriched["genre"] = track.get("genre")
    return enriched