def normalize_artist_name(name):
    if name is None:
        return None
    return name.strip().title()


def validate_track_schema(track):
    errors = []
    if "title" not in track or not track["title"]:
        errors.append("title is required")
    if "duration_ms" not in track:
        errors.append("duration_ms is required")
    elif track["duration_ms"] < 0:
        errors.append("duration_ms cannot be negative")
    elif track["duration_ms"] > 36_000_000:
        errors.append("duration_ms too long")
    if "artist_id" not in track:
        errors.append("artist_id is required")
    return errors


def deduplicate_artists(artists):
    seen = set()
    result = []
    for artist in artists:
        key = (artist["name"].strip().lower(), artist["label"])
        if key not in seen:
            seen.add(key)
            result.append(artist)
    return result