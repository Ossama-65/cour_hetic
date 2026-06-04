# DATA_MODEL.md — Spotify Data Platform

## Schéma de la base de données PostgreSQL

### ERD (Entity Relationship Diagram)

```
┌─────────────┐       ┌─────────────┐       ┌─────────────┐
│   genres    │       │   artists   │       │   albums    │
│─────────────│       │─────────────│       │─────────────│
│ id (PK)     │       │ id (PK)     │◄──────│ artist_id   │
│ name        │       │ name        │       │ id (PK)     │
│ created_at  │       │ country     │       │ title       │
└─────────────┘       │ label       │       │ release_year│
                      │ genres[]    │       │ total_tracks│
                      │ monthly_    │       │ created_at  │
                      │ listeners   │       └──────┬──────┘
                      │ created_at  │              │
                      │ updated_at  │              │
                      └──────┬──────┘              │
                             │                     │
                             ▼                     ▼
                      ┌─────────────────────────────────┐
                      │             tracks              │
                      │─────────────────────────────────│
                      │ id (PK)                         │
                      │ album_id (FK → albums)          │
                      │ artist_id (FK → artists)        │
                      │ title                           │
                      │ duration_ms                     │
                      │ genre                           │
                      │ bpm                             │
                      │ explicit                        │
                      │ audio_file_path (MinIO)         │
                      │ created_at / updated_at         │
                      └────────────────┬────────────────┘
                                       │
              ┌────────────────────────┼───────────────────────┐
              │                        │                       │
              ▼                        ▼                       ▼
  ┌─────────────────┐    ┌──────────────────────┐  ┌──────────────────┐
  │  daily_streams  │    │   listening_events   │  │  recommendations │
  │─────────────────│    │──────────────────────│  │──────────────────│
  │ track_id (PK,FK)│    │ id (PK)              │  │ user_id (PK)     │
  │ date (PK)       │    │ user_id              │  │ track_id (PK,FK) │
  │ total_streams   │    │ track_id (FK)        │  │ score            │
  │ unique_listeners│    │ source_peer_id (FK)  │  │ generated_at     │
  │ total_duration  │    │ timestamp            │  └──────────────────┘
  │ countries[]     │    │ duration_ms          │
  │ updated_at      │    │ device_type          │
  └─────────────────┘    │ geo_country          │
                         │ completed            │
  ┌─────────────────┐    │ event_source         │
  │  artist_stats   │    │ created_at           │
  │─────────────────│    └──────────┬───────────┘
  │ artist_id (PK)  │               │
  │ date (PK)       │               │
  │ total_streams   │    ┌──────────▼───────────┐
  │ unique_listeners│    │        peers         │
  │ top_track_id    │    │──────────────────────│
  │ updated_at      │    │ id (PK)              │
  └─────────────────┘    │ peer_name            │
                         │ ip_address           │
                         │ device_type          │
                         │ geo_country/city     │
                         │ status               │
                         │ cached_tracks[]      │
                         │ last_seen            │
                         └──────────────────────┘

┌──────────────────────┐    ┌──────────────────────┐
│  dead_letter_events  │    │ realtime_top_tracks  │
│──────────────────────│    │──────────────────────│
│ id (PK)              │    │ window_start (PK)    │
│ original_topic       │    │ track_id (PK, FK)    │
│ payload (JSONB)      │    │ stream_count         │
│ error_type           │    │ unique_listeners     │
│ error_message        │    │ updated_at           │
│ retry_count          │    └──────────────────────┘
│ status               │
│ created_at           │    ┌──────────────────────┐
│ last_retry_at        │    │  fraud_detections    │
│ resolved_at          │    │──────────────────────│
└──────────────────────┘    │ id (PK)              │
                            │ user_id              │
┌──────────────────────┐    │ peer_id              │
│  federated_catalog   │    │ fraud_type           │
│──────────────────────│    │ suspicion_score      │
│ track_id (PK)        │    │ evidence (JSONB)     │
│ source_group (PK)    │    │ window_start/end     │
│ artist_name          │    │ detected_at          │
│ track_title          │    └──────────────────────┘
│ duration_ms          │
│ genre                │
│ audio_peer_endpoint  │
│ ingested_at          │
└──────────────────────┘
```

---

## Description des tables

### Module 1 — Catalogue Musical

**`genres`** — Référentiel des genres musicaux (Pop, Rock, Hip-Hop, etc.)

**`artists`** — Artistes du catalogue.
- `genres[]` : array PostgreSQL de noms de genres
- `UNIQUE(name, label)` : contrainte d'idempotence pour l'upsert Airflow

**`albums`** — Albums liés à un artiste via `artist_id`.

**`tracks`** — Titres musicaux, cœur du catalogue.
- `audio_file_path` : chemin simulé vers MinIO
- Lié à `albums` et `artists`

---

### Module 1 — Réseau P2P

**`peers`** — Nœuds du réseau P2P simulé.
- `status` : `online`, `offline`, `streaming`
- `cached_tracks[]` : track_ids mis en cache local sur le peer

---

### Module 1 — Événements d'écoute

**`listening_events`** — Table principale des événements générés par le simulateur P2P.
- Indexée sur `user_id`, `track_id`, `timestamp` et `date_trunc('hour', timestamp)`
- `completed` : `true` si l'écoute dépasse 30 secondes
- `event_source` : `p2p`, `direct`, ou `cache`

---

### Module 1 — Agrégats Batch

**`daily_streams`** — Streams agrégés par track et par jour. Alimenté par le DAG `aggregation`.
- Clé primaire composite `(track_id, date)`

**`artist_stats`** — Stats quotidiennes par artiste.

**`recommendations`** — Recommandations générées par le DAG `recommendation` et stockées aussi dans Redis (`reco:<user_id>`).

---

### Module 1 — Dead Letter Queue

**`dead_letter_events`** — Événements défectueux isolés pour audit et retraitement.
- `status` : `pending`, `reprocessed`, `abandoned`
- Alimenté automatiquement quand un événement P2P est invalide
- Retraité par le DAG `dlq_reprocessing`

---

### Module 2 — Temps Réel (Spark Structured Streaming)

**`realtime_top_tracks`** — Top tracks par fenêtre temporelle de 5 min.
- Alimentée par le job Spark `streaming_trends_job`
- Clé primaire composite `(window_start, track_id)`

**`fraud_detections`** — Alertes de fraude détectées par `fraud_detection_job`.
- `fraud_type` : `bot_stream`, `free_rider`, `burst_listen`
- `evidence` : JSONB avec les preuves brutes

---

### Module 3 — Inter-Groupes

**`federated_catalog`** — Tracks agrégées depuis tous les groupes.
- `source_group` : identifiant du groupe d'origine (ex: `groupe-d`)
- Alimentée par le DAG `catalog_federation`

---

## Choix de conception

| Décision | Justification |
|---|---|
| UUID comme PK | Évite les collisions lors de l'interconnexion inter-groupes |
| `UNIQUE(name, label)` sur artists | Permet `ON CONFLICT DO UPDATE` — idempotence obligatoire |
| `TEXT[]` pour genres et cached_tracks | Flexibilité sans table de jointure, suffisant pour ce volume |
| Index sur `date_trunc('hour', timestamp)` | Optimise les requêtes d'agrégation horaire Spark |
| JSONB pour payload DLQ et evidence | Schéma flexible pour des événements hétérogènes |
| Clés composites sur agrégats | Idempotence native — un recalcul ne crée pas de doublon |
