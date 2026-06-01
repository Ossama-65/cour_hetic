# Architecture SPOTIFY

> **À compléter par votre groupe** — Ce document doit décrire VOTRE architecture, pas celle de référence.

---

## Vision d'ensemble

```mermaid
erDiagram
    %% --- DIMENSIONS ---
    ALBUMS {
        varchar id PK
        varchar name
        varchar artist_id FK
        date release_date
    }

    ARTISTS {
        varchar id PK
        varchar name
        int popularity
    }

    TRACKS {
        varchar id PK
        varchar title
        varchar album_id FK
        int duration_ms
    }

    GENRES {
        serial id PK
        varchar name
    }

    %% --- FAITS & ANALYTICS ---
    LISTENING_EVENTS {
        varchar id PK
        varchar user_id
        varchar track_id FK
        timestamp timestamp
    }

    DAILY_STREAMS {
        varchar track_id PK, FK
        date date PK
        int count
    }

    REALTIME_TOP_TRACKS {
        timestamp window_start PK
        varchar track_id PK, FK
        int rank
    }

    ARTIST_STATS {
        varchar artist_id PK, FK
        bigint total_streams
        timestamp last_updated
    }

    %% --- SYSTÈME & RECOMMANDATIONS ---
    RECOMMENDATIONS {
        varchar user_id PK
        varchar track_id PK, FK
        float score
    }

    FRAUD_DETECTIONS {
        varchar event_id PK
        varchar reason
        timestamp detected_at
    }

    DEAD_LETTER_EVENTS {
        serial id PK
        jsonb payload
        text error_message
        timestamp failed_at
    }

    FEDERATED_CATALOG {
        varchar id PK
        varchar source_system
        varchar external_id
    }

    PEERS {
        varchar peer_id PK
        varchar ip_address
        timestamp last_seen
    }

    %% --- RELATIONS ---
    ARTISTS ||--o{ ALBUMS : "produit"
    ALBUMS ||--o{ TRACKS : "contient"
    TRACKS ||--o{ LISTENING_EVENTS : "genere"
    TRACKS ||--o{ DAILY_STREAMS : "est agrégé dans"
    TRACKS ||--o{ REALTIME_TOP_TRACKS : "apparaît dans"
    TRACKS ||--o{ RECOMMENDATIONS : "est suggéré"
    ARTISTS ||--o{ ARTIST_STATS : "possède"




```

```mermaid


graph TD
    SIM[Simulateur P2P] -->|pub/sub| REDIS[(Redis)]
    SIM -->|produce| KAFKA[Apache Kafka]
    
    REDIS -->|consume| AIR[Airflow DAGs]
    KAFKA -->|consume| SPARK[Spark Streaming]
    KAFKA -->|availableNow| AIR
    
    AIR -->|upsert| PG[(PostgreSQL)]
    AIR -->|write| MINIO[(MinIO / Parquet)]
    AIR -->|cache| REDIS
    
    SPARK -->|write| PG
    SPARK -->|checkpoint| MINIO
    SPARK -->|cache| REDIS
    SPARK -->|produce| KAFKA
```

---

## Décisions architecturales

### ETL vs ELT — Mapping par pipeline

| Pipeline | Approche | Justification |
|----------|----------|---------------|
| catalog_ingestion | ETL | On valide le schéma JSON et on nettoie les chaînes (strip, title) en Python via Airflow avant d'insérer dans Postgres pour garantir l'intégrité du catalogue.
 |
| streaming_events | ELT | Les événements bruts sont chargés directement dans MinIO (Data Lake). On ne transforme rien à l'entrée pour absorber un maximum de débit sans latence. |

| aggregation | ELT | On utilise la puissance de PostgreSQL pour transformer les données brutes déjà présentes en tables de synthèse (daily_streams) via des requêtes SQL complexes. |

| streaming_trends (Spark) | ETL | Spark transforme les flux en temps réel (filtrage, fenêtrage) pour ne charger que les résultats agrégés (Top tracks) dans la base finale. |

### Partitionnement Parquet

Expliquer ici votre stratégie de partitionnement des fichiers Parquet sur MinIO.

```
spotify-parquet/
└── listening_events/
    └── date=2025-01-15/
        └── hour=14/
            └── part-00000.parquet
```

**Pourquoi cette structure ?**
→ À compléter

### Topics Kafka — Stratégie de partitionnement

| Topic | Partitions | Clé | Justification |
|-------|-----------|-----|---------------|
| listening_events | 6 | user_id | ... |
| p2p_network_events | 6 | peer_id | ... |
| catalog_updates | 3 | track_id | ... |
| fraud_alerts | 3 | user_id | ... |

**Pourquoi `user_id` comme clé pour `listening_events` ?**
→ À compléter

---

## Choix techniques

### Pourquoi CeleryExecutor (pas KubernetesExecutor) ?

→ À compléter

### Gestion des secrets

→ Comment votre groupe gère les credentials (PostgreSQL password, MinIO keys...) ?

---

## Architecture Lambda — Batch + Speed Layer

```
Speed layer  : Simulateur → Kafka → Spark → PostgreSQL (realtime_*) + Redis
Batch layer  : Simulateur → Kafka (availableNow) → Airflow → PostgreSQL (daily_*) + MinIO
Serving layer: PostgreSQL + Redis ← consommé par les clients
```

**Ce qui est en batch et pourquoi :**
→ À compléter

**Ce qui est en streaming et pourquoi :**
→ À compléter

---

## Schémas d'événements

### listening_event

```json
{
  "event_id":    "uuid",
  "user_id":     "uuid",
  "track_id":    "uuid",
  "source_peer": "uuid",
  "timestamp":   "2025-01-15T14:30:00Z",
  "duration_ms": 45000,
  "device_type": "mobile",
  "geo_country": "FR",
  "completed":   true,
  "event_source": "p2p"
}
```

### p2p_network_event

```json
{
  "event_id":   "uuid",
  "event_type": "chunk_transfer",
  "peer_id":    "uuid",
  "target_peer": "uuid",
  "track_id":   "uuid",
  "chunk_size_bytes": 65536,
  "latency_ms": 12,
  "timestamp":  "2025-01-15T14:30:01Z"
}
```

---

## Leçons apprises

> À compléter au fur et à mesure de la semaine.

- **Lundi** : ...
- **Mardi** : ...
- **Mercredi** : ...
- **Jeudi** : ...
- **Vendredi** : ...
