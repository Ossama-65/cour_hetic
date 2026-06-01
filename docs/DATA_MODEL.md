
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