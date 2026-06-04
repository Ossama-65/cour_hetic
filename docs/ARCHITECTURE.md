# Architecture SPOTIFY — Groupe D

---

## Vision d'ensemble

```mermaid
graph TD
    GEN[Data Generator<br/>Faker — 3 labels] -->|JSON files| MINIO_RAW[(MinIO<br/>labels-raw/)]
    SIM[Simulateur P2P<br/>Python] -->|pub/sub + buffer| REDIS[(Redis DB1<br/>listening_events)]
    SIM -->|produce dual| KAFKA[Apache Kafka<br/>listening_events<br/>p2p_network_events]

    MINIO_RAW -->|extract| DAG1[catalog_ingestion_pipeline<br/>0 2 * * *]
    REDIS -->|consume buffer| DAG2[streaming_events_pipeline<br/>*/5 * * * *]

    DAG1 -->|upsert| PG[(PostgreSQL<br/>artists / albums / tracks)]
    DAG1 -->|schema invalide| DLQ[(dead_letter_events)]

    DAG2 -->|upsert ON CONFLICT| PG
    DAG2 -->|partitionné date/heure| MINIO_PQ[(MinIO<br/>spotify-parquet/)]
    DAG2 -->|invalide / track inconnue| DLQ

    PG -->|ExternalTaskSensor| DAG3[aggregation_pipeline<br/>0 4 * * *]
    DAG3 -->|upsert| PG

    PG -->|ExternalTaskSensor| DAG4[recommendation_pipeline<br/>0 5 * * *]
    DAG4 -->|reco:user_id TTL 24h| REDIS
    DAG4 -->|upsert| PG

    DLQ -->|fetch pending| DAG5[dlq_reprocessing_pipeline<br/>@hourly]
    DAG5 -->|réinjection| PG

    KAFKA -->|Spark Structured Streaming| SPARK[Spark Jobs<br/>Phase 2]
    SPARK -->|write| PG
    SPARK -->|checkpoint| MINIO_CK[(MinIO<br/>spotify-checkpoints/)]
    SPARK -->|cache| REDIS
```

---

## Décisions architecturales

### ETL vs ELT — Mapping par pipeline

| Pipeline | Approche | Justification |
|---|---|---|
| `catalog_ingestion_pipeline` | **ETL** | Les données JSON des labels sont transformées (déduplication, normalisation du nom, validation de `duration_ms`) avant d'être chargées dans PostgreSQL. La transformation est nécessaire car les fichiers source peuvent contenir des doublons ou des champs mal formatés. |
| `streaming_events_pipeline` | **ETL** | Les événements Redis sont validés (champs obligatoires, `duration_ms > 0`) et enrichis (jointure avec le catalogue PostgreSQL pour ajouter `artist_id`, `genre`, `track_title`) avant d'être stockés. Les invalides vont directement en DLQ. |
| `aggregation_pipeline` | **ELT** | Les données brutes sont déjà dans `listening_events` (PostgreSQL). Les agrégations (`COUNT`, `SUM`, `COUNT DISTINCT`) sont calculées directement en SQL sur place, puis le résultat est chargé dans `daily_streams` et `artist_stats`. |
| `recommendation_pipeline` | **ELT** | Les données d'écoute sont lues depuis PostgreSQL, la matrice user×track est construite en mémoire Python, puis la similarité cosinus (sklearn) est calculée. Le résultat est ensuite chargé dans Redis et PostgreSQL. |
| `dlq_reprocessing_pipeline` | **ETL** | Les payloads DLQ sont lus, corrigés (ajout du timestamp manquant, vérification des champs) puis réinjectés dans `listening_events`. |

---

### Partitionnement Parquet

Le DAG `streaming_events_pipeline` écrit les événements enrichis sur MinIO avec la structure suivante :

```
spotify-parquet/
└── listening_events/
    └── date=2025-01-15/
        └── hour=14/
            └── part-{run_id}.parquet
```

**Pourquoi cette structure ?**

- **Partition par `date`** : permet de requêter une journée entière sans lire tout le dataset. Les agrégations du DAG `aggregation_pipeline` filtrent toujours sur `DATE(timestamp) = today`.
- **Sous-partition par `hour`** : le DAG `streaming_events_pipeline` tourne toutes les 5 minutes et écrit un fichier par run. Grouper par heure évite d'avoir des milliers de petits fichiers à la racine de la date.
- **`run_id` dans le nom de fichier** : garantit l'unicité — deux runs successifs n'écrasent pas le fichier précédent.

---

### Topics Kafka — Stratégie de partitionnement

| Topic | Partitions | Clé | Justification |
|---|---|---|---|
| `listening_events` | 6 | `user_id` | Tous les events d'un même utilisateur vont dans la même partition → garantit l'ordre des écoutes par user pour la détection de fraude et les fenêtres Spark. |
| `p2p_network_events` | 6 | `peer_id` | Les events réseau d'un même peer arrivent en ordre → cohérence pour l'analyse des connexions peer-to-peer. |
| `catalog_updates` | 3 | `track_id` | 3 partitions suffisent (volume faible). `cleanup.policy=compact` avec `track_id` comme clé garantit que seul l'état le plus récent d'un track est conservé. |
| `fraud_alerts` | 3 | `user_id` | Volume d'alertes faible. Clé `user_id` pour regrouper les alertes par utilisateur. |
| `late_listening_events` | 3 | `user_id` | Events arrivés hors watermark Spark, redirigés ici pour retraitement par Airflow. |

**Pourquoi `user_id` comme clé pour `listening_events` ?**

Le simulateur P2P utilise `user_id` comme clé Kafka (`producer.produce(topic, key=user_id)`). Cela garantit que tous les events d'un utilisateur donné arrivent dans la même partition, dans l'ordre. C'est indispensable pour :
1. Le job Spark `fraud_detection_job` qui détecte des patterns anormaux sur la fenêtre d'écoute d'un user.
2. La cohérence des agrégations windowed dans `streaming_trends_job`.

---

## Choix techniques

### Pourquoi CeleryExecutor (pas KubernetesExecutor) ?

CeleryExecutor a été choisi pour sa simplicité de déploiement dans notre contexte :

- **Redis déjà présent** dans la stack (port 6379) — il sert à la fois de broker Celery (DB 0) et de cache recommandations (DB 1). Pas de service supplémentaire à déployer.
- **Stack locale Docker Compose** — KubernetesExecutor nécessite un cluster Kubernetes, ce qui dépasse le cadre d'un projet local sur Docker Compose.
- **Scalabilité suffisante** — on peut ajouter des workers Celery (`airflow-worker`) sans modifier l'architecture. Le Scheduler envoie les tasks dans la file Redis, les Workers les consomment indépendamment.

```
Scheduler → Redis (DB 0, broker Celery) → Worker(s) → PostgreSQL (métadonnées)
```

### Gestion des secrets

Les credentials sont gérés via un fichier `.env` copié depuis `.env.example` au démarrage :

```bash
cp .env.example .env
```

Les variables d'environnement sont injectées dans tous les conteneurs Airflow via la section `environment` du `docker-compose.yml` :

```
SPOTIFY_POSTGRES_CONN : postgresql+psycopg2://spotify:spotify@postgres/spotify
MINIO_ENDPOINT        : http://minio:9000
MINIO_ACCESS_KEY      : minioadmin
MINIO_SECRET_KEY      : minioadmin
REDIS_URL             : redis://redis:6379/1
```

Les connexions Airflow (`spotify_postgres`, `spotify_redis`, `spotify_minio`) sont créées automatiquement par le conteneur `airflow-init` au premier démarrage via `airflow connections add`.

### Idempotence des pipelines

Tous les pipelines sont idempotents — relancer un DAGrun plusieurs fois produit le même résultat :

- **catalog_ingestion** : `ON CONFLICT (name, label) DO UPDATE SET monthly_listeners = EXCLUDED.monthly_listeners`
- **streaming_events** : `ON CONFLICT (id) DO NOTHING` — l'`event_id` UUID garantit l'unicité
- **aggregation** : `ON CONFLICT (track_id, date) DO UPDATE SET ...`
- **recommendation** : `ON CONFLICT (user_id, track_id) DO UPDATE SET score = EXCLUDED.score`

---

## Architecture Lambda — Batch + Speed Layer

```
Speed layer  : Simulateur P2P → Redis (pub/sub + buffer) → Airflow streaming_events_pipeline (*/5 min)
Batch layer  : Airflow catalog_ingestion (0 2h) → aggregation (0 4h) → recommendation (0 5h)
Serving layer: PostgreSQL + Redis ← consommé par les clients
```

**Ce qui est en batch et pourquoi :**

- `catalog_ingestion_pipeline` (quotidien à 2h) : le catalogue musical ne change pas en temps réel. Une ingestion quotidienne depuis MinIO suffit.
- `aggregation_pipeline` (quotidien à 4h) : les agrégats `daily_streams` et `artist_stats` sont des métriques journalières calculées une fois que tous les events de la journée sont collectés. Le DAG attend la fin de `streaming_events_pipeline` via `ExternalTaskSensor`.
- `recommendation_pipeline` (quotidien à 5h) : la matrice user×track et la similarité cosinus sont calculées sur les 7 derniers jours. Un recalcul quotidien est suffisant pour des recommandations personnalisées.
- `dlq_reprocessing_pipeline` (toutes les heures) : les events défectueux sont retraités périodiquement. Après 3 tentatives, ils passent en `abandoned`.

**Ce qui est en pseudo-streaming et pourquoi :**

- `streaming_events_pipeline` (toutes les 5 minutes) : les événements d'écoute du simulateur P2P sont consommés depuis les buffers Redis (`listening_events:buffer`) en micro-batch. Un run toutes les 5 minutes offre une latence acceptable pour peupler `listening_events` sans surcharger PostgreSQL avec des inserts ligne par ligne.

---

## Schémas d'événements

### listening_event

Généré par `src/p2p_simulator/simulator.py`, publié sur Redis `listening_events` et Kafka `listening_events` :

```json
{
  "event_id":    "b40e6561-3b19-4fb5-87ac-ef4569e71ac7",
  "user_id":     "066ad9bd-4c70-474f-a66c-bf78434b3d03",
  "track_id":    "179ab52a-8c66-49ef-ab3e-1478edf22d99",
  "source_peer": "a6c2c0d5-00d1-440b-80a7-9923c0ab320d",
  "timestamp":   "2026-06-04T14:12:31.503283Z",
  "duration_ms": 106760,
  "device_type": "web",
  "geo_country": "BR",
  "completed":   true,
  "event_source": "cache"
}
```

`completed = true` si `duration_ms > 30000` (30 secondes). `event_source` est choisi aléatoirement parmi `p2p` (majoritaire), `direct`, `cache`.

### p2p_network_event

5 types possibles : `peer_connect`, `peer_disconnect`, `chunk_transfer`, `cache_hit`, `cache_miss`.

```json
{
  "event_id":        "uuid",
  "event_type":      "chunk_transfer",
  "peer_id":         "uuid",
  "target_peer":     "uuid",
  "track_id":        "uuid",
  "chunk_size_kb":   512,
  "timestamp":       "2026-06-04T14:12:31Z"
}
```

---

## Dead Letter Queue

Les événements défectueux sont capturés dans `dead_letter_events` sans bloquer le pipeline :

| `error_type` | Cause | Pipeline source |
|---|---|---|
| `schema_validation` | Champs obligatoires manquants (artist) | `catalog_ingestion` |
| `validation` | `user_id` null, `duration_ms <= 0`, timestamp futur | `streaming_events` |
| `unknown_track` | `track_id` non trouvé dans le catalogue PostgreSQL | `streaming_events` |

Le DAG `dlq_reprocessing_pipeline` tourne toutes les heures. Il tente de corriger et réinjecter les events (max 3 tentatives). Au-delà → `status = 'abandoned'`.

---

## Leçons apprises

- **Lundi** : Le healthcheck `depends_on: condition: service_healthy` est indispensable — sans lui, Airflow démarre avant que PostgreSQL soit prêt et crashe. L'ordre postgres → redis → minio → airflow-* est critique.
- **Mardi** : L'idempotence `ON CONFLICT DO UPDATE` est obligatoire sur tous les pipelines. Un `DELETE + INSERT` casse la cohérence si le pipeline est relancé à mi-chemin. Le simulateur P2P publie en dual (Redis buffer + Kafka) pour couvrir Phase 1 et Phase 2 sans modifier le code.
- **Mercredi** : Le fichier `docker-compose.kafka.yml` devait être fusionné dans `docker-compose.yml` — les services Kafka et Spark étaient commentés et doivent être décommentés pour la Phase 2. `pytest` ne tourne pas nativement sur Windows (module `fcntl` manquant dans Airflow) — il faut lancer `pytest tests/unit/` uniquement pour éviter `test_dag_structure.py`.
- **Jeudi** : Les jobs Spark Structured Streaming (`streaming_trends_job`) ont besoin du checkpoint MinIO pour garantir l'exactly-once. Sans checkpoint, un restart Spark relit tous les offsets Kafka depuis le début → doublons dans PostgreSQL.
- **Vendredi** : La soutenance est une démo live — s'assurer que `docker compose ps` montre tous les services `Up` ou `healthy` avant de présenter.
