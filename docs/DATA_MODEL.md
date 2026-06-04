# DATA MODEL — Groupe KFK — Spotify Data Platform

## 1. Vue d'ensemble

La base PostgreSQL du projet Spotify contient **13 tables**, organisées en 4 domaines :

| Domaine | Tables |
|---|---|
| Catalogue musical | `genres`, `artists`, `albums`, `tracks` |
| Réseau P2P et événements | `peers`, `listening_events` |
| Agrégats et recommandations | `daily_streams`, `artist_stats`, `recommendations`, `realtime_top_tracks` |
| Qualité, fraude et inter-groupes | `dead_letter_events`, `fraud_detections`, `federated_catalog` |

Le script SQL de référence est `sql/init_spotify_db.sql`. Il crée l'utilisateur et la base `spotify`, les 13 tables, leurs index, et insère 10 genres de référence.

Ce document décrit chaque table, son rôle, ses relations et les choix techniques importants du modèle.

---

## 2. Diagramme ERD

Les flèches `──FK──>` indiquent une clé étrangère réelle (contrainte `REFERENCES`).
Les flèches `··soft··>` indiquent un lien logique **non contraint** par une clé étrangère.

```text
genres (table de référence, 10 genres pré-insérés)
   ▲
   ··soft··  tracks.genre   (VARCHAR, pas de FK)
   ··soft··  artists.genres (TEXT[], pas de FK)

artists
   │──FK──> albums.artist_id
   │──FK──> tracks.artist_id
   └──FK──> artist_stats.artist_id

albums
   └──FK──> tracks.album_id   (nullable : un morceau peut ne pas avoir d'album)

tracks
   ├──FK──> listening_events.track_id
   ├──FK──> daily_streams.track_id
   ├──FK──> recommendations.track_id
   └──FK──> realtime_top_tracks.track_id

peers
   └──FK──> listening_events.source_peer_id   (nullable)

-- Tables sans FK entrante/sortante stricte (volontairement découplées) :
dead_letter_events     ← événements invalides ou non retraitables (payload JSONB libre)
fraud_detections       ← alertes de fraude temps réel
federated_catalog      ← catalogue provenant des autres groupes (track_id NON contraint)
```

> Remarque de conception : `dead_letter_events`, `fraud_detections` et `federated_catalog`
> ne posent **pas** de clé étrangère vers `tracks`. C'est volontaire : ces tables doivent
> pouvoir accueillir des données *malformées* ou *externes* (autres groupes) sans qu'une
> contrainte d'intégrité ne bloque l'insertion. La validation se fait dans le code, pas en base.

---

## 3. Description des tables

### 3.1 `genres`

Table de **référence** (vocabulaire contrôlé des genres musicaux). Pré-remplie au démarrage avec : Pop, Rock, Hip-Hop, Electronic, Jazz, Classical, R&B, Metal, Folk, Latin.

| Colonne | Type | Description |
|---|---|---|
| `id` | SERIAL | Clé primaire auto-incrémentée |
| `name` | VARCHAR(100) | Nom du genre, **unique** |
| `created_at` | TIMESTAMP | Date de création |

```sql
name VARCHAR(100) NOT NULL UNIQUE
```

Aucune table ne référence `genres` par clé étrangère : `tracks.genre` et `artists.genres` y font référence par **valeur** (le nom), pas par contrainte.

---

### 3.2 `artists`

Artistes du catalogue musical.

| Colonne | Type | Description |
|---|---|---|
| `id` | UUID | Clé primaire (`gen_random_uuid()`) |
| `name` | VARCHAR(255) | Nom de l'artiste |
| `country` | VARCHAR(100) | Pays d'origine |
| `label` | VARCHAR(255) | Label musical |
| `genres` | TEXT[] | Tableau de noms de genres |
| `monthly_listeners` | INT | Auditeurs mensuels (défaut 0) |
| `created_at` | TIMESTAMP | Date de création |
| `updated_at` | TIMESTAMP | Date de mise à jour |

```sql
UNIQUE(name, label)
```

Cette contrainte est **la clé de l'idempotence** de l'ingestion catalogue : l'upsert de l'issue #4 s'appuie dessus (`ON CONFLICT (name, label) DO UPDATE`).

---

### 3.3 `albums`

Albums associés aux artistes.

| Colonne | Type | Description |
|---|---|---|
| `id` | UUID | Clé primaire |
| `artist_id` | UUID | FK vers `artists(id)`, **NOT NULL** |
| `title` | VARCHAR(255) | Titre de l'album |
| `release_year` | INT | Année de sortie |
| `total_tracks` | INT | Nombre de morceaux |
| `created_at` | TIMESTAMP | Date de création |

Relation : `albums.artist_id ──FK──> artists.id`.

---

### 3.4 `tracks`

Morceaux du catalogue — **table centrale** du modèle.

| Colonne | Type | Description |
|---|---|---|
| `id` | UUID | Clé primaire |
| `album_id` | UUID | FK vers `albums(id)`, **nullable** |
| `artist_id` | UUID | FK vers `artists(id)`, **NOT NULL** |
| `title` | VARCHAR(255) | Titre du morceau |
| `duration_ms` | INT | Durée en millisecondes, **NOT NULL** |
| `genre` | VARCHAR(100) | Genre (valeur, pas de FK vers `genres`) |
| `bpm` | INT | Tempo |
| `explicit` | BOOLEAN | Contenu explicite (défaut FALSE) |
| `audio_file_path` | VARCHAR(500) | Chemin MinIO simulé du fichier audio |
| `created_at` | TIMESTAMP | Date de création |
| `updated_at` | TIMESTAMP | Date de mise à jour |

Relations : `tracks.album_id ──FK──> albums.id` (optionnelle), `tracks.artist_id ──FK──> artists.id`.
Référencée par `listening_events`, `daily_streams`, `recommendations`, `realtime_top_tracks`.

---

### 3.5 `peers`

Nœuds (appareils) du réseau P2P.

| Colonne | Type | Description |
|---|---|---|
| `id` | UUID | Clé primaire |
| `peer_name` | VARCHAR(100) | Nom du peer, **NOT NULL** |
| `ip_address` | VARCHAR(45) | Adresse IP (compatible IPv6) |
| `device_type` | VARCHAR(50) | mobile, desktop, speaker… |
| `geo_country` | VARCHAR(100) | Pays |
| `geo_city` | VARCHAR(100) | Ville |
| `status` | VARCHAR(20) | online, offline, streaming (défaut offline) |
| `cached_tracks` | TEXT[] | track_ids en cache local |
| `last_seen` | TIMESTAMP | Dernière activité |
| `created_at` | TIMESTAMP | Date de création |

Modélise l'architecture distribuée et le fonctionnement P2P.

---

### 3.6 `listening_events`

Événements d'écoute générés par le simulateur P2P. C'est la table la plus volumineuse.

| Colonne | Type | Description |
|---|---|---|
| `id` | UUID | Clé primaire |
| `user_id` | UUID | Identifiant utilisateur, **NOT NULL** |
| `track_id` | UUID | FK vers `tracks(id)`, **NOT NULL** |
| `source_peer_id` | UUID | FK vers `peers(id)`, **nullable** |
| `timestamp` | TIMESTAMP | Heure de l'écoute, **NOT NULL** |
| `duration_ms` | INT | Durée réellement écoutée |
| `device_type` | VARCHAR(50) | Appareil utilisé |
| `geo_country` | VARCHAR(100) | Pays de l'écoute |
| `completed` | BOOLEAN | Écoute complète, > 30 s (défaut FALSE) |
| `event_source` | VARCHAR(20) | p2p, direct, cache (défaut p2p) |
| `created_at` | TIMESTAMP | Date d'insertion en base |

Index :

```sql
CREATE INDEX idx_listening_events_user_id     ON listening_events(user_id);
CREATE INDEX idx_listening_events_track_id    ON listening_events(track_id);
CREATE INDEX idx_listening_events_timestamp   ON listening_events(timestamp);
CREATE INDEX idx_listening_events_ts_partition ON listening_events(date_trunc('hour', timestamp));
```

---

### 3.7 `daily_streams`

Agrégats **batch** journaliers par morceau (calculés par Airflow, issue #7).

| Colonne | Type | Description |
|---|---|---|
| `track_id` | UUID | FK vers `tracks(id)` |
| `date` | DATE | Jour d'agrégation |
| `total_streams` | BIGINT | Nombre total d'écoutes (défaut 0) |
| `unique_listeners` | BIGINT | Auditeurs uniques (défaut 0) |
| `total_duration_ms` | BIGINT | Durée totale écoutée (défaut 0) |
| `countries` | TEXT[] | Pays concernés |
| `updated_at` | TIMESTAMP | Date de mise à jour |

```sql
PRIMARY KEY (track_id, date)
```

> Attention : la colonne s'appelle bien `total_streams` (et non `stream_count`).
> L'issue #7 mentionne « par stream_count » par abus de langage — c'est `total_streams` qu'il faut trier.

---

### 3.8 `artist_stats`

Agrégats journaliers par artiste.

| Colonne | Type | Description |
|---|---|---|
| `artist_id` | UUID | FK vers `artists(id)` |
| `date` | DATE | Jour d'agrégation |
| `total_streams` | BIGINT | Écoutes totales de l'artiste (défaut 0) |
| `unique_listeners` | BIGINT | Auditeurs uniques (défaut 0) |
| `top_track_id` | UUID | Morceau le plus écouté du jour |
| `updated_at` | TIMESTAMP | Date de mise à jour |

```sql
PRIMARY KEY (artist_id, date)
```

---

### 3.9 `recommendations`

Recommandations générées par utilisateur (issue #8). Doublées dans Redis (`reco:{user_id}`) pour l'accès rapide.

| Colonne | Type | Description |
|---|---|---|
| `user_id` | UUID | Identifiant utilisateur |
| `track_id` | UUID | FK vers `tracks(id)` — morceau recommandé |
| `score` | FLOAT | Score de recommandation, **NOT NULL** |
| `generated_at` | TIMESTAMP | Date de génération |

```sql
PRIMARY KEY (user_id, track_id)
```

---

### 3.10 `dead_letter_events`

Dead Letter Queue : événements invalides ou non traitables (issue #9).

| Colonne | Type | Description |
|---|---|---|
| `id` | UUID | Clé primaire |
| `original_topic` | VARCHAR(100) | Source : redis_pub_sub, kafka_topic… |
| `payload` | JSONB | Données brutes de l'événement, **NOT NULL** |
| `error_type` | VARCHAR(100) | Type d'erreur |
| `error_message` | TEXT | Message détaillé |
| `retry_count` | INT | Tentatives de retraitement (défaut 0) |
| `status` | VARCHAR(20) | pending, reprocessed, abandoned (défaut pending) |
| `created_at` | TIMESTAMP | Entrée dans la DLQ |
| `last_retry_at` | TIMESTAMP | Dernière tentative |
| `resolved_at` | TIMESTAMP | Résolution |

Index :

```sql
CREATE INDEX idx_dlq_status     ON dead_letter_events(status);
CREATE INDEX idx_dlq_created_at ON dead_letter_events(created_at);
```

---

### 3.11 `realtime_top_tracks`

Alimentée par Spark Structured Streaming (`streaming_trends_job`, fenêtres de 5 min).

| Colonne | Type | Description |
|---|---|---|
| `window_start` | TIMESTAMP | Début de fenêtre, **NOT NULL** |
| `window_end` | TIMESTAMP | Fin de fenêtre, **NOT NULL** |
| `track_id` | UUID | FK vers `tracks(id)` |
| `stream_count` | BIGINT | Écoutes sur la fenêtre (défaut 0) |
| `unique_listeners` | BIGINT | Auditeurs uniques (défaut 0) |
| `updated_at` | TIMESTAMP | Date de mise à jour |

```sql
PRIMARY KEY (window_start, track_id)
```

> Ici la colonne s'appelle bien `stream_count` (table streaming), à ne pas confondre
> avec `total_streams` de `daily_streams` (table batch).

---

### 3.12 `fraud_detections`

Fraudes détectées par les jobs temps réel (issue #18).

| Colonne | Type | Description |
|---|---|---|
| `id` | UUID | Clé primaire |
| `user_id` | UUID | Utilisateur concerné |
| `peer_id` | UUID | Peer concerné |
| `fraud_type` | VARCHAR(100) | bot_stream, free_rider, burst_listen |
| `suspicion_score` | FLOAT | Score de suspicion |
| `evidence` | JSONB | Éléments justificatifs |
| `window_start` | TIMESTAMP | Début de fenêtre d'analyse |
| `window_end` | TIMESTAMP | Fin de fenêtre d'analyse |
| `detected_at` | TIMESTAMP | Date de détection |

---

### 3.13 `federated_catalog`

Morceaux provenant des autres groupes (issue #22). Pas de FK vers `tracks` : les `track_id` viennent d'instances externes.

| Colonne | Type | Description |
|---|---|---|
| `track_id` | UUID | Identifiant du morceau (externe), **NOT NULL** |
| `source_group` | VARCHAR(50) | Groupe d'origine (groupe-a, groupe-b…), **NOT NULL** |
| `artist_name` | VARCHAR(255) | Nom de l'artiste |
| `track_title` | VARCHAR(255) | Titre du morceau |
| `duration_ms` | INT | Durée |
| `genre` | VARCHAR(100) | Genre |
| `audio_peer_endpoint` | VARCHAR(500) | Endpoint du peer distant |
| `ingested_at` | TIMESTAMP | Date d'ingestion |

```sql
PRIMARY KEY (track_id, source_group)
```

La clé composite permet d'identifier un morceau par son id **et** son groupe d'origine (deux groupes peuvent générer le même track_id sans collision).

---

## 4. Réponses aux questions de l'issue #2

### 4.1 Pourquoi `listening_events` est indexé sur `timestamp` ET sur `date_trunc('hour', timestamp)` ?

Les deux index répondent à deux familles de requêtes différentes.

L'index sur `timestamp` sert aux **recherches sur un intervalle précis** :

```sql
SELECT * FROM listening_events
WHERE timestamp BETWEEN '2026-06-02 10:00:00' AND '2026-06-02 12:00:00';
```

L'index fonctionnel sur `date_trunc('hour', timestamp)` sert aux **agrégations groupées par heure**, sans recalculer la troncature à chaque ligne :

```sql
SELECT date_trunc('hour', timestamp) AS heure, COUNT(*)
FROM listening_events
GROUP BY date_trunc('hour', timestamp);
```

Sans l'index fonctionnel, PostgreSQL devrait évaluer `date_trunc(...)` sur toute la table à chaque agrégation horaire. La base est ainsi optimisée à la fois pour le filtrage temporel fin (pipelines batch sur une plage) et pour le partitionnement logique par heure (agrégats, stockage Parquet partitionné par heure dans l'issue #6).

### 4.2 Quelle est la différence entre `daily_streams` (batch) et `realtime_top_tracks` (Spark) ?

| | `daily_streams` | `realtime_top_tracks` |
|---|---|---|
| Type | Batch | Streaming |
| Alimentée par | Airflow (`aggregation_pipeline`) | Spark Structured Streaming |
| Granularité | 1 ligne par (track, **jour**) | 1 ligne par (**fenêtre 5 min**, track) |
| Latence | Différée (recalcul périodique) | Quasi temps réel |
| Vocation | Bilan consolidé, stable, recalculable | Tendance immédiate, volatile |
| Métrique | `total_streams` | `stream_count` |

Les deux sont complémentaires : `realtime_top_tracks` donne une vision instantanée des tendances, `daily_streams` une vérité consolidée. L'issue #19 (réconciliation) compare justement les deux pour vérifier qu'elles convergent.

### 4.3 Pourquoi `dead_letter_events.payload` est en `JSONB` plutôt qu'en `TEXT` ?

`JSONB` conserve l'événement brut **tout en restant interrogeable** par PostgreSQL :

```sql
SELECT payload->>'track_id'
FROM dead_letter_events
WHERE status = 'pending';
```

Avantages concrets pour une DLQ : on peut filtrer/extraire des champs précis du payload, indexer (GIN) si besoin, diagnostiquer une erreur sans parser une chaîne, et surtout **retraiter automatiquement** l'événement après correction (issue #9). En `TEXT`, le payload ne serait qu'une chaîne opaque qu'il faudrait reparser côté applicatif à chaque lecture. `JSONB` (vs `JSON`) stocke une forme binaire normalisée, plus rapide à requêter.

> Le choix **ETL vs ELT** par pipeline est traité séparément dans `docs/ARCHITECTURE.md`, comme demandé par l'issue #2.

---

## 5. Conclusion

Le modèle couvre tout le cycle de vie de la donnée : ingestion du catalogue, génération et stockage des événements d'écoute, agrégats batch, tendances temps réel, recommandations, détection de fraude, gestion des erreurs (DLQ) et interconnexion inter-groupes. Le découplage volontaire des tables de qualité/fraude/fédération (sans FK stricte) permet d'absorber des données malformées ou externes sans casser l'intégrité du catalogue interne.
