# DATA MODEL — Groupe KFK — Plateforme Spotify

## Vue d'ensemble

La base de données PostgreSQL contient 13 tables réparties en 4 domaines :

- Catalogue : artists, albums, tracks, genres
- Evenements : listening_events, peers
- Agregats : daily_streams, artist_stats, realtime_top_tracks
- Qualite et IA : dead_letter_events, recommendations, fraud_detections, federated_catalog

---

## ERD — Diagramme de relations

artists
  |-- albums
  |-- tracks --> listening_events
  |-- artist_stats
  |-- tracks --> daily_streams

listening_events (invalides) --> dead_letter_events
listening_events --> recommendations
listening_events --> fraud_detections
artists (autres groupes) --> federated_catalog

---

## Tables

### 1. artists
| Colonne | Type | Description |
|---------|------|-------------|
| id | UUID | Identifiant unique |
| name | VARCHAR(255) | Nom de l artiste |
| country | VARCHAR(100) | Pays d origine |
| label | VARCHAR(255) | Label musical |
| genres | TEXT[] | Genres musicaux |
| monthly_listeners | INTEGER | Auditeurs mensuels |
| created_at | TIMESTAMP | Date de creation |
| updated_at | TIMESTAMP | Date de mise a jour |

### 2. albums
| Colonne | Type | Description |
|---------|------|-------------|
| id | UUID | Identifiant unique |
| artist_id | UUID | Cle etrangere vers artists |
| title | VARCHAR(255) | Titre de l album |
| release_date | DATE | Date de sortie |
| created_at | TIMESTAMP | Date de creation |

### 3. tracks
| Colonne | Type | Description |
|---------|------|-------------|
| id | UUID | Identifiant unique |
| artist_id | UUID | Cle etrangere vers artists |
| album_id | UUID | Cle etrangere vers albums |
| title | VARCHAR(255) | Titre du morceau |
| duration_ms | INTEGER | Duree en millisecondes |
| release_date | DATE | Date de sortie |
| created_at | TIMESTAMP | Date de creation |

### 4. genres
| Colonne | Type | Description |
|---------|------|-------------|
| id | UUID | Identifiant unique |
| name | VARCHAR(100) | Nom du genre |

### 5. listening_events
| Colonne | Type | Description |
|---------|------|-------------|
| id | UUID | Identifiant unique |
| user_id | UUID | Utilisateur |
| track_id | UUID | Morceau ecoute |
| timestamp | TIMESTAMP | Horodatage |
| duration_ms | INTEGER | Duree d ecoute |
| device_type | VARCHAR(50) | Type d appareil |
| geo_country | VARCHAR(10) | Pays |
| completed | BOOLEAN | Ecoute complete |
| event_source | VARCHAR(50) | Source P2P ou direct |

### 6. daily_streams
| Colonne | Type | Description |
|---------|------|-------------|
| id | UUID | Identifiant unique |
| track_id | UUID | Morceau |
| date | DATE | Jour d agregation |
| stream_count | INTEGER | Nombre d ecoutes |
| unique_listeners | INTEGER | Auditeurs uniques |
| avg_duration_ms | FLOAT | Duree moyenne |
| updated_at | TIMESTAMP | Mise a jour |

### 7. dead_letter_events
| Colonne | Type | Description |
|---------|------|-------------|
| id | UUID | Identifiant unique |
| raw_payload | JSONB | Donnees brutes |
| error_message | TEXT | Message d erreur |
| status | VARCHAR(50) | Etat du retraitement |
| created_at | TIMESTAMP | Date de creation |

### 8. recommendations
| Colonne | Type | Description |
|---------|------|-------------|
| id | UUID | Identifiant unique |
| user_id | UUID | Utilisateur |
| track_id | UUID | Morceau recommande |
| score | FLOAT | Score de recommandation |
| created_at | TIMESTAMP | Date de creation |

### 9. peers
| Colonne | Type | Description |
|---------|------|-------------|
| id | UUID | Identifiant unique |
| peer_id | VARCHAR(255) | Identifiant du pair |
| ip_address | VARCHAR(50) | Adresse IP |
| status | VARCHAR(50) | Statut actif ou inactif |
| connected_at | TIMESTAMP | Date de connexion |

### 10. artist_stats
| Colonne | Type | Description |
|---------|------|-------------|
| id | UUID | Identifiant unique |
| artist_id | UUID | Cle etrangere vers artists |
| total_streams | INTEGER | Total des ecoutes |
| monthly_streams | INTEGER | Ecoutes du mois |
| updated_at | TIMESTAMP | Mise a jour |

### 11. realtime_top_tracks
| Colonne | Type | Description |
|---------|------|-------------|
| id | UUID | Identifiant unique |
| track_id | UUID | Morceau |
| rank | INTEGER | Classement |
| stream_count | INTEGER | Nombre d ecoutes |
| updated_at | TIMESTAMP | Mise a jour |

### 12. fraud_detections
| Colonne | Type | Description |
|---------|------|-------------|
| id | UUID | Identifiant unique |
| user_id | UUID | Utilisateur suspect |
| track_id | UUID | Morceau concerne |
| reason | TEXT | Raison de la fraude |
| detected_at | TIMESTAMP | Date de detection |

### 13. federated_catalog
| Colonne | Type | Description |
|---------|------|-------------|
| id | UUID | Identifiant unique |
| source_group | VARCHAR(50) | Groupe source |
| track_id | UUID | Morceau distant |
| artist_name | VARCHAR(255) | Nom de l artiste |
| title | VARCHAR(255) | Titre du morceau |
| imported_at | TIMESTAMP | Date d import |
