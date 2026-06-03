# Data Contracts Inter-Groupes SPOTIFY

> Ce document définit les formats partagés entre groupes pour les topics Kafka inter-groupes.
> Tous les groupes **doivent** respecter ces schémas pour garantir l'interopérabilité.

---

## Topics partagés

| Topic | Partitions | RF | Description |
|-------|-----------|-----|-------------|
| `catalog_federation` | 6 | 3 | Tracks publiés par chaque groupe |
| `p2p_cross_requests` | 6 | 3 | Requêtes P2P cross-groupe |
| `global_metrics`     | 6 | 3 | Métriques agrégées pour le Top 50 Global |

---

## 1. catalog_federation_schema (v1.0)

**Fichier :** [contracts/catalog_federation_schema.json](../contracts/catalog_federation_schema.json)

### Champs obligatoires

| Champ | Type | Contrainte | Description |
|-------|------|-----------|-------------|
| `track_id` | UUID | required | ID unique du track (stable entre republications) |
| `source_group` | string | `^groupe-[a-z]$` | Groupe émetteur |
| `artist_name` | string | 1-255 chars | Nom de l'artiste |
| `track_title` | string | 1-255 chars | Titre du morceau |
| `duration_ms` | integer | 1–3 600 000 | Durée en millisecondes |
| `schema_version` | string | `"1.0"` | Version du contrat |

### Exemple

```json
{
  "track_id": "550e8400-e29b-41d4-a716-446655440000",
  "source_group": "groupe-c",
  "artist_name": "Daft Punk",
  "track_title": "Get Lucky",
  "duration_ms": 248000,
  "genre": "Electronic",
  "audio_peer_endpoint": "http://groupe-c-peer:8000/tracks/550e8400",
  "published_at": "2026-06-03T08:00:00Z",
  "schema_version": "1.0"
}
```

---

## 2. p2p_cross_request_schema (v1.0)

**Fichier :** [contracts/p2p_cross_request_schema.json](../contracts/p2p_cross_request_schema.json)

### Flux

```
Groupe A détecte un track manquant → publie dans p2p_cross_requests
Groupe B (owner) consomme → répond via response_url
Groupe A reçoit le chunk audio
```

### Champs obligatoires

| Champ | Type | Description |
|-------|------|-------------|
| `request_id` | UUID | ID de la requête |
| `requester_group` | string | Groupe qui demande |
| `target_group` | string | Groupe qui possède le track |
| `track_id` | UUID | Track demandé |
| `peer_endpoint` | string | Adresse du peer demandeur |
| `timestamp` | datetime | Heure de la requête |

---

## 3. global_metrics_schema (v1.0)

**Fichier :** [contracts/global_metrics_schema.json](../contracts/global_metrics_schema.json)

### Utilisation

Chaque groupe publie ses métriques quotidiennes → `global_aggregation_pipeline`
agrège tous les messages → `top50:global` dans Redis.

### Champs obligatoires

| Champ | Type | Description |
|-------|------|-------------|
| `source_group` | string | Groupe émetteur |
| `date` | date | Date des métriques (YYYY-MM-DD) |
| `top_tracks` | array[50] | Liste des 50 meilleurs tracks locaux |
| `schema_version` | string | `"1.0"` |

---

## Décisions architecture inter-groupes

### Pourquoi Kafka et non REST ?

- **Découplage** : un groupe peut être hors ligne sans bloquer les autres
- **Replay** : le topic `catalog_federation` peut être relu depuis le début
- **Exactly-once** : `isolation.level=read_committed` sur tous les consommateurs

### Gestion des incompatibilités de schéma

Events non conformes → `dead_letter_events` avec `error_type=federation_schema`.
Le DAG `dlq_reprocessing_pipeline` tente le retraitement toutes les heures.

### Contact ambassadeurs

Chaque groupe désigne un ambassadeur responsable de la négociation des schémas.
Toute modification de schéma → bump de version (ex: `1.0` → `1.1`) + préavis 24h.
