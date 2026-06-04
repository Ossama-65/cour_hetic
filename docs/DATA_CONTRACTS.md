# DATA CONTRACTS — Inter-groupes SPOTIFY

## Contexte

Chaque groupe expose ses données via Kafka et les valide contre un schéma JSON commun.
La règle de base : ne jamais faire confiance aux données des autres groupes.
Tout ce qui est invalide part en DLQ.

## Schémas définis

### 1. catalog_federation_schema.json
Format d'un track fédéré entre groupes.
Champ obligatoire : `source_group` pour tracer l'origine.
Version actuelle : `1.0`

### 2. p2p_cross_request_schema.json
Format d'une requête P2P cross-group (ex: demande de stream d'un peer distant).
TTL par défaut : 30 secondes. Au-delà, la requête est ignorée.
Version actuelle : `1.0`

### 3. global_metrics_schema.json
Format des métriques agrégées partagées pour construire le Top 50 Global Redis.
Chaque groupe envoie son top local toutes les 5 minutes.
Version actuelle : `1.0`

## Règles de validation

- Tout champ `uuid` doit matcher `^[0-9a-f-]{36}$`
- `duration_ms` entre 1000ms et 3 600 000ms (1s à 1h)
- `source_group` doit être dans la liste des groupes connus
- Tout event invalide → `dead_letter_events` avec `error_type = 'contract_violation'`

## Décisions ambassadeurs

| Date | Décision |
|---|---|
| 2026-06-04 | Adoption du format uuid v4 pour tous les identifiants |
| 2026-06-04 | Version schema `1.0` signée par tous les groupes |
| 2026-06-04 | DLQ obligatoire pour toute violation de contrat |
