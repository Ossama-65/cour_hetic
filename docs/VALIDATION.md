# Critères de validation — Issues #1 à #5
## Groupe D · Phase 1 — Data Pipelines Batch

> Ce document recense les preuves de validation pour les 5 premières issues du projet.
> Les screenshots sont dans `docs/screenshots/`.

---

## Issue #1 — Setup Docker Compose

**Objectif** : Lancer la stack complète et vérifier que tous les services sont `Up` ou `healthy`.

**Commande exécutée :**
```bash
cd ~/spotify && docker compose ps
```

**Critère de validation** : Tous les services affichent `Up` ou `healthy` dans la colonne STATUS.

**Services validés :**
| Service | Image | Status |
|---|---|---|
| spotify-airflow-scheduler-1 | apache/airflow:2.9.1 | Up (unhealthy)* |
| spotify-airflow-webserver-1 | apache/airflow:2.9.1 | Up (healthy) |
| spotify-airflow-worker-1 | apache/airflow:2.9.1 | Up |
| spotify-airflow-triggerer-1 | apache/airflow:2.9.1 | Up |
| spotify-kafka-1-1 | confluentinc/cp-kafka:7.6.0 | Up |
| spotify-kafka-2-1 | confluentinc/cp-kafka:7.6.0 | Up |
| spotify-kafka-3-1 | confluentinc/cp-kafka:7.6.0 | Up |
| spotify-kafka-ui-1 | provectuslabs/kafka-ui | Up |
| spotify-minio-1 | minio/minio | Up (healthy) |
| spotify-postgres-1 | postgres:15 | Up (healthy) |
| spotify-redis-1 | redis:7 | Up (healthy) |
| spotify-spark-master | apache/spark:3.5.0 | Up |
| spotify-spark-worker-1 | apache/spark:3.5.0 | Up |
| spotify-spark-worker-2 | apache/spark:3.5.0 | Up |

> \* Le scheduler Airflow affiche `unhealthy` sur le healthcheck HTTP mais fonctionne correctement.
> Il planifie et déclenche les DAGs normalement. Ce comportement est connu sur Airflow 2.9 avec
> le CeleryExecutor : le healthcheck HTTP du scheduler répond lentement mais le service est opérationnel.

**Screenshots :**

![docker compose ps](screenshots/issue1_docker_compose_ps.png)
*`docker compose ps` — tous les services démarrés*

![Airflow UI](screenshots/issue1_airflow_ui.png)
*Interface Airflow sur http://localhost:8080 — 5 DAGs actifs visibles*

---

## Issue #2 — Schéma PostgreSQL & ERD

**Objectif** : Vérifier que le schéma PostgreSQL est créé avec toutes les tables, et que le fichier
`docs/DATA_MODEL.md` est complété avec le diagramme ERD.

**Commande exécutée :**
```bash
docker exec spotify-postgres-1 psql -U spotify spotify -c "\dt"
```

**Critère de validation** : 13 tables présentes dans le schéma `public`.

**Tables créées :**
```
albums · artist_stats · artists · daily_streams · dead_letter_events
federated_catalog · fraud_detections · genres · listening_events
peers · realtime_top_tracks · recommendations · tracks
```

**Screenshots :**

![Tables PostgreSQL](screenshots/issue2_postgresql_tables.png)
*`\dt` dans psql — 13 tables présentes (13 rows)*

![ERD partie 1](screenshots/issue2_erd_part1.png)
*Diagramme ERD — Module catalogue (genres / artists / albums / tracks)*

![ERD partie 2](screenshots/issue2_erd_part2.png)
*Diagramme ERD — Module événements (listening_events / peers / daily_streams / recommendations)*

> Le fichier `docs/DATA_MODEL.md` contient le diagramme ERD complet ainsi que la description
> de chaque table et les choix de conception (UUID comme PK, idempotence, index sur timestamp,
> JSONB pour les payloads DLQ).

---

## Issue #3 — Data Generator — Faker

**Objectif** : Vérifier que les 4 tests unitaires `TestDataGenerator` passent sans modification.

**Commande exécutée :**
```bash
export PYTHONPATH=$(pwd)
python -m pytest tests/unit/test_transformations.py::TestDataGenerator -v
```

**Critère de validation** : `4 passed` — aucun test échoué.

**Tests validés :**
| Test | Résultat |
|---|---|
| `test_generate_catalog_structure` | PASSED |
| `test_generated_track_has_required_fields` | PASSED |
| `test_generated_artist_has_label` | PASSED |
| `test_track_ids_are_unique` | PASSED |

**Screenshot :**

![pytest 4 passed](screenshots/issue3_pytest_4passed.png)
*`pytest TestDataGenerator` — 4 passed in 0.42s*

---

## Issue #4 — DAG catalog_ingestion_pipeline

**Objectif** : Vérifier que le DAG `catalog_ingestion_pipeline` s'exécute avec succès
et que tous les tests unitaires passent.

**Commandes exécutées :**
```bash
# Vérification via l'UI Airflow -> http://localhost:8080
# DAG : catalog_ingestion_pipeline -> Graph -> toutes les tâches en "success"

# Tests unitaires complets
export PYTHONPATH=$(pwd)
python -m pytest tests/unit/ -v --tb=short
```

**Critère de validation** :
- Toutes les tâches du DAG affichent le statut `success` dans la vue Graph
- `18 passed, 0 failed` pour les tests unitaires

**Tâches du DAG validées :**
| Tâche | Statut |
|---|---|
| `extract_from_minio` | success |
| `validate_schema` | success |
| `transform_catalog` | success |
| `load_to_postgres` | success |
| `notify_success` | success |

**Screenshots :**

![Airflow DAG vert](screenshots/issue4_airflow_dag_vert.png)
*Vue Graph du DAG `catalog_ingestion_pipeline` — toutes les tâches en success*

![pytest 18 passed](screenshots/issue4_pytest_18passed.png)
*`pytest tests/unit/` — 18 passed, 4 warnings in 0.30s*

---

## Issue #5 — Simulateur P2P

**Objectif** : Vérifier que le simulateur P2P publie des événements JSON en continu sur Redis.

**Commandes exécutées :**

Terminal 1 — lancement du simulateur :
```bash
export PYTHONPATH=$(pwd)
python -m src.p2p_simulator.simulator --peers 10 --rate 3
```

Terminal 2 — écoute Redis :
```bash
docker exec -it spotify-redis-1 redis-cli subscribe listening_events
```

**Critère de validation** : Des messages JSON arrivent en continu sur le canal `listening_events`
avec les 10 champs requis : `event_id`, `user_id`, `track_id`, `source_peer`, `timestamp`,
`duration_ms`, `device_type`, `geo_country`, `completed`, `event_source`.

**Exemple d'événement reçu :**
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

**Screenshots :**

![Redis subscribe start](screenshots/issue5_redis_subscribe_start.png)
*`redis-cli subscribe listening_events` — premier message reçu*

![Redis events continu](screenshots/issue5_redis_events_continu.png)
*Flux d'événements JSON continu sur le canal `listening_events`*

---

## Résumé des validations

| Issue | Titre | Critère | Statut |
|---|---|---|---|
| #1 | Setup Docker Compose | `docker compose ps` tous Up + Airflow UI accessible | Validé |
| #2 | Schema PostgreSQL & ERD | 13 tables + `DATA_MODEL.md` complété | Validé |
| #3 | Data Generator Faker | `pytest TestDataGenerator` 4 passed | Validé |
| #4 | DAG catalog_ingestion | DAGRun vert + `pytest tests/unit/` 18 passed | Validé |
| #5 | Simulateur P2P | `redis-cli subscribe` events JSON en continu | Validé |
