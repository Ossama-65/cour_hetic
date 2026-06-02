# Rapport d'avancement — Lundi 1 juin 2026

**Projet :** SPOTIFY — Plateforme de streaming musical distribuée
**Phase :** Phase 1 — Batch Airflow (Milestone 1)
**Groupe :** MEKDAM Ghiles · CHABA Ramdane · Aouimeur Ouissem

---

## 1. Issues fermées

Les 5 premières issues de la Phase 1 sont terminées et fermées (commit `feat(phase-1): close #1 #2 #3 #4 #5`).

| # | Titre | Livrable | Validation |
|---|-------|----------|------------|
| **#1** | Setup Docker Compose et vérification de la stack | Stack complète opérationnelle : PostgreSQL (5432), Redis (6379), MinIO (9000/9001), Airflow (8080). Base `spotify` créée, 3 buckets MinIO en place. | `docker compose ps` → tous les services `Up` |
| **#2** | Schéma PostgreSQL et modèle de données | `docs/DATA_MODEL.md` : ERD (Mermaid) + inventaire des 13 tables et 6 index + réponses aux 3 questions. Mapping ETL/ELT par pipeline ajouté dans `docs/ARCHITECTURE.md`. | Schéma vérifié sur la base live : `nb_tables = 13`, 6 index explicites présents |
| **#3** | Data Generator (Faker) | `src/data_generator/generate_catalog.py` exécuté avec `--artists 15` → 3 catalogues JSON (`sunset_records`, `nightwave_music`, `urban_pulse`). Script d'upload MinIO ajouté (`upload_to_minio.py`). | `pytest ...::TestDataGenerator` → **4 PASSED** |
| **#4** | DAG `catalog_ingestion_pipeline` | DAG d'ingestion du catalogue : MinIO → validation → transformation (normalisation, dédoublonnage) → upsert PostgreSQL (`ON CONFLICT`). | DAG parse sans erreur, tâches enchaînées |
| **#5** | Simulateur P2P + Redis pub/sub | `src/p2p_simulator/simulator.py` : génère des événements d'écoute réalistes et les publie sur Redis pub/sub (modes `normal`, `fraud`, `late_events`). | Événements publiés sur les topics Redis `listening_events` / `p2p_network_events` |

---

## 2. En cours (non fermées)

Le reste de la Phase 1 est amorcé : les fichiers de DAG existent dans le repo mais sont encore au stade de squelette (`TODO` à implémenter).

| # | Titre | État |
|---|-------|------|
| **#6** | DAG `streaming_events_pipeline` | Squelette présent. Reste : consommation Redis en micro-batch (5 min), validation → DLQ, enrichissement catalogue, écriture Parquet + upsert PostgreSQL. |
| **#7** | DAG `aggregation_pipeline` + MinIO | Squelette présent. Reste : `ExternalTaskSensor`, calcul top tracks / stats artistes / métriques P2P, écriture des agrégats. |
| **#8** | DAG `recommendation_pipeline` | Squelette présent. Reste : collaborative filtering, écriture Redis (`reco:{user_id}`) + table `recommendations`. |
| **#9** | DAG `dlq_reprocessing_pipeline` | Squelette présent. Reste : retraitement des événements `pending`, logique de retry, mise à jour des statuts. |
| **#10** | Tests pytest + README + `doc_md` | Tests `TestDataGenerator` OK ; reste à couvrir les transformations des DAGs #6→#9 et compléter les `doc_md`. |

---

## 3. Difficultés rencontrées

**#1 — Docker / Airflow.** Le service `airflow-scheduler` apparaissait en `unhealthy` alors qu'il fonctionnait. Cause identifiée : le healthcheck `airflow jobs check ... --hostname $${HOSTNAME}` est en forme `CMD` (exec, sans shell), donc `${HOSTNAME}` n'est pas substitué et la commande échoue. Corrigé en passant en `CMD-SHELL` avec `--local`. Autre point d'attention : le script `init_spotify_db.sql` ne s'exécute qu'au **premier** démarrage du volume PostgreSQL — il faut `docker compose down -v` pour le rejouer, et le webserver démarre ~60 s après le scheduler.

**#2 — Modèle de données.** Deux subtilités à comprendre et documenter : le double index sur `listening_events` (`timestamp` pour le filtrage continu **et** `date_trunc('hour', timestamp)` pour le bucketing horaire, non redondants), et l'absence de table `users` (le `user_id` n'est donc pas une clé étrangère, choix volontaire de flux d'événements brut). Le choix `JSONB` vs `TEXT` pour la DLQ a aussi nécessité une justification (interrogeabilité, validation, accès rapide au retraitement).

**#3 — Data Generator.** L'upload vers MinIO nécessite que la stack soit lancée et accessible sur `localhost:9000` ; impossible à automatiser hors environnement local. Le dossier `data/labels/` est potentiellement ignoré par `.gitignore` (dossiers `data/` souvent exclus) — à vérifier avant commit pour décider si on versionne les JSON ou seulement le générateur.

**#4 — DAG catalog_ingestion.** Le fichier fourni est un squelette : il faut implémenter l'upsert avec gestion des conflits (`ON CONFLICT`) cohérente avec les contraintes `UNIQUE(name, label)` du schéma. Le `catchup=True` impose de soigner l'idempotence pour éviter les doublons lors d'un backfill.

**#5 — Simulateur P2P.** Attention à la cible Redis : `localhost:6379` depuis la machine hôte mais `redis:6379` à l'intérieur du réseau Docker, et la base utilisée est `db 1` (`redis://...:6379/1`). Bien séparer les deux topics (`listening_events` vs `p2p_network_events`) et calibrer le débit (`--rate`) pour ne pas saturer le consumer en micro-batch.

---

## 4. Objectif demain (mardi)

Terminer la Phase 1 en implémentant les 4 DAGs batch restants et la couche tests/documentation.

- **#6** — `streaming_events_pipeline` : consommation Redis micro-batch, validation + DLQ, enrichissement, Parquet (MinIO) + PostgreSQL.
- **#7** — `aggregation_pipeline` : agrégats quotidiens (`daily_streams`, `artist_stats`) via `ExternalTaskSensor`.
- **#8** — `recommendation_pipeline` : recommandations (collaborative filtering) → Redis + PostgreSQL.
- **#9** — `dlq_reprocessing_pipeline` : retraitement de la Dead Letter Queue avec logique de retry.
- **#10** — Tests pytest (transformations des nouveaux DAGs) + `doc_md` + mise à jour README.

**Critère de sortie Phase 1 :** les 5 DAGs s'exécutent sans erreur avec le simulateur P2P actif, agrégats cohérents, recommandations dans Redis, DLQ fonctionnelle, suite pytest verte.

---

## 5. Répartition du travail

Répartition **par issue, en rotation** sur les 3 membres. Sur 5 issues la rotation donne 2 / 2 / 1 ; elle reprend demain à **Aouimeur Ouissem** (issue #6) pour rééquilibrer sur la durée.

### Lundi (réalisé)

| # | Issue | Membre |
|---|-------|--------|
| #1 | Setup Docker Compose | **MEKDAM Ghiles** |
| #2 | Schéma PostgreSQL & modèle de données | **CHABA Ramdane** |
| #3 | Data Generator (Faker) | **Aouimeur Ouissem** |
| #4 | DAG catalog_ingestion_pipeline | **MEKDAM Ghiles** |
| #5 | Simulateur P2P + Redis pub/sub | **CHABA Ramdane** |

**Charge lundi :** Ghiles 2 issues · Ramdane 2 issues · Ouissem 1 issue.

### Mardi (prévu — la rotation reprend à Ouissem)

| # | Issue | Membre |
|---|-------|--------|
| #6 | DAG streaming_events_pipeline | **Aouimeur Ouissem** |
| #7 | DAG aggregation_pipeline + MinIO | **MEKDAM Ghiles** |
| #8 | DAG recommendation_pipeline | **CHABA Ramdane** |
| #9 | DAG dlq_reprocessing_pipeline | **Aouimeur Ouissem** |
| #10 | Tests pytest + README + doc_md | **MEKDAM Ghiles** |

**Cumul sur les 2 jours :** Ghiles 4 · Ramdane 3 · Ouissem 3 — équilibré.

> Chaque membre reste responsable de comprendre l'ensemble de l'architecture (objectif soutenance) ; la répartition fixe seulement le pilote de chaque issue.
