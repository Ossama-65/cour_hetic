# Rapport journalier du Jour 1 : 01/06/2026 : Groupe KFK

## 1. Issues fermées

- #1 — Setup Docker Compose : validée
- #3 — Data Generator : validée

## 2. En cours / non fermées

- #2 — Schéma PostgreSQL et documentation DATA_MODEL.md
- #4 — DAG catalog_ingestion_pipeline
- #5 — Simulateur P2P

## 3. Difficultés rencontrées

- Port PostgreSQL 5432 déjà utilisé localement.
  - Solution : arrêt du service PostgreSQL local avec `sudo systemctl stop postgresql`.

- Disque saturé au démarrage du projet.
  - Solution : suppression d’Anaconda, anciens projets lourds et nettoyage Docker.

- Airflow Scheduler affiché `unhealthy` côté Docker.
  - Diagnostic : la commande `airflow jobs check --job-type SchedulerJob` retourne `Found one alive job`.
  - Conclusion : le scheduler fonctionne, le problème semble limité au healthcheck Docker.

## 4. Objectif demain

- Finaliser le DAG `catalog_ingestion_pipeline`.
- Charger les catalogues depuis MinIO vers PostgreSQL.
- Vérifier l’insertion des artistes, albums et tracks.
- Préparer le simulateur P2P.
- Avancer sur la documentation du modèle de données.

## 5. Répartition du travail

| Membre | Branche | Tâches |
|---|---|---|
| Steve | `groupe-kfk/feat/setup` | Setup, intégration, coordination, rapport journalier |
| Chantal | `groupe-kfk/feat/issue-2-data-model` | Documentation PostgreSQL, ERD, `DATA_MODEL.md`, début `RUNBOOK.md` |
| Théophane | `groupe-kfk/feat/issue-4-catalog-dag` | DAG `catalog_ingestion_pipeline`, ingestion MinIO vers PostgreSQL |
| Linda | `groupe-kfk/feat/issue-5-p2p-simulator` | Simulateur P2P, génération d’événements, publication Redis |

## 6. Validation technique du jour

- Airflow Webserver accessible : OK
- MinIO accessible : OK
- PostgreSQL accessible : OK
- Redis répond `PONG` : OK
- 13 tables PostgreSQL créées : OK
- 3 fichiers JSON générés :
  - `sunset_records.json`
  - `nightwave_music.json`
  - `urban_pulse.json`
- 3 fichiers uploadés dans MinIO bucket `labels-raw` : OK

## 7. Règles de travail Git

- Branche principale du groupe : `groupe-kfk/main`
- Chaque membre travaille sur sa branche dédiée.
- Les commits doivent être réguliers.
- Les Pull Requests doivent être ouvertes vers `groupe-kfk/main`.
- Ne pas committer `.env` ni `.venv`.
