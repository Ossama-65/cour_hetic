# Rapport journalier du Jour 2 : 02/06/2026 : Groupe KFK

## 1. Issues fermées

* #4 — DAG `catalog_ingestion_pipeline`
* #6 — DAG `streaming_events_pipeline`
* #9 — DAG `dlq_reprocessing_pipeline`
* #2 — Documentation du modèle de données PostgreSQL

## 2. En cours / non fermées

  - Correction effectuée.
  - Pull Request à ouvrir ou à merger.

* #5 — Simulateur P2P
  - Tâche attribuée à Linda.
  - À démarrer sur la branche `groupe-kfk/feat/issue-5-p2p-simulator`.

## 3. Difficultés rencontrées

* La branche #4 contenait aussi des modifications liées aux issues #6 et #9.
  - Solution : nettoyage de branche pour garder uniquement le DAG catalogue.

* Le DAG `streaming_events_pipeline` ne consommait pas les événements Redis au départ.
  - Cause : Airflow utilise Redis DB 1 alors que les tests injectaient dans Redis DB 0.
  - Solution : utiliser `redis-cli -n 1` et documenter que le simulateur doit écrire dans Redis DB 1.

* La Pull Request #9 affichait des conflits.
  - Cause : mauvaise branche de base utilisée sur GitHub (`main` au lieu de `groupe-kfk/main`).
  - Solution : recréer ou modifier la PR avec `groupe-kfk/main` comme base.

## 4. Objectif demain

* Finaliser et merger la documentation `DATA_MODEL.md`.
* Implémenter ou accompagner l’issue #5 : simulateur P2P.
* Tester le flux complet :
  - simulateur P2P ;
  - Redis DB 1 ;
  - DAG streaming ;
  - PostgreSQL `listening_events` ;
  - Parquet MinIO.
* Préparer les issues batch suivantes : agrégats, recommandations et tests.

## 5. Répartition du travail

| Membre | Branche | Tâches |
|---|---|---|
| Steve | `groupe-kfk/feat/report-day-2` | Coordination, tests d’intégration, rapport journalier |
| Chantal | `groupe-kfk/feat/issue-2-data-model` | PR documentation du modèle de données |
| Théophane | `groupe-kfk/feat/issue-4-catalog-dag`, `issue-6`, `issue-9` | DAGs catalogue, streaming et DLQ corrigés/testés |
| Linda | `groupe-kfk/feat/issue-5-p2p-simulator` | Simulateur P2P, génération et envoi d’événements dans Redis DB 1 |

## 6. Validations techniques

### DAG catalogue

* `catalog_ingestion_pipeline` exécuté avec succès.
* Données chargées :
  - Artists : 45
  - Albums : 113
  - Tracks : 1349
  - DLQ : 0

### DAG streaming

* `streaming_events_pipeline` exécuté avec succès.
* Événement Redis consommé : 1
* Événement validé : 1
* Événement enrichi : 1
* Événement écrit en Parquet : 1
* Événement inséré dans `listening_events` : 1

### DAG DLQ

* `dlq_reprocessing_pipeline` exécuté avec succès.
* Événement DLQ trouvé : 1
* Événement corrigé : 1
* Événement réinséré dans `listening_events` : 1
* Statut final : `reprocessed`
* `retry_count` final : 1
