# SPOTIFY — Plateforme Streaming Distribuée
## M1 Data & IA — Groupe D

## Architecture
## Stack technique

| Composant | Version | Role |
|---|---|---|
| Apache Kafka | 3.6 KRaft | Streaming events |
| Apache Spark | 3.5 | Traitement temps réel |
| Apache Airflow | 2.9.1 | Orchestration batch |
| PostgreSQL | 15 | Stockage principal |
| Redis | 7 | Cache + Top50 Global |
| MinIO | latest | Stockage fichiers JSON |

## Démarrage rapide

```bash
# Phase 1 + 2
docker compose -f docker-compose.yml -f docker-compose.kafka.yml up -d

# Simulateur
python3 -m src.p2p_simulator.simulator --peers 10 --rate 3

# Spark streaming
docker compose -f docker-compose.yml -f docker-compose.kafka.yml exec spark-master \
  /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
  --conf spark.jars.ivy=/tmp/.ivy \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/streaming_trends_job.py
```

## DAGs Airflow (http://localhost:8080)

| DAG | Schedule | Role |
|---|---|---|
| catalog_ingestion_pipeline | 02:00 UTC | Ingestion MinIO → PostgreSQL |
| streaming_events_pipeline | continu | Events Kafka → PostgreSQL |
| aggregation_pipeline | 04:00 UTC | Stats journalières |
| recommendation_pipeline | 05:00 UTC | Collaborative filtering |
| dlq_reprocessing_pipeline | @hourly | Retraitement erreurs |
| reconciliation_pipeline | */30 | Cohérence streaming/batch |
| late_events_reprocessing | @hourly | Events tardifs DLQ |
| catalog_federation_pipeline | 03:00 UTC | Fédération inter-groupes |
| top50_global_pipeline | */5 | Top 50 Global Redis |

## Tests

```bash
pytest tests/ -v
# 18/18 tests passent
```

## Chaos Engineering

Voir `docs/RUNBOOK.md` — 3 scénarios validés.
