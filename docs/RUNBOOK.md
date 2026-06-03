# RUNBOOK SPOTIFY — Procédures incidents

> Référence opérationnelle Phase 1 et Phase 2.
> Un bon runbook = ce dont vous auriez eu besoin pendant la panne.

---

## Incidents Phase 1 — Airflow / Batch

### INC-01 — DAG bloqué en "running" depuis > 30 minutes

**Symptômes :** Une tâche reste en état `running` dans l'UI Airflow sans progresser.

**Causes probables :**
- Worker Airflow mort ou surchargé (OOM)
- Tâche en attente d'une ressource externe (PostgreSQL, MinIO) indisponible
- Deadlock PostgreSQL sur une connexion non fermée

**Diagnostic :**
```bash
# 1. Voir les logs de la tâche bloquée
docker compose logs airflow-worker -f --tail=100

# 2. Vérifier l'état des workers Celery
docker exec cours_hetic-airflow-worker-1 airflow celery status

# 3. Vérifier les connexions PostgreSQL actives
docker exec cours_hetic-postgres-1 psql -U airflow -d airflow -c \
  "SELECT pid, state, query_start, left(query, 80) FROM pg_stat_activity WHERE state != 'idle';"
```

**Résolution :**
```bash
# Option A : marquer la tâche comme failed et relancer
docker exec cours_hetic-airflow-scheduler-1 \
  airflow tasks clear <dag_id> -t <task_id> --yes

# Option B : redémarrer le worker proprement
docker compose restart airflow-worker

# Option C : si la tâche est zombie (running mais pas de process actif)
docker exec cours_hetic-airflow-scheduler-1 \
  airflow tasks failed <dag_id> <task_id> <execution_date>
```

**Prévention :**
- `execution_timeout` configuré sur chaque tâche (10–30 min selon la tâche)
- `retries: 2–3` sur les tâches réseau avec `retry_exponential_backoff: True`
- Airflow Pool limité pour éviter la surcharge du worker

---

### INC-02 — PostgreSQL : `too many connections` / connexions exhausted

**Symptômes :**
```
FATAL: too many connections for role "spotify"
OperationalError: connection pool exhausted
```

**Diagnostic :**
```sql
-- Connexions actives par rôle
SELECT usename, count(*), state
FROM pg_stat_activity
GROUP BY usename, state
ORDER BY count DESC;

-- Limite actuelle
SHOW max_connections;
```

**Résolution :**
```bash
# 1. Tuer les connexions idle du rôle spotify
docker exec cours_hetic-postgres-1 psql -U airflow -d airflow -c "
  SELECT pg_terminate_backend(pid)
  FROM pg_stat_activity
  WHERE usename = 'spotify' AND state = 'idle';"

# 2. Si insuffisant — redémarrer le pool Airflow
docker compose restart airflow-worker airflow-scheduler

# 3. Augmenter max_connections (nécessite redémarrage PostgreSQL)
# Dans docker-compose.yml ajouter : POSTGRES_MAX_CONNECTIONS: 200
docker compose up -d --force-recreate postgres
```

**Prévention :**
- Fermer explicitement les connexions (`conn.close()` dans un `finally`)
- Créer un Airflow Pool `spotify_postgres_pool` (max 10 slots) : Admin → Pools
- Ajouter `pool: "spotify_postgres_pool"` sur les tâches SQL

---

### INC-03 — MinIO inaccessible depuis Airflow

**Symptômes :**
```
botocore.exceptions.EndpointResolutionError: Could not connect to http://minio:9000
NoSuchBucket: The specified bucket does not exist
```

**Diagnostic :**
```bash
# 1. Vérifier que MinIO est healthy
docker compose ps minio
curl -f http://localhost:9000/minio/health/live && echo "MinIO OK"

# 2. Tester la connexion depuis le container Airflow
docker exec cours_hetic-airflow-worker-1 python -c "
import boto3, os
s3 = boto3.client('s3', endpoint_url='http://minio:9000',
                  aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
print([b['Name'] for b in s3.list_buckets()['Buckets']])"
```

**Résolution :**
```bash
# 1. Redémarrer MinIO
docker compose restart minio
# Attendre le healthcheck (~15s), vérifier : docker compose ps minio

# 2. Recréer les buckets si perdus (volume supprimé accidentellement)
docker compose up minio-init
# Recrée : spotify-parquet, spotify-checkpoints, spotify-audio, labels-raw

# 3. Relancer le DAGRun échoué
# UI Airflow → DAG → DAGRun → "Clear" les tâches failed
```

**Prévention :**
- Ne jamais faire `docker compose down -v` en production (détruit les données MinIO)
- Préférer `docker compose down` (sans `-v`) pour conserver les volumes

---

### INC-04 — Dead Letter Queue : croissance anormale (> 1000 events pending)

**Symptômes :**
```sql
SELECT COUNT(*) FROM dead_letter_events WHERE status = 'pending';
-- résultat > 1000 et croissant rapidement
```

**Causes probables :**
- Le simulateur utilise des track_ids aléatoires non présents en BD (SAMPLE_TRACKS)
- `catalog_ingestion_pipeline` n'a pas encore tourné → catalogue vide
- `dlq_reprocessing_pipeline` en pause ou échouant silencieusement

**Diagnostic :**
```sql
-- Répartition par error_type
SELECT error_type, status, COUNT(*)
FROM dead_letter_events
GROUP BY error_type, status
ORDER BY count DESC;
```

**Résolution :**
```bash
# Si error_type = 'unknown_track' en masse → catalogue pas chargé
# Déclencher catalog_ingestion_pipeline manuellement :
curl -X POST http://localhost:8080/api/v1/dags/catalog_ingestion_pipeline/dagRuns \
  -H "Content-Type: application/json" -u admin:admin \
  -d '{"dag_run_id": "manual_reload"}'

# Purger les events abandonnés > 7 jours
docker exec cours_hetic-postgres-1 psql -U spotify -d spotify -c "
  DELETE FROM dead_letter_events
  WHERE status = 'abandoned' AND created_at < NOW() - INTERVAL '7 days';"
```

---

## Incidents Phase 2 — Kafka / Spark

### INC-05 — Consumer lag Kafka > 10 000 messages

**Symptômes :** Kafka UI → consumer group `spark-streaming-trends` → lag croissant

**Diagnostic :**
```bash
docker logs spark-master -f | grep "Batch Duration"
docker stats spark-worker-1 --no-stream
```

**Résolution :**
```bash
# Augmenter le trigger interval
# .trigger(processingTime="30 seconds")

# Augmenter la mémoire Spark dans docker-compose
# SPARK_WORKER_MEMORY: 4G
docker compose up -d --force-recreate spark-worker-1
```

---

### INC-06 — Job Spark crash OutOfMemoryError

**Symptômes :** `java.lang.OutOfMemoryError: GC overhead limit exceeded`

**Résolution :**
```bash
# Réduire les offsets consommés par batch
# .option("maxOffsetsPerTrigger", "5000")

# Augmenter driver/executor memory
# spark-submit --driver-memory 2g --executor-memory 2g

# Ajouter TTL sur flatMapGroupsWithState
# GroupState.setTimeoutDuration("1 hour")
```

---

### INC-07 — Spark ne reprend pas depuis le checkpoint

**Symptômes :** Après redémarrage, le job repart de zéro.

**Diagnostic :**
```bash
# Vérifier que le checkpoint existe sur MinIO
docker exec cours_hetic-minio-1 mc ls local/spotify-checkpoints/ 2>/dev/null \
  || echo "Checkpoint absent"

docker logs spark-master | grep -i "checkpoint\|resume"
```

**Résolution :**
```bash
# Checkpoint corrompu : supprimer et relancer (avec risque de perte d'events)
mc rm --recursive --force local/spotify-checkpoints/streaming_trends/
docker compose restart spark-master

# Vérifier l'absence de doublons après relance
docker exec cours_hetic-postgres-1 psql -U spotify -d spotify -c \
  "SELECT COUNT(*) - COUNT(DISTINCT id) AS doublons FROM listening_events;"
# → doit retourner 0
```

---

## Chaos Engineering — Résultats (Issue #25)

> À compléter lors des tests de chaos engineering.

### Scénario 1 : Arrêt d'un broker Kafka
**Commande :** `docker compose stop kafka-2`
**Comportement observé :** ...
**Recovery automatique :** oui / non — détails : ...

### Scénario 2 : Kill du driver Spark
**Commande :** `docker compose kill spark-master`
**Recovery depuis checkpoint :** oui / non — détails : ...
**Doublons introduits :** 0 / N — vérification : ...

### Scénario 3 : Coupure PostgreSQL 2 minutes
**Commande :** `docker compose stop postgres` → 2min → `docker compose start postgres`
**Comportement observé (Airflow) :** ...
**Données perdues :** oui / non — détails : ...
