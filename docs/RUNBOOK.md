# RUNBOOK — Spotify Data Platform
## Groupe D — M1 Data & IA — Juin 2026

---

## Environnement

| Service       | URL                        | Credentials                  |
|---------------|----------------------------|------------------------------|
| Airflow UI    | http://localhost:8080       | admin / admin                |
| MinIO UI      | http://localhost:9001       | minioadmin / minioadmin      |
| Kafka UI      | http://localhost:8090       | Phase 2 uniquement           |
| Spark UI      | http://localhost:8888       | Phase 2 uniquement           |
| PostgreSQL    | localhost:5432              | spotify / spotify            |
| Redis         | localhost:6379              | —                            |

---

## Demarrage de la stack

### Phase 1 — Batch uniquement

```bash
cp .env.example .env
docker compose up -d
sleep 60 && docker compose ps
```

### Phase 2 — Avec Kafka et Spark

```bash
docker compose -f docker-compose.yml -f docker-compose.kafka.yml up -d
```

### Arreter tout

```bash
docker compose down
```

---

## Incidents rencontres

### INC-001 — git pull bloque par modifications locales non commitees

**Symptome**

```
error: Your local changes to the following files would be overwritten by merge:
        dags/recommendation_pipeline.py
Please commit your changes or stash them before you merge.
Aborting
```

**Cause** : modifications locales sur un fichier modifie aussi sur la branche distante.

**Resolution**

Si les modifications locales ne sont pas necessaires :
```bash
git checkout <fichier>
git pull origin groupe-d/main
```

Si les modifications locales doivent etre conservees :
```bash
git stash
git pull origin groupe-d/main
git stash pop
```

**Fichiers concernes** : `dags/recommendation_pipeline.py`

---

### INC-002 — pytest introuvable sur Windows

**Symptome**

```
bash: pytest: command not found
```

**Cause** : le script `pytest.exe` est installe dans un repertoire absent du PATH Windows (`C:\Users\DELL\AppData\Roaming\Python\Python313\Scripts`).

**Resolution**

```bash
python -m pytest tests/unit/ -v --tb=short
```

Ne pas utiliser `pytest` directement sous Git Bash sur Windows.

---

### INC-003 — pip ne peut pas installer pandas depuis les sources

**Symptome**

```
ERROR: Unknown compiler(s): [['icl'], ['cl'], ['cc'], ['gcc'], ['clang'], ['clang-cl'], ['pgcc']]
error: metadata-generation-failed
```

**Cause** : Python 3.13 + pandas 2.2.0 n'a pas de wheel precompile compatible. pip tente de compiler depuis les sources mais aucun compilateur C n'est disponible sur la machine.

**Resolution**

```bash
pip install "pandas>=2.0" --only-binary=:all:
```

Ou forcer la version avec wheel disponible :
```bash
pip install pandas --upgrade
```

---

### INC-004 — test_dag_structure.py echoue au import

**Symptome**

```
ModuleNotFoundError: No module named 'fcntl'
ERROR tests/structure/test_dag_structure.py
```

**Cause** : `fcntl` est un module POSIX uniquement. Airflow ne supporte pas Windows nativement. Ce test ne peut pas s'executer hors Linux/macOS.

**Resolution**

Lancer uniquement les tests unitaires :
```bash
python -m pytest tests/unit/ -v --tb=short
```

Les tests de structure (`tests/structure/`) sont a executer dans le conteneur Docker ou sous WSL2.

**Resultat attendu sur Windows** : 4 passed, 14 skipped, 0 failed.

---

### INC-005 — ImportError sur src.* dans les tests

**Symptome**

```
ImportError: No module named 'src'
```

**Cause** : `PYTHONPATH` n'inclut pas la racine du projet.

**Resolution**

```bash
export PYTHONPATH=$(pwd)
python -m pytest tests/unit/ -v --tb=short
```

---

## Verification de l'etat de la plateforme

### DAGs Airflow

```bash
# Lister les DAGs actifs
docker exec $(docker ps -qf "name=airflow-scheduler") airflow dags list

# Verifier le dernier run d'un DAG
docker exec $(docker ps -qf "name=airflow-scheduler") airflow dags list-runs -d catalog_ingestion_pipeline
```

### PostgreSQL

```bash
# Connexions actives
psql -U spotify spotify -c "SELECT count(*), state FROM pg_stat_activity GROUP BY state;"

# Tuer les connexions idle
psql -U spotify spotify -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state='idle';"

# Verifier le catalogue
psql -U spotify spotify -c "SELECT COUNT(*) FROM artists;"
psql -U spotify spotify -c "SELECT COUNT(*) FROM tracks;"

# DLQ
psql -U spotify spotify -c "SELECT status, COUNT(*) FROM dead_letter_events GROUP BY status;"

# Doublons listening_events (Phase 2)
psql -U spotify spotify -c "SELECT COUNT(*) - COUNT(DISTINCT id) AS doublons FROM listening_events;"
```

### Redis

```bash
# Recommandations
redis-cli keys 'reco:*' | head -5
redis-cli get reco:<user_id>

# Top 50 Global (Phase 2)
redis-cli get top50:global | python3 -m json.tool | head -30
```

### MinIO

```bash
# Lister les buckets
docker exec $(docker ps -qf "name=minio") mc ls local/

# Verifier les checkpoints Spark
# http://localhost:9001 -> bucket spotify-checkpoints
```

### Kafka (Phase 2)

```bash
# Logs d'un broker
docker compose -f docker-compose.yml -f docker-compose.kafka.yml logs kafka-1 | grep -i error

# Topics disponibles
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list
```

---

## Procedures de reset

### Airflow — DAG bloque en running

```bash
# Via l'interface : cliquer sur la tache -> Clear
# Via CLI :
docker exec $(docker ps -qf "name=airflow-scheduler") \
  airflow tasks clear <dag_id> -t <task_id> --yes
```

### MinIO — bucket introuvable

```bash
docker compose restart minio-init
```

### Spark — OutOfMemoryError

Reduire la memoire dans `docker-compose.kafka.yml` :
```yaml
SPARK_WORKER_MEMORY: 1G
```

```bash
docker compose -f docker-compose.yml -f docker-compose.kafka.yml \
  restart spark-worker-1 spark-worker-2
```

### Docker — no space left on device

```bash
docker system prune -af
docker compose up -d
```

---

## Workflow Git

```bash
# Synchroniser avec la branche du groupe
git pull origin groupe-d/main

# Convention de commits
feat(dag): description
fix(spark): description
docs(readme): description
test(unit): description

# En cas de non-fast-forward
git pull origin groupe-d/main --rebase
git push origin groupe-d/main
```

---

## Resultats des tests Phase 1

| Suite                  | Passed | Skipped | Failed | Plateforme |
|------------------------|--------|---------|--------|------------|
| tests/unit/            | 4      | 14      | 0      | Windows    |
| tests/structure/       | N/A    | N/A     | N/A    | Linux only |
