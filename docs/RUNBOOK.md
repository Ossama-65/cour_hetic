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
| Redis         | localhost:6379/1            | base 1 pour les reco         |

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

**Cause** : le script `pytest.exe` est installe dans un repertoire absent du PATH Windows (`C:\Users\...\Python313\Scripts`).

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

### INC-006 — 14 tests skipped apres lancement des tests unitaires

**Symptome**

```
14 skipped
SKIPPED [1] tests\unit\test_transformations.py:86: TODO : implementer normalize_artist_name()
```

**Cause** : les fonctions `normalize_artist_name()`, `validate_track_schema()`, `is_valid_listening_event()` et `deduplicate_artists()` n'etaient pas implementees. Les tests etaient marques `@pytest.mark.skip`.

**Resolution**

Creer `src/transformations/catalog.py` et `src/transformations/events.py` avec les fonctions requises, puis decommenter les tests dans `tests/unit/test_transformations.py`.

**Resultat apres correction** : 18 passed, 0 skipped, 0 failed.

---

### INC-007 — AttributeError PlainXComArg dans load_to_postgres

**Symptome**

```
AttributeError: 'PlainXComArg' object has no attribute 'get'
File "/opt/airflow/dags/catalog_ingestion_pipeline.py", line 236, in load_to_postgres
    "errors_count": validated.get("errors_count", 0),
```

**Cause** : la variable `validated` utilisee dans `load_to_postgres` est un `XComArg` non resolu — elle appartient a une autre tache et n'est pas passee en parametre de la fonction.

**Resolution**

```bash
sed -i 's/validated.get("errors_count", 0)/0/' ~/spotify/dags/catalog_ingestion_pipeline.py
```

Puis relancer le DAG depuis Airflow.

**Fichiers concernes** : `dags/catalog_ingestion_pipeline.py` ligne 236

---

### INC-008 — recommendation_pipeline bloque le worker avec wait_for_aggregation

**Symptome**

`wait_for_aggregation` tourne en boucle toutes les minutes en `up_for_reschedule` depuis la veille, bloquant les autres DAGs.

**Cause** : `ExternalTaskSensor` attend un run du DAG `aggregation_pipeline` a une date anterieure qui n'existe pas.

**Resolution**

Pauser le DAG pour liberer le worker :
```bash
docker exec spotify-airflow-scheduler-1 airflow dags pause recommendation_pipeline
```

Forcer `wait_for_aggregation` a success via l'interface Airflow : cliquer sur la tache → Mark state as → Success.

Puis remettre le DAG actif :
```bash
docker exec spotify-airflow-scheduler-1 airflow dags unpause recommendation_pipeline
```

---

### INC-009 — ModuleNotFoundError sklearn dans compute_recommendations

**Symptome**

```
ModuleNotFoundError: No module named 'sklearn'
File "/opt/airflow/dags/recommendation_pipeline.py", line 116, in compute_recommendations
    from sklearn.metrics.pairwise import cosine_similarity
```

**Cause** : `scikit-learn` n'est pas installe dans le conteneur airflow-worker.

**Resolution**

```bash
docker exec -u airflow spotify-airflow-worker-1 python -m pip install scikit-learn
docker exec -u airflow spotify-airflow-scheduler-1 python -m pip install scikit-learn
```

Puis faire un **Clear task** sur `compute_recommendations` dans Airflow.

**Note** : cette installation est temporaire. En cas de recreation du conteneur, relancer les deux commandes.

---

### INC-010 — Recommandations Redis introuvables avec redis-cli keys 'reco:*'

**Symptome**

```bash
docker exec spotify-redis-1 redis-cli keys 'reco:*'
# retourne vide
```

**Cause** : les recommandations sont stockees sur la base Redis 1 (`redis://redis:6379/1`) et non la base 0 par defaut.

**Resolution**

```bash
docker exec spotify-redis-1 redis-cli -n 1 keys 'reco:*' | head -5
docker exec spotify-redis-1 redis-cli -n 1 get reco:<user_id>
```

---

## Verification de l'etat de la plateforme

### DAGs Airflow

```bash
docker exec $(docker ps -qf "name=airflow-scheduler") airflow dags list
docker exec $(docker ps -qf "name=airflow-scheduler") airflow dags list-runs -d catalog_ingestion_pipeline
```

### PostgreSQL

```bash
psql -U spotify spotify -c "SELECT count(*), state FROM pg_stat_activity GROUP BY state;"
psql -U spotify spotify -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state='idle';"
docker exec spotify-postgres-1 psql -U spotify spotify -c "SELECT COUNT(*) FROM artists;"
docker exec spotify-postgres-1 psql -U spotify spotify -c "SELECT COUNT(*) FROM tracks;"
docker exec spotify-postgres-1 psql -U spotify spotify -c "SELECT status, COUNT(*) FROM dead_letter_events GROUP BY status;"
docker exec spotify-postgres-1 psql -U spotify spotify -c "SELECT COUNT(*) - COUNT(DISTINCT id) AS doublons FROM listening_events;"
```

### Redis

```bash
# Base 1 — recommandations
docker exec spotify-redis-1 redis-cli -n 1 keys 'reco:*' | head -5
docker exec spotify-redis-1 redis-cli -n 1 get reco:<user_id>

# Phase 2 — Top 50 Global
docker exec spotify-redis-1 redis-cli get top50:global | python3 -m json.tool | head -30
```

### Kafka (Phase 2)

```bash
docker compose -f docker-compose.yml -f docker-compose.kafka.yml logs kafka-1 | grep -i error
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list
```

---

## Procedures de reset

### Airflow — DAG bloque en running

```bash
docker exec $(docker ps -qf "name=airflow-scheduler") \
  airflow tasks clear <dag_id> -t <task_id> --yes
```

### MinIO — bucket introuvable

```bash
docker compose restart minio-init
```

### Spark — OutOfMemoryError

Reduire dans `docker-compose.kafka.yml` :
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
git pull origin groupe-d/main

# Convention de commits
feat(#10): description
fix(#10): description
docs(#10): description
test(#10): description

# En cas de non-fast-forward
git pull origin groupe-d/main --rebase
git push origin groupe-d/main
```

---

## Resultats des tests Phase 1

| Suite                  | Passed | Skipped | Failed | Plateforme |
|------------------------|--------|---------|--------|------------|
| tests/unit/            | 18     | 0       | 0      | Windows    |
| tests/structure/       | N/A    | N/A     | N/A    | Linux only |

## Etat final Phase 1

| Critere                        | Valeur              | Statut |
|-------------------------------|---------------------|--------|
| DAGs actifs                   | 5/5                 | OK     |
| Artistes PostgreSQL            | 45                  | OK     |
| Tracks PostgreSQL              | 1437                | OK     |
| Dead letter events             | 382 pending         | OK     |
| Recommandations Redis (base 1) | 448 / 45 users      | OK     |

---

## Phase 2 — Incidents & Résolutions

### Incident P2-001 : DNS Kafka non résolu depuis Mac
**Symptôme** : `Failed to resolve 'kafka-1:9092'` dans le simulateur
**Cause** : Kafka annonce ses hostnames internes Docker mais le Mac ne les résout pas
**Fix** : `echo "127.0.0.1 kafka-1 kafka-2 kafka-3" >> /etc/hosts`
**Statut** : Résolu ✅

### Incident P2-002 : Duplicate key sur realtime_top_tracks
**Symptôme** : `ERROR: duplicate key value violates unique constraint`
**Cause** : Spark mode `append` rejoue les mêmes fenêtres à chaque micro-batch
**Fix** : Passage en `mode="overwrite"` dans write_to_postgres()
**Statut** : Résolu ✅

### Incident P2-003 : Checkpoints Spark corrompus
**Symptôme** : `FileNotFoundException: 1.delta does not exist`
**Cause** : Redémarrage Spark sans nettoyage checkpoint
**Fix** : `docker exec spark-master rm -rf /tmp/spark-checkpoints`
**Statut** : Résolu ✅

### Incident P2-004 : psycopg2 absent dans conteneur Spark
**Symptôme** : `ModuleNotFoundError: No module named 'psycopg2'`
**Cause** : Image apache/spark:3.5.0 ne contient pas psycopg2
**Fix** : Utiliser JDBC natif Spark plutôt que psycopg2
**Statut** : Résolu ✅

---

## Chaos Engineering — Résultats

### Scénario 1 : kafka-2 DOWN
- **Action** : `docker compose stop kafka-2`
- **Résultat** : Cluster KRaft reste opérationnel avec kafka-1 + kafka-3
- **Production** : Continue sans interruption (réplication factor=1)
- **Status** : ✅ PASS

### Scénario 2 : spark-master KILL
- **Action** : `docker compose kill spark-master` puis restart
- **Résultat** : Job Spark redémarre depuis le checkpoint
- **Données** : Aucune perte (exactly-once via checkpoint)
- **Status** : ✅ PASS

### Scénario 3 : postgres DOWN 2 minutes
- **Action** : `docker compose stop postgres` → 2 min → restart
- **Résultat** : Airflow retry automatique, Spark catch exception et continue
- **Données** : Aucune perte (events bufferisés dans Kafka)
- **Status** : ✅ PASS
