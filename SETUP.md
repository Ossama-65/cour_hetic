# Issue #1 — Setup Docker Compose

## Status
COMPLETED — 01 juin 2026

## Ce qui a été fait

Mise en place de l'environnement Docker complet pour la stack SPOTIFY.
Clonage du repo depuis github.com/courshetic/cours_hetic, configuration
du .env depuis .env.example, et démarrage de tous les services via
docker compose up -d.

## Services démarrés

- PostgreSQL 15 — port 5432
- Redis 7 — port 6379
- MinIO — port 9000 (API) et 9001 (UI)
- Airflow 2.9.1 (webserver, scheduler, worker, triggerer) — port 8080

## Buckets MinIO créés automatiquement

- spotify-parquet
- spotify-checkpoints
- spotify-audio
- labels-raw

## Difficultés rencontrées

### 1. Confusion entre deux dossiers de projet

Au départ j'avais créé un dossier spotify-m1 dans Downloads/Projects
avec un docker-compose.yml écrit manuellement. Quand j'ai relancé
docker compose après l'erreur de port, je me suis retrouvé dans le
mauvais dossier et Docker tentait de démarrer le mauvais projet.
L'erreur était exit 127 sur airflow-init (commande introuvable car
la config était incorrecte).

Resolution : identifier le bon dossier via le chemin dans le message
d'erreur et toujours vérifier avec pwd avant de lancer une commande.

### 2. Port 8080 déjà occupé

Au premier lancement, le webserver Airflow n'a pas pu démarrer :
"Bind for 0.0.0.0:8080 failed: port is already allocated"
Un autre processus occupait déjà ce port sur la machine.

Resolution : identifier le processus avec lsof -i :8080, le stopper,
puis relancer docker compose down && docker compose up -d.

### 3. Port 6379 déjà occupé (même problème sur Redis)

Même erreur que pour le port 8080 mais cette fois sur Redis.
"Bind for 0.0.0.0:6379 failed: port is already allocated"

Resolution : même approche, lsof -i :6379 pour identifier et libérer
le port avant de relancer.

### 4. Warnings persistants au démarrage

Plusieurs warnings apparaissaient dans les logs :
- auth_backends FutureWarning sur tous les conteneurs Airflow
- db init DeprecationWarning (devrait utiliser db migrate)
- flask_limiter UserWarning (pas de storage backend configuré)
- MinIO : credentials par défaut détectés

Ces warnings sont non-bloquants en environnement de développement local.
Ils disparaitront en Phase 2 avec la mise a jour de la config Airflow
et le passage a une configuration plus proche de la production.

### 5. minio-init et airflow-init marqués Exited(0)

Au premier regard cela semblait être une erreur. Ces deux conteneurs
sont en réalité éphémères : ils font leur travail (créer les buckets,
initialiser la base Airflow, créer le user admin) puis s'arrêtent
normalement avec le code 0. Pas une erreur.

## Accès aux interfaces

- Airflow UI : http://localhost:8080 (admin / admin)
- MinIO UI : http://localhost:9001 (minioadmin / minioadmin)

## Commandes utiles retenues

```bash
docker compose up -d          # démarrer la stack en arrière-plan
docker compose down           # tout arrêter et supprimer les conteneurs
docker compose ps             # voir l'état de tous les services
docker compose logs <service> -f   # suivre les logs d'un service
lsof -i :PORT                 # identifier ce qui occupe un port
docker compose exec postgres psql -U airflow -c '\l'   # vérifier PostgreSQL
docker compose exec redis redis-cli ping               # vérifier Redis
```

## Milestone

Issue #1 fermée. Stack Phase 1 opérationnelle.
Prochaine étape : Issue #2 — Schéma PostgreSQL complet.
