# RUNBOOK — Incidents & procédures

## Incidents rencontrés pendant le projet

### Port 8080 occupé au démarrage
Symptôme : "Bind for 0.0.0.0:8080 failed: port is already allocated"
Cause : un autre processus occupait le port sur la machine hôte
Résolution : lsof -i :8080 pour identifier, kill du processus, puis docker compose down && docker compose up -d

### Port 6379 occupé (Redis)
Même problème que le 8080 mais sur Redis.
Résolution : lsof -i :6379, kill, relance.

### Mauvais dossier de projet
Symptôme : airflow-init exit 127, docker-compose.yml incorrect
Cause : lancement depuis ~/Downloads/Projects/spotify-m1 au lieu de ~/cours_hetic
Résolution : vérifier pwd avant toute commande docker compose

### scikit-learn manquant dans le conteneur
Symptôme : ModuleNotFoundError: No module named 'sklearn' dans recommendation_pipeline
Cause : _PIP_ADDITIONAL_REQUIREMENTS mal indenté dans docker-compose.yml
Résolution : corriger l'indentation YAML, docker compose down && up -d

### Conflit Git sur src/transformations/
Symptôme : CONFLICT (add/add) lors du rebase
Cause : collègue et moi avons créé les mêmes fichiers en parallèle
Résolution : git checkout --ours pour garder notre version, git rebase --continue

### dlq_reprocessing_pipeline en échec
Symptôme : NotImplementedError sur fetch_pending_dlq
Cause : fonction non implémentée dans le fichier de base du prof
Résolution : implémenter les 3 fonctions fetch_pending_dlq, reprocess_events, update_dlq_status

## Commandes utiles

Relancer la stack :
docker compose down && docker compose up -d

Voir les logs d'un service :
docker compose logs <service> -f

Vérifier les DAGs :
docker compose exec airflow-scheduler airflow dags list

Lancer les tests :
docker compose exec airflow-worker bash -c "cd /opt/airflow && export PYTHONPATH=/opt/airflow && /home/airflow/.local/bin/pytest tests/unit/ -v"

Lancer le simulateur P2P :
python3 -m src.p2p_simulator.simulator --peers 10 --rate 3
