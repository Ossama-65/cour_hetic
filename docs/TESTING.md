# TESTING — Groupe KFK — Plateforme Spotify

## Vue d'ensemble

Ce document decrit la strategie de tests du projet Spotify Data Platform.

---

## Lancer les tests

### Tous les tests
pytest tests/ -v --tb=short

### Tests unitaires uniquement
pytest tests/unit/ -v

### Tests de structure uniquement
pytest tests/structure/ -v

### Tests d integration uniquement
pytest tests/integration/ -v

---

## Structure des tests

tests/
|-- unit/
|   |-- test_transformations.py  <- tests des fonctions de transformation
|-- integration/
|   |-- (a completer)
|-- structure/
|   |-- test_dag_structure.py    <- tests de structure des DAGs

---

## Tests unitaires

### TestDataGenerator
Teste le generateur de donnees faker.

| Test | Description | Statut |
|------|-------------|--------|
| test_generate_catalog_structure | Verifie la structure du catalogue genere | OK |
| test_generated_track_has_required_fields | Verifie les champs requis des tracks | OK |
| test_generated_artist_has_label | Verifie l association artiste-label | OK |
| test_track_ids_are_unique | Verifie l unicite des IDs | OK |

### TestNormalizeArtistName
Teste la normalisation des noms d artistes.

| Test | Description | Statut |
|------|-------------|--------|
| test_strips_whitespace | Supprime les espaces en debut et fin | OK |
| test_title_case | Met en majuscule la premiere lettre | OK |
| test_handles_none | Gere les valeurs None | OK |
| test_preserves_special_chars | Gere les caracteres speciaux | OK |

### TestValidateTrackSchema
Teste la validation du schema des tracks.

| Test | Description | Statut |
|------|-------------|--------|
| test_valid_track_passes | Un track valide ne retourne pas d erreurs | OK |
| test_missing_title_fails | Un track sans titre retourne une erreur | OK |
| test_negative_duration_fails | Une duree negative retourne une erreur | OK |
| test_too_long_duration_fails | Une duree trop longue retourne une erreur | OK |

### TestListeningEventValidation
Teste la validation des evenements d ecoute.

| Test | Description | Statut |
|------|-------------|--------|
| test_valid_event_passes | Un evenement valide retourne True | OK |
| test_missing_user_id_fails | Un evenement sans user_id retourne False | OK |
| test_future_timestamp_fails | Un timestamp futur retourne False | OK |
| test_bot_pattern_detected | Une duree trop courte retourne False | OK |

### TestDeduplication
Teste la deduplication des artistes.

| Test | Description | Statut |
|------|-------------|--------|
| test_removes_duplicate_artists_same_label | Supprime les doublons meme label | OK |
| test_keeps_different_labels | Garde les artistes avec labels differents | OK |

---

## Resultats des tests

### Jour 1 — 01/06/2026
- 4 tests passes (TestDataGenerator)
- 0 tests echoues

### Jour 2 — 02/06/2026
- Tests unitaires complets implementes
- Fonctions de transformation creees
- 0 tests echoues

---

## Conventions

- Un test = une assertion claire
- Les fixtures sont dans conftest.py ou en haut du fichier
- Les tests ne dependent pas de Docker ni de PostgreSQL
- Les tests unitaires tournent sans infrastructure