"""
SPOTIFY — Simulateur P2P (Version Catalogue CSV Automatique)
============================================================
Ce simulateur extrait les vrais track_ids depuis le fichier CSV du projet
pour garantir que le DAG Airflow "enrichir_événements" passe au vert.
"""

import argparse
import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime

import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("p2p_simulator")


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

REDIS_URL = "redis://localhost:6379/1"

TOPICS = {
    "listening":   "listening_events",
    "p2p_network": "p2p_network_events",
}

DEVICE_TYPES = ["mobile", "desktop", "smart_speaker", "web", "tv"]
GEO_COUNTRIES = ["FR", "DE", "US", "GB", "ES", "IT", "BR", "JP", "KR", "AU"]
EVENT_SOURCES = ["p2p", "p2p", "p2p", "direct", "cache"]


# ─────────────────────────────────────────────────────────────
# SIMULATEUR PRINCIPAL
# ─────────────────────────────────────────────────────────────

class P2PSimulator:
    """
    Simulateur de flux Spotify lisant un CSV local pour obtenir de vrais IDs.
    """

    def __init__(self, n_peers: int = 10, events_per_second: float = 5.0, mode: str = "normal"):
        self.n_peers = n_peers
        self.events_per_second = events_per_second
        self.mode = mode
        self.running = True
        self.event_count = 0

        # Connexion Redis
        self.redis = redis.from_url(REDIS_URL, decode_responses=True)

        # Chargement automatique des vrais morceaux depuis les fichiers possibles
        self.tracks = self._load_tracks_from_csv_automatically()

        self.sample_users = [str(uuid.uuid4()) for _ in range(50)]
        self.active_peers = [str(uuid.uuid4()) for _ in range(n_peers)]

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        logger.info(f"Simulateur prêt | mode={mode} | {len(self.tracks)} morceaux chargés.")

    def _load_tracks_from_csv_automatically(self) -> list:
        """Cherche et lit le fichier CSV du projet pour extraire de vrais UUIDs."""
        # Liste des chemins et noms de fichiers probables dans ton projet
        possible_files = [
            "tracks.csv", 
            "data/tracks.csv", 
            "src/data/tracks.csv",
            "dataset.csv",
            "data/dataset.csv"
        ]
        
        for file_path in possible_files:
            if os.path.exists(file_path):
                logger.info(f"Fichier catalogue trouve : {file_path}. Extraction des IDs...")
                try:
                    tracks_list = []
                    with open(file_path, "r", encoding="utf-8") as f:
                        lines = f.readlines()
                        # On saute l'en-tête et on prend les lignes
                        for line in lines[1:]:
                            parts = line.strip().split(",")
                            if parts and len(parts[0]) > 10:  # Détecter un ID/UUID valide
                                tracks_list.append({"id": parts[0].strip('"'), "duration_ms": 240000})
                    
                    if tracks_list:
                        return tracks_list
                except Exception as e:
                    logger.error(f"Erreur lecture {file_path}: {e}")
        
        # Solution de secours ultime (Hardcoded UUID d'un vrai morceau si aucun fichier n'est trouvé)
        logger.warning("Aucun fichier CSV trouve automatiquement. Injection d'un catalogue virtuel de secours.")
        return [{"id": "4zZuj8f3fXbC3AX7uC6WvK", "duration_ms": 210000} for _ in range(10)]

    def run(self):
        """Boucle principale du simulateur."""
        interval = 1.0 / self.events_per_second
        logger.info("Simulation en cours d'execution...")

        while self.running:
            try:
                if random.random() < 0.8:
                    event = self._generate_listening_event()
                    self._publish_event("listening", event)
                else:
                    event = self._generate_p2p_network_event()
                    self._publish_event("p2p_network", event)

                self.event_count += 1

                if self.event_count % 100 == 0:
                    logger.info(f"Evenements synchronises dans Redis : {self.event_count}")

                time.sleep(interval)

            except Exception as e:
                logger.error(f"Erreur boucle : {e}")
                time.sleep(1)

    # ── Génération d'événements ──────────────────────────────

    def _generate_listening_event(self) -> dict:
        track = random.choice(self.tracks)
        duration_ms = random.randint(30000, track["duration_ms"])
        
        event = {
            "event_id":     str(uuid.uuid4()),
            "user_id":      random.choice(self.sample_users),
            "track_id":     track["id"],  # Vrai ID extrait du projet !
            "source_peer":  random.choice(self.active_peers),
            "timestamp":    datetime.utcnow().isoformat() + "Z",
            "duration_ms":  duration_ms,
            "device_type":  random.choice(DEVICE_TYPES),
            "geo_country":  random.choice(GEO_COUNTRIES),
            "completed":    duration_ms > 30000,
            "event_source": random.choice(EVENT_SOURCES),
        }
        return event

    def _generate_p2p_network_event(self) -> dict:
        event_type = random.choice([
            "peer_connect", "peer_disconnect",
            "chunk_transfer", "cache_hit", "cache_miss"
        ])

        event = {
            "event_id":    str(uuid.uuid4()),
            "event_type":  event_type,
            "peer_id":     random.choice(self.active_peers),
            "timestamp":   datetime.utcnow().isoformat() + "Z",
            "target_peer": random.choice(self.active_peers),
            "track_id":    random.choice(self.tracks)["id"],
            "bytes":       random.randint(1024, 1048576),
        }
        return event

    # ── Publication ──────────────────────────────────────────

    def _publish_event(self, topic_key: str, event: dict):
        payload = json.dumps(event)
        channel = TOPICS[topic_key]
        self._publish_to_redis(channel, payload)

    def _publish_to_redis(self, channel: str, payload: str):
        try:
            self.redis.lpush(channel, payload)
        except Exception as e:
            logger.error(f"Erreur d'ecriture Redis : {e}")

    def _shutdown(self, signum, frame):
        logger.info("Arret demande...")
        self.running = False


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SPOTIFY P2P Simulator")
    parser.add_argument("--peers",  type=int,   default=10)
    parser.add_argument("--rate",   type=float, default=5.0)
    parser.add_argument("--mode",   type=str,   default="normal")
    args = parser.parse_args()

    simulator = P2PSimulator(
        n_peers=args.peers,
        events_per_second=args.rate,
        mode=args.mode,
    )
    simulator.run()


if __name__ == "__main__":
    main()