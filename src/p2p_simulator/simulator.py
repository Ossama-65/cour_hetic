"""
SPOTIFY — Simulateur P2P
========================
Ce simulateur génère des événements réalistes d'un réseau peer-to-peer
de streaming musical. Il publie dans Redis pub/sub (Phase 1) et dans
Kafka (Phase 2, après décommentage).

Usage :
    python -m src.p2p_simulator.simulator --peers 10 --rate 5
    python -m src.p2p_simulator.simulator --mode fraud --peers 5
    python -m src.p2p_simulator.simulator --mode late_events
"""

import argparse
import json
import logging
import random
import signal
import time
import uuid
from datetime import datetime, timedelta

import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("p2p_simulator")


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

REDIS_URL       = "redis://localhost:6379/1"
KAFKA_BOOTSTRAP = "kafka-1:9092"  # Phase 2

TOPICS = {
    "listening":   "listening_events",
    "p2p_network": "p2p_network_events",
}

DEVICE_TYPES  = ["mobile", "desktop", "smart_speaker", "web", "tv"]
GEO_COUNTRIES = ["FR", "DE", "US", "GB", "ES", "IT", "BR", "JP", "KR", "AU"]
EVENT_SOURCES = ["p2p", "p2p", "p2p", "direct", "cache"]  # 60% P2P


# ─────────────────────────────────────────────────────────────
# DONNÉES SIMULÉES
# ─────────────────────────────────────────────────────────────

SAMPLE_TRACKS = [
    {"id": str(uuid.uuid4()), "title": f"Track {i}", "duration_ms": random.randint(120000, 300000)}
    for i in range(50)
]

SAMPLE_USERS = [str(uuid.uuid4()) for _ in range(200)]
SAMPLE_PEERS = [str(uuid.uuid4()) for _ in range(20)]


# ─────────────────────────────────────────────────────────────
# SIMULATEUR PRINCIPAL
# ─────────────────────────────────────────────────────────────

class P2PSimulator:
    """
    Simulateur du réseau P2P SPOTIFY.

    Génère deux types d'événements :
    - listening_events   : un utilisateur écoute un morceau via un peer
    - p2p_network_events : connexion/déconnexion/transfert entre peers
    """

    def __init__(
        self,
        n_peers: int = 10,
        events_per_second: float = 5.0,
        mode: str = "normal",
    ):
        self.n_peers            = n_peers
        self.events_per_second  = events_per_second
        self.mode               = mode
        self.running            = True
        self.event_count        = 0

        # Connexion Redis
        self.redis = redis.from_url(REDIS_URL, decode_responses=True)

        # Peers actifs simulés
        self.active_peers = [str(uuid.uuid4()) for _ in range(n_peers)]

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT,  self._shutdown)

        logger.info(f"Simulateur démarré | mode={mode} | peers={n_peers} | rate={events_per_second} evt/s")

    def run(self):
        """Boucle principale : génère et publie des événements en continu."""
        interval = 1.0 / self.events_per_second

        while self.running:
            try:
                # 80% écoutes, 20% événements réseau P2P
                if random.random() < 0.8:
                    event = self._generate_listening_event()
                    self._publish_event("listening", event)
                else:
                    event = self._generate_p2p_network_event()
                    self._publish_event("p2p_network", event)

                self.event_count += 1

                if self.event_count % 100 == 0:
                    logger.info(f"Événements publiés : {self.event_count}")

                time.sleep(interval)

            except Exception as e:
                logger.error(f"Erreur lors de la génération d'événement : {e}")
                time.sleep(1)

    # ── Génération d'événements ──────────────────────────────

    def _generate_listening_event(self) -> dict:
        """
        Génère un événement d'écoute réaliste.
        """
        track       = random.choice(SAMPLE_TRACKS)
        duration_ms = random.randint(30000, track["duration_ms"])
        completed   = duration_ms > 30000

        event = {
            "event_id":     str(uuid.uuid4()),
            "user_id":      random.choice(SAMPLE_USERS),
            "track_id":     track["id"],
            "source_peer":  random.choice(self.active_peers),
            "timestamp":    datetime.utcnow().isoformat() + "Z",
            "duration_ms":  duration_ms,
            "device_type":  random.choice(DEVICE_TYPES),
            "geo_country":  random.choice(GEO_COUNTRIES),
            "completed":    completed,
            "event_source": random.choice(EVENT_SOURCES),
        }

        # Mode fraud (Phase 2)
        if self.mode == "fraud" and random.random() < 0.3:
            event["duration_ms"] = random.randint(100, 4999)
            event["completed"]   = False

        # Mode late_events (Phase 2)
        if self.mode == "late_events" and random.random() < 0.4:
            delay_minutes    = random.randint(5, 30)
            ts               = datetime.utcnow() - timedelta(minutes=delay_minutes)
            event["timestamp"] = ts.isoformat() + "Z"

        return event

    def _generate_p2p_network_event(self) -> dict:
        """
        Génère un événement réseau P2P.
        """
        event_type = random.choice([
            "peer_connect", "peer_disconnect",
            "chunk_transfer", "cache_hit", "cache_miss"
        ])

        event = {
            "event_id":   str(uuid.uuid4()),
            "event_type": event_type,
            "peer_id":    random.choice(self.active_peers),
            "timestamp":  datetime.utcnow().isoformat() + "Z",
        }

        # Ajouter des infos selon le type d'événement
        if event_type == "chunk_transfer":
            event["track_id"]      = random.choice(SAMPLE_TRACKS)["id"]
            event["chunk_size_kb"] = random.randint(64, 512)
            event["target_peer"]   = random.choice(self.active_peers)

        elif event_type in ("cache_hit", "cache_miss"):
            event["track_id"]    = random.choice(SAMPLE_TRACKS)["id"]
            event["latency_ms"]  = random.randint(5, 500)

        elif event_type == "peer_connect":
            event["geo_country"] = random.choice(GEO_COUNTRIES)
            event["device_type"] = random.choice(DEVICE_TYPES)

        return event

    # ── Publication ──────────────────────────────────────────

    def _publish_event(self, topic_key: str, event: dict):
        """Publie un événement dans Redis."""
        payload = json.dumps(event)
        channel = TOPICS[topic_key]
        self._publish_to_redis(channel, payload)

    def _publish_to_redis(self, channel: str, payload: str):
        """
        Publie le payload dans le channel Redis via pub/sub
        ET écrit dans une liste Redis pour le DAG batch.
        """
        try:
            # Pub/sub — temps réel
            self.redis.publish(channel, payload)
            # Liste — stockage pour le DAG (toutes les 5 min)
            self.redis.lpush(channel + "_list", payload)
        except Exception as e:
            logger.error(f"Erreur Redis — channel={channel} : {e}")

    def _shutdown(self, signum, frame):
        logger.info(f"Arrêt du simulateur (signal {signum}) — {self.event_count} événements publiés")
        self.running = False


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SPOTIFY P2P Simulator")
    parser.add_argument("--peers", type=int,   default=10,      help="Nombre de peers simulés")
    parser.add_argument("--rate",  type=float, default=5.0,     help="Événements par seconde")
    parser.add_argument("--mode",  type=str,   default="normal",
                        choices=["normal", "fraud", "late_events", "chaos"],
                        help="Mode de simulation")
    args = parser.parse_args()

    simulator = P2PSimulator(
        n_peers=args.peers,
        events_per_second=args.rate,
        mode=args.mode,
    )
    simulator.run()


if __name__ == "__main__":
    main()