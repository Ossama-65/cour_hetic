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

TODO Phase 2 :  Activer _publish_to_kafka() et le mode fraude
"""

import argparse
import json
import logging
import random
import signal
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional

import redis

# Phase 2 — décommenter quand Kafka est prêt
# from confluent_kafka import Producer

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
EVENT_SOURCES = ["p2p", "p2p", "p2p", "direct", "cache"]  # pondéré : 60% P2P

POSTGRES_DSN = "host=localhost port=5432 dbname=spotify user=spotify password=spotify"


# ─────────────────────────────────────────────────────────────
# DONNÉES SIMULÉES (fallback si PostgreSQL indisponible)
# ─────────────────────────────────────────────────────────────

SAMPLE_TRACKS = [
    {"id": str(uuid.uuid4()), "title": f"Track {i}", "duration_ms": random.randint(120_000, 300_000)}
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
        self.n_peers = n_peers
        self.events_per_second = events_per_second
        self.mode = mode
        self.running = True
        self.event_count = 0

        # Connexion Redis — vérifiée au démarrage
        self.redis = redis.from_url(REDIS_URL, decode_responses=True)
        self._check_redis()

        # Phase 2 — Kafka producer
        # self.kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

        # Peers actifs simulés
        self.active_peers = [str(uuid.uuid4()) for _ in range(n_peers)]

        # Catalogue : essaie PostgreSQL, fallback sur SAMPLE_TRACKS
        self.tracks = self._load_catalog()
        self.users  = SAMPLE_USERS

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT,  self._shutdown)

        logger.info(
            "Simulateur démarré | mode=%s | peers=%d | rate=%.1f evt/s | tracks=%d",
            mode, n_peers, events_per_second, len(self.tracks)
        )

    def _check_redis(self):
        """Vérifie la connexion Redis avant de démarrer la boucle principale."""
        try:
            self.redis.ping()
            logger.info("Redis connecté : %s", REDIS_URL)
        except redis.exceptions.ConnectionError as exc:
            logger.error("Redis inaccessible (%s) — arrêt du simulateur", exc)
            raise SystemExit(1) from exc

    def _load_catalog(self) -> list:
        """
        Charge les tracks depuis PostgreSQL.
        Retourne SAMPLE_TRACKS si PostgreSQL est indisponible.
        """
        try:
            import psycopg2
            conn = psycopg2.connect(POSTGRES_DSN)
            cur  = conn.cursor()
            cur.execute("SELECT id::text, title, duration_ms FROM tracks LIMIT 500")
            rows = cur.fetchall()
            cur.close()
            conn.close()
            if rows:
                logger.info("Catalogue chargé depuis PostgreSQL : %d tracks", len(rows))
                return [{"id": r[0], "title": r[1], "duration_ms": r[2]} for r in rows]
        except Exception as exc:
            logger.warning("PostgreSQL indisponible (%s) — utilisation du catalogue de test", exc)
        return SAMPLE_TRACKS

    def run(self):
        """Boucle principale : génère et publie des événements en continu."""
        interval = 1.0 / self.events_per_second

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
                    logger.info("Événements publiés : %d", self.event_count)

                time.sleep(interval)

            except Exception as exc:
                logger.error("Erreur lors de la génération d'événement : %s", exc)
                time.sleep(1)

    # ── Génération d'événements ──────────────────────────────

    def _generate_listening_event(self) -> dict:
        """
        Génère un événement d'écoute complet.

        Champs :
            event_id, user_id, track_id, source_peer, timestamp,
            duration_ms, device_type, geo_country, completed, event_source
        """
        track       = random.choice(self.tracks)
        duration_ms = random.randint(30_000, track["duration_ms"])

        event = {
            "event_id":     str(uuid.uuid4()),
            "user_id":      random.choice(self.users),
            "track_id":     track["id"],
            "source_peer":  random.choice(self.active_peers),
            "timestamp":    datetime.utcnow().isoformat() + "Z",
            "duration_ms":  duration_ms,
            "device_type":  random.choice(DEVICE_TYPES),
            "geo_country":  random.choice(GEO_COUNTRIES),
            "completed":    duration_ms > 30_000,
            "event_source": random.choice(EVENT_SOURCES),
        }

        if self.mode == "fraud" and random.random() < 0.3:
            event["duration_ms"] = random.randint(100, 4_999)
            event["completed"]   = False

        if self.mode == "late_events" and random.random() < 0.4:
            delay_minutes    = random.randint(5, 30)
            ts               = datetime.utcnow() - timedelta(minutes=delay_minutes)
            event["timestamp"] = ts.isoformat() + "Z"

        return event

    def _generate_p2p_network_event(self) -> dict:
        """
        Génère un événement réseau P2P parmi les 5 types :
            peer_connect, peer_disconnect, chunk_transfer, cache_hit, cache_miss
        """
        event_type = random.choice([
            "peer_connect", "peer_disconnect",
            "chunk_transfer", "cache_hit", "cache_miss",
        ])
        peer_id = random.choice(self.active_peers)
        track   = random.choice(self.tracks)

        event = {
            "event_id":   str(uuid.uuid4()),
            "event_type": event_type,
            "peer_id":    peer_id,
            "timestamp":  datetime.utcnow().isoformat() + "Z",
        }

        if event_type == "peer_connect":
            event["geo_country"] = random.choice(GEO_COUNTRIES)
            event["device_type"] = random.choice(DEVICE_TYPES)

        elif event_type == "peer_disconnect":
            event["session_duration_s"] = random.randint(60, 3_600)

        elif event_type == "chunk_transfer":
            event["source_peer"]          = peer_id
            event["target_peer"]          = random.choice(self.active_peers)
            event["track_id"]             = track["id"]
            event["chunk_size_kb"]        = random.randint(64, 512)
            event["transfer_duration_ms"] = random.randint(50, 2_000)
            event["success"]              = random.random() > 0.05  # 95% réussite

        elif event_type == "cache_hit":
            event["track_id"]          = track["id"]
            event["cache_size_tracks"] = random.randint(10, 100)

        elif event_type == "cache_miss":
            event["track_id"]      = track["id"]
            event["fallback_peer"] = random.choice(self.active_peers)

        return event

    # ── Publication ──────────────────────────────────────────

    def _publish_event(self, topic_key: str, event: dict):
        """Publie un événement dans Redis et (Phase 2) dans Kafka."""
        payload = json.dumps(event)
        channel = TOPICS[topic_key]

        self._publish_to_redis(channel, payload)
        # Phase 2 — décommenter
        # self._publish_to_kafka(channel, event.get("user_id", ""), payload)

    def _publish_to_redis(self, channel: str, payload: str):
        """Publie payload dans le channel Redis via pub/sub."""
        try:
            self.redis.publish(channel, payload)
        except redis.exceptions.ConnectionError as exc:
            logger.error("Redis indisponible, événement ignoré : %s", exc)
        except Exception as exc:
            logger.error("Erreur Redis inattendue : %s", exc)

    # def _publish_to_kafka(self, topic: str, key: str, payload: str):
    #     """
    #     TODO Phase 2 : publier payload dans le topic Kafka.
    #     - key     : utilisé pour le partitionnement (user_id ou peer_id)
    #     - acks    : 'all' pour la durabilité
    #     - Gérer le callback de confirmation (delivery_report)
    #     """
    #     raise NotImplementedError("TODO Phase 2 : implémenter _publish_to_kafka()")

    def _shutdown(self, signum, frame):
        logger.info(
            "Arrêt du simulateur (signal %d) — %d événements publiés",
            signum, self.event_count
        )
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
