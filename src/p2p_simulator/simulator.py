"""
SPOTIFY — Simulateur P2P
========================
Ce simulateur génère des événements réalistes d'écoute et de réseau P2P,
puis les pousse dans Redis DB 1 avec LPUSH.

Usage :
    python -m src.p2p_simulator.simulator --peers 10 --rate 5
    python -m src.p2p_simulator.simulator --mode late_events --peers 5
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


REDIS_URL = "redis://localhost:6379/1"

TOPICS = {
    "listening": "listening_events",
    "p2p_network": "p2p_network_events",
}

DEVICE_TYPES = ["mobile", "desktop", "smart_speaker", "web", "tv"]
GEO_COUNTRIES = ["FR", "DE", "US", "GB", "ES", "IT", "BR", "JP", "KR", "AU"]
EVENT_SOURCES = ["p2p", "p2p", "p2p", "direct", "cache"]


SAMPLE_TRACKS = [
    {
        "id": str(uuid.uuid4()),
        "title": f"Track {i}",
        "duration_ms": random.randint(120000, 300000),
    }
    for i in range(50)
]

SAMPLE_USERS = [str(uuid.uuid4()) for _ in range(200)]


class P2PSimulator:
    """
    Simulateur du réseau P2P Spotify.

    Génère deux types d'événements :
    - listening_events
    - p2p_network_events
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
        self.listening_count = 0
        self.p2p_count = 0

        self.redis = redis.from_url(REDIS_URL, decode_responses=True)
        self.active_peers = [str(uuid.uuid4()) for _ in range(n_peers)]

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        logger.info(
            f"Simulateur démarré | mode={mode} | peers={n_peers} | "
            f"rate={events_per_second} evt/s | redis={REDIS_URL}"
        )

    def run(self):
        interval = 1.0 / self.events_per_second

        while self.running:
            try:
                if random.random() < 0.8:
                    event = self._generate_listening_event()
                    self._publish_event("listening", event)
                    self.listening_count += 1
                else:
                    event = self._generate_p2p_network_event()
                    self._publish_event("p2p_network", event)
                    self.p2p_count += 1

                self.event_count += 1

                if self.event_count % 100 == 0:
                    logger.info(
                        f"Événements générés : {self.event_count} | "
                        f"listening={self.listening_count} | "
                        f"p2p={self.p2p_count}"
                    )

                time.sleep(interval)

            except Exception as e:
                logger.error(f"Erreur lors de la génération d'événement : {e}")
                time.sleep(1)

    def _generate_listening_event(self) -> dict:
        track = random.choice(SAMPLE_TRACKS)
        duration_ms = random.randint(30_000, track["duration_ms"])

        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": random.choice(SAMPLE_USERS),
            "track_id": track["id"],
            "source_peer": random.choice(self.active_peers),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "duration_ms": duration_ms,
            "device_type": random.choice(DEVICE_TYPES),
            "geo_country": random.choice(GEO_COUNTRIES),
            "completed": duration_ms >= 30_000,
            "event_source": random.choice(EVENT_SOURCES),
        }

        if self.mode == "fraud" and random.random() < 0.3:
            event["duration_ms"] = random.randint(100, 4999)
            event["completed"] = False

        if self.mode == "late_events" and random.random() < 0.4:
            delay_minutes = random.randint(5, 30)
            ts = datetime.utcnow() - timedelta(minutes=delay_minutes)
            event["timestamp"] = ts.isoformat() + "Z"

        return event

    def _generate_p2p_network_event(self) -> dict:
        event_type = random.choice([
            "peer_connect",
            "peer_disconnect",
            "chunk_transfer",
            "cache_hit",
            "cache_miss",
        ])

        peer_id = random.choice(self.active_peers)

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "peer_id": peer_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        if event_type == "peer_connect":
            event.update({
                "status": "connected",
                "connected_peers": random.randint(1, self.n_peers),
            })

        elif event_type == "peer_disconnect":
            event.update({
                "status": "disconnected",
                "reason": random.choice(["timeout", "user_left", "network_error"]),
            })

        elif event_type == "chunk_transfer":
            target_peer = random.choice(self.active_peers)
            event.update({
                "source_peer": peer_id,
                "target_peer": target_peer,
                "track_id": random.choice(SAMPLE_TRACKS)["id"],
                "chunk_id": str(uuid.uuid4()),
                "latency_ms": random.randint(10, 500),
                "bandwidth_kbps": random.randint(128, 5000),
                "success": random.random() > 0.1,
            })

        elif event_type == "cache_hit":
            event.update({
                "track_id": random.choice(SAMPLE_TRACKS)["id"],
                "cache_status": "hit",
                "latency_ms": random.randint(1, 50),
            })

        elif event_type == "cache_miss":
            event.update({
                "track_id": random.choice(SAMPLE_TRACKS)["id"],
                "cache_status": "miss",
                "fallback": "p2p_download",
            })

        return event

    def _publish_event(self, topic_key: str, event: dict):
        payload = json.dumps(event)
        redis_key = TOPICS[topic_key]
        self._publish_to_redis(redis_key, payload)

    def _publish_to_redis(self, redis_key: str, payload: str):
        try:
            self.redis.lpush(redis_key, payload)
        except redis.RedisError as e:
            logger.error(f"Redis indisponible, événement non envoyé sur {redis_key} : {e}")

    def _shutdown(self, signum, frame):
        logger.info(
            f"Arrêt du simulateur signal={signum} | total={self.event_count} | "
            f"listening={self.listening_count} | p2p={self.p2p_count}"
        )
        self.running = False


def main():
    parser = argparse.ArgumentParser(description="SPOTIFY P2P Simulator")
    parser.add_argument("--peers", type=int, default=10, help="Nombre de peers simulés")
    parser.add_argument("--rate", type=float, default=5.0, help="Événements par seconde")
    parser.add_argument(
        "--mode",
        type=str,
        default="normal",
        choices=["normal", "fraud", "late_events", "chaos"],
        help="Mode de simulation",
    )

    args = parser.parse_args()

    simulator = P2PSimulator(
        n_peers=args.peers,
        events_per_second=args.rate,
        mode=args.mode,
    )
    simulator.run()


if __name__ == "__main__":
    main()