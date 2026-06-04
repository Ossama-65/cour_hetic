"""
SPOTIFY — Simulateur P2P
========================

Ce simulateur génère des événements réalistes d'écoute et de réseau P2P,
puis les pousse dans Redis DB 1 avec LPUSH.

Point important :
Les track_id utilisés dans les événements d'écoute sont récupérés depuis PostgreSQL.
Ils correspondent donc au catalogue réellement chargé dans la table tracks.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime, timedelta
from typing import Any

import psycopg2
import redis


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("p2p_simulator")


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "1"))

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "spotify")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")

LISTENING_EVENTS_KEY = "listening_events"
P2P_NETWORK_EVENTS_KEY = "p2p_network_events"

DEVICE_TYPES = ["mobile", "desktop", "smart_speaker", "web", "tv"]
GEO_COUNTRIES = ["FR", "DE", "US", "GB", "ES", "IT", "BR", "JP", "KR", "AU"]
EVENT_SOURCES = ["p2p", "p2p", "p2p", "direct", "cache"]

SAMPLE_USERS = [str(uuid.uuid4()) for _ in range(200)]


class P2PSimulator:
    def __init__(
        self,
        n_peers: int = 10,
        events_per_second: float = 5.0,
        mode: str = "normal",
        max_events: int | None = 100,
        include_p2p: bool = True,
        track_limit: int = 500,
    ) -> None:
        self.n_peers = n_peers
        self.events_per_second = events_per_second
        self.mode = mode
        self.max_events = max_events
        self.include_p2p = include_p2p
        self.track_limit = track_limit

        self.running = True
        self.event_count = 0
        self.listening_count = 0
        self.p2p_count = 0

        self.redis = self._get_redis_client()
        self.tracks = self._fetch_tracks_from_postgres(limit=track_limit)
        self.active_peers = [str(uuid.uuid4()) for _ in range(n_peers)]

        if not self.tracks:
            raise RuntimeError(
                "Aucun track récupéré depuis PostgreSQL. "
                "Vérifie que Docker est lancé et que le catalogue est chargé."
            )

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        logger.info(
            "Simulateur démarré | mode=%s | peers=%s | rate=%s evt/s | events=%s | "
            "redis=%s:%s db=%s | tracks=%s",
            mode,
            n_peers,
            events_per_second,
            max_events if max_events is not None else "continuous",
            REDIS_HOST,
            REDIS_PORT,
            REDIS_DB,
            len(self.tracks),
        )

    def _get_redis_client(self) -> redis.Redis:
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True,
        )
        client.ping()
        return client

    def _fetch_tracks_from_postgres(self, limit: int = 500) -> list[dict[str, Any]]:
        query = """
            SELECT id::text, title, duration_ms
            FROM tracks
            WHERE duration_ms IS NOT NULL
              AND duration_ms > 0
            ORDER BY random()
            LIMIT %s
        """

        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )

        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (limit,))
                rows = cursor.fetchall()
        finally:
            conn.close()

        tracks = [
            {
                "id": row[0],
                "title": row[1],
                "duration_ms": int(row[2]),
            }
            for row in rows
        ]

        logger.info("Tracks récupérées depuis PostgreSQL : %s", len(tracks))
        return tracks

    def run(self) -> None:
        interval = 1.0 / self.events_per_second if self.events_per_second > 0 else 0

        while self.running:
            if self.max_events is not None and self.event_count >= self.max_events:
                break

            try:
                generate_listening = True

                if self.include_p2p:
                    generate_listening = random.random() < 0.8

                if generate_listening:
                    event = self._generate_listening_event()
                    self._publish_event(LISTENING_EVENTS_KEY, event)
                    self.listening_count += 1
                else:
                    event = self._generate_p2p_network_event()
                    self._publish_event(P2P_NETWORK_EVENTS_KEY, event)
                    self.p2p_count += 1

                self.event_count += 1

                if self.event_count % 20 == 0:
                    self._log_progress()

                time.sleep(interval)

            except Exception as exc:
                logger.exception("Erreur lors de la génération d'événement : %s", exc)
                time.sleep(1)

        self._log_progress(final=True)

    def _generate_listening_event(self) -> dict[str, Any]:
        track = random.choice(self.tracks)
        max_duration = max(int(track["duration_ms"]), 30_000)
        duration_ms = random.randint(30_000, max_duration)

        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": random.choice(SAMPLE_USERS),
            "track_id": track["id"],
            "source_peer": random.choice(self.active_peers),
            "timestamp": datetime.utcnow().isoformat(),
            "duration_ms": duration_ms,
            "device_type": random.choice(DEVICE_TYPES),
            "geo_country": random.choice(GEO_COUNTRIES),
            "completed": duration_ms >= 30_000,
            "event_source": random.choice(EVENT_SOURCES),
        }

        if self.mode == "fraud" and random.random() < 0.3:
            event["duration_ms"] = random.randint(100, 4_999)
            event["completed"] = False

        if self.mode == "late_events" and random.random() < 0.4:
            delay_minutes = random.randint(5, 120)
            event["timestamp"] = (datetime.utcnow() - timedelta(minutes=delay_minutes)).isoformat()

        if self.mode == "chaos" and random.random() < 0.1:
            event.pop(random.choice(["track_id", "duration_ms", "timestamp"]), None)

        return event

    def _generate_p2p_network_event(self) -> dict[str, Any]:
        event_type = random.choice(
            ["peer_connect", "peer_disconnect", "chunk_transfer", "cache_hit", "cache_miss"]
        )

        peer_id = random.choice(self.active_peers)
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "peer_id": peer_id,
            "timestamp": datetime.utcnow().isoformat(),
        }

        if event_type == "peer_connect":
            event.update(
                {
                    "status": "connected",
                    "connected_peers": random.randint(1, self.n_peers),
                }
            )

        elif event_type == "peer_disconnect":
            event.update(
                {
                    "status": "disconnected",
                    "reason": random.choice(["timeout", "user_left", "network_error"]),
                }
            )

        elif event_type == "chunk_transfer":
            event.update(
                {
                    "source_peer": peer_id,
                    "target_peer": random.choice(self.active_peers),
                    "track_id": random.choice(self.tracks)["id"],
                    "chunk_id": str(uuid.uuid4()),
                    "latency_ms": random.randint(10, 500),
                    "bandwidth_kbps": random.randint(128, 5_000),
                    "success": random.random() > 0.1,
                }
            )

        elif event_type == "cache_hit":
            event.update(
                {
                    "track_id": random.choice(self.tracks)["id"],
                    "cache_status": "hit",
                    "latency_ms": random.randint(1, 50),
                }
            )

        elif event_type == "cache_miss":
            event.update(
                {
                    "track_id": random.choice(self.tracks)["id"],
                    "cache_status": "miss",
                    "fallback": "p2p_download",
                }
            )

        return event

    def _publish_event(self, redis_key: str, event: dict[str, Any]) -> None:
        payload = json.dumps(event, ensure_ascii=False)
        self.redis.lpush(redis_key, payload)

    def _log_progress(self, final: bool = False) -> None:
        prefix = "Bilan final" if final else "Progression"
        logger.info(
            "%s | total=%s | listening=%s | p2p=%s",
            prefix,
            self.event_count,
            self.listening_count,
            self.p2p_count,
        )

    def _shutdown(self, signum, frame) -> None:
        logger.info("Arrêt demandé signal=%s", signum)
        self.running = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SPOTIFY P2P Simulator")
    parser.add_argument("--peers", type=int, default=10, help="Nombre de peers simulés")
    parser.add_argument("--rate", type=float, default=5.0, help="Événements par seconde")
    parser.add_argument("--events", type=int, default=100, help="Nombre total d'événements à générer")
    parser.add_argument("--continuous", action="store_true", help="Mode continu sans limite d'événements")
    parser.add_argument("--no-p2p", action="store_true", help="Ne générer que des listening_events")
    parser.add_argument("--track-limit", type=int, default=500, help="Nombre de tracks à charger depuis PostgreSQL")
    parser.add_argument(
        "--mode",
        type=str,
        default="normal",
        choices=["normal", "fraud", "late_events", "chaos"],
        help="Mode de simulation",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    simulator = P2PSimulator(
        n_peers=args.peers,
        events_per_second=args.rate,
        mode=args.mode,
        max_events=None if args.continuous else args.events,
        include_p2p=not args.no_p2p,
        track_limit=args.track_limit,
    )
    simulator.run()


if __name__ == "__main__":
    main()
