"""
Handler P2P cross-group (#23)
Consomme les requetes cross-group entrantes depuis `p2p_cross_requests`
et repond avec les metadonnees du track si disponible localement.
Log format: [CROSS-GROUP] Groupe-X -> Groupe-D : track_id=... latency=XXms OK/NOT_FOUND
"""
import json
import logging
import time
import os
import uuid
from datetime import datetime

import psycopg2
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] cross_group — %(message)s"
)
logger = logging.getLogger("cross_group_handler")

KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
REQUEST_TOPIC     = "p2p_cross_requests"
RESPONSE_TOPIC    = "p2p_cross_responses"
SOURCE_GROUP      = "groupe-d"
CONSUMER_GROUP_ID = f"{SOURCE_GROUP}-cross-handler"

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   "spotify",
    "user":     "airflow",
    "password": "airflow",
}


def get_track_local(track_id: str) -> dict | None:
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            SELECT t.id, t.title, t.artist_id, a.name, t.duration_ms, t.genre
            FROM tracks t
            JOIN artists a ON a.id = t.artist_id
            WHERE t.id = %s
        """, (track_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return {
                "track_id":    str(row[0]),
                "title":       row[1],
                "artist_id":   str(row[2]),
                "artist_name": row[3],
                "duration_ms": row[4],
                "genre":       row[5],
                "source_group": SOURCE_GROUP,
            }
        return None
    except Exception as e:
        logger.error(f"Erreur DB lookup track {track_id}: {e}")
        return None


def handle_incoming_requests():
    consumer = KafkaConsumer(
        REQUEST_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="latest",
        consumer_timeout_ms=60000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    logger.info(f"Handler cross-group démarré — écoute sur {REQUEST_TOPIC}")

    for msg in consumer:
        request = msg.value
        t_start = time.time()

        # ignorer nos propres requetes
        if request.get("from_group") == SOURCE_GROUP:
            continue

        # ignorer les requetes expirées
        emitted_at = request.get("emitted_at", "")
        try:
            age = time.time() - datetime.fromisoformat(emitted_at.replace("Z","")).timestamp()
            if age > request.get("ttl_seconds", 30):
                logger.warning(f"Requete expirée ignorée (age={age:.0f}s) de {request.get('from_group')}")
                continue
        except Exception:
            pass

        track_id   = request.get("track_id")
        from_group = request.get("from_group", "unknown")
        request_id = request.get("request_id", str(uuid.uuid4()))

        track = get_track_local(track_id)
        latency_ms = int((time.time() - t_start) * 1000)
        status = "OK" if track else "NOT_FOUND"

        response = {
            "request_id":   request_id,
            "from_group":   SOURCE_GROUP,
            "to_group":     from_group,
            "track_id":     track_id,
            "status":       status,
            "track":        track,
            "latency_ms":   latency_ms,
            "responded_at": datetime.utcnow().isoformat() + "Z",
        }
        producer.send(RESPONSE_TOPIC, value=response)

        logger.info(
            f"[CROSS-GROUP] {from_group} -> {SOURCE_GROUP} : "
            f"track_id={track_id} latency={latency_ms}ms {status}"
        )

    consumer.close()
    producer.flush()
    producer.close()


def send_cross_group_request(track_id: str, to_group: str = None):
    """Publie une requete cross-group quand un track est introuvable localement."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    request = {
        "request_id":  str(uuid.uuid4()),
        "from_group":  SOURCE_GROUP,
        "to_group":    to_group or "broadcast",
        "track_id":    track_id,
        "user_id":     str(uuid.uuid4()),
        "timestamp":   datetime.utcnow().isoformat() + "Z",
        "ttl_seconds": 30,
        "emitted_at":  datetime.utcnow().isoformat() + "Z",
        "schema_version": "1.0",
    }
    producer.send(REQUEST_TOPIC, value=request)
    producer.flush()
    producer.close()
    logger.info(f"[CROSS-GROUP] {SOURCE_GROUP} -> {to_group or 'broadcast'} : track_id={track_id} SENT")


if __name__ == "__main__":
    handle_incoming_requests()
