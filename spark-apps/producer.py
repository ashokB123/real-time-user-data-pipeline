"""
Kafka producer for the Random User pipeline.

Pulls fake user profiles from the Random User API (https://randomuser.me)
and pushes each one as a JSON message onto a Kafka topic, wrapped in an
envelope the Spark job (spark_job.py) knows how to parse:

    { "ingested_at": <ISO timestamp>, "source": "randomuser.me", "user": {...} }

Run standalone for a batch of N users, e.g.:
    python producer.py --count 20
"""

import argparse
import json
import logging
import os
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("random-user-producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "random-user-topic")
RANDOM_USER_API = "https://randomuser.me/api/"


def fetch_users(count: int) -> list:
    """Call the Random User API once for a batch of `count` users."""
    resp = requests.get(RANDOM_USER_API, params={"results": count}, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def send_users(producer: KafkaProducer, users: list) -> int:
    sent = 0
    for user in users:
        envelope = {
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "source": "randomuser.me",
            "user": user,
        }
        producer.send(KAFKA_TOPIC, value=envelope)
        sent += 1
    producer.flush()
    return sent


def run(count: int) -> None:
    log.info("Fetching %d user(s) from %s", count, RANDOM_USER_API)
    users = fetch_users(count)
    log.info("Fetched %d user(s).", len(users))

    producer = build_producer()
    try:
        sent = send_users(producer, users)
        log.info("Sent %d message(s) to Kafka topic '%s'.", sent, KAFKA_TOPIC)
    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produce fake user data onto Kafka.")
    parser.add_argument("--count", type=int, default=20, help="Number of users to fetch and send.")
    args = parser.parse_args()
    run(args.count)