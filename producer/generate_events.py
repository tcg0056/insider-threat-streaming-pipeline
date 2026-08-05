"""Generate synthetic identity and data-access telemetry for a Kafka topic."""
from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()
USERS = [f"user{i:03d}@example.com" for i in range(1, 51)]
DEPARTMENTS = ["Finance", "Engineering", "Legal", "Sales", "Operations"]
EVENT_TYPES = ["file_download", "file_upload", "login", "permission_change"]
COUNTRIES = ["US", "US", "US", "CA", "GB"]


def create_event(anomalous: bool = False) -> dict:
    now = datetime.now(timezone.utc)
    event_time = now - timedelta(seconds=random.randint(0, 120))
    event_type = random.choice(EVENT_TYPES)
    bytes_transferred = random.randint(1_000, 8_000_000)
    country = random.choice(COUNTRIES)
    privileged = random.random() < 0.08
    sensitive = random.choice(["public", "internal", "confidential", "restricted"])

    if anomalous:
        event_type = random.choice(["file_download", "file_upload", "permission_change"])
        bytes_transferred = random.randint(750_000_000, 5_000_000_000)
        country = random.choice(["KP", "RU", "CN"])
        privileged = True
        sensitive = "restricted"
        event_time = event_time.replace(hour=random.choice([0, 1, 2, 3, 4, 23]))

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": event_time.isoformat(),
        "user_id": random.choice(USERS),
        "department": random.choice(DEPARTMENTS),
        "event_type": event_type,
        "source_ip": fake.ipv4_public(),
        "country": country,
        "device_id": f"device-{random.randint(1, 80):03d}",
        "resource": f"gs://sensitive-data/{fake.file_name()}",
        "data_classification": sensitive,
        "bytes_transferred": bytes_transferred,
        "privileged_account": privileged,
        "is_known_device": random.random() > (0.7 if anomalous else 0.05),
        "label": "simulated_anomaly" if anomalous else "normal",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--anomaly-rate", type=float, default=0.1)
    parser.add_argument("--delay", type=float, default=0.02)
    args = parser.parse_args()

    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "security-events")
    producer = KafkaProducer(
        bootstrap_servers=servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
    )
    for _ in range(args.count):
        event = create_event(random.random() < args.anomaly_rate)
        producer.send(topic, key=event["user_id"].encode(), value=event)
        time.sleep(args.delay)
    producer.flush()
    print(f"Published {args.count} synthetic events to {topic}")


if __name__ == "__main__":
    main()

