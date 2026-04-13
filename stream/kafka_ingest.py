import os
import json
import time
import logging
from confluent_kafka import Consumer
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TEAM = os.getenv("TEAM_NAME", "team")
TOPIC_WATCH = os.getenv("KAFKA_TOPIC_WATCH", f"{TEAM}.watch")
TOPIC_RATE = os.getenv("KAFKA_TOPIC_RATE", f"{TEAM}.rate")
TOPIC_RECO_REQUESTS = os.getenv("KAFKA_TOPIC_RECO_REQUESTS", f"{TEAM}.reco_requests")
TOPIC_RECO_RESPONSES = os.getenv("KAFKA_TOPIC_RECO_RESPONSES", f"{TEAM}.reco_responses")

SNAPSHOT_CSV = os.getenv("SNAPSHOT_CSV", "snapshots/watch_events.csv")
SNAPSHOT_PARQUET = os.getenv("SNAPSHOT_PARQUET", "snapshots/watch_events.parquet")

REQUIRED_WATCH_COLUMNS = ["user_id", "movie_id", "event_ts"]


def validate_watch_event(event: dict) -> bool:
    if not all(k in event for k in REQUIRED_WATCH_COLUMNS):
        logging.warning("Invalid event schema (missing required field): %s", event)
        return False

    try:
        int(event["user_id"])
        int(event["movie_id"])
        float(event["event_ts"])
        return True
    except (ValueError, TypeError):
        logging.warning("Invalid event types: %s", event)
        return False


def append_snapshot(event: dict):
    snapshot = {
        "user_id": int(event["user_id"]),
        "movie_id": int(event["movie_id"]),
        "event_ts": float(event["event_ts"]),
        "event_type": event.get("event_type", "watch")
    }

    if os.path.exists(SNAPSHOT_CSV):
        df = pd.read_csv(SNAPSHOT_CSV)
    else:
        df = pd.DataFrame(columns=["user_id", "movie_id", "event_ts", "event_type"])

    df = pd.concat([df, pd.DataFrame([snapshot])], ignore_index=True)

    logging.info("Updating snapshot files: %s, %s", SNAPSHOT_CSV, SNAPSHOT_PARQUET)
    df.to_csv(SNAPSHOT_CSV, index=False)
    df.to_parquet(SNAPSHOT_PARQUET, index=False)


def consume_events():
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': os.getenv('KAFKA_GROUP_ID', f'{TEAM}-ingest'),
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([TOPIC_WATCH, TOPIC_RATE])

    logging.info('Kafka ingest consumer connected to %s; topics: %s, %s', BOOTSTRAP_SERVERS, TOPIC_WATCH, TOPIC_RATE)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                time.sleep(0.3)
                continue
            if msg.error():
                logging.warning('Kafka poll error: %s', msg.error())
                continue

            topic = msg.topic()
            raw = msg.value()
            try:
                value = json.loads(raw.decode('utf-8') if isinstance(raw, bytes) else raw)
            except Exception as exc:
                logging.warning('Malformed message on topic=%s: %s', topic, exc)
                continue

            logging.info('Received topic=%s value=%s', topic, value)

            if topic == TOPIC_WATCH:
                if validate_watch_event(value):
                    append_snapshot(value)
                else:
                    logging.warning('Dropping invalid watch event')
            elif topic == TOPIC_RATE:
                logging.info('Skipping rating event (not persisted yet): %s', value)
            else:
                logging.warning('Unexpected topic: %s', topic)

    except KeyboardInterrupt:
        logging.info('Consumer shutdown requested')
    finally:
        consumer.close()


if __name__ == "__main__":
    offset_reset = os.getenv("KAFKA_OFFSET_RESET", "false").lower() == "true"
    if offset_reset:
        logging.warning("Offset reset requested; please manually reset consumer group to earliest if needed")

    os.makedirs(os.path.dirname(SNAPSHOT_CSV), exist_ok=True)
    consume_events()
