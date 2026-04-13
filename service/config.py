import os

MODEL_VERSION = os.getenv("MODEL_VERSION", "m3")

POPULARITY_PATH = os.getenv("POPULARITY_PATH", "artifacts/popularity.json")
ITEM_CF_PATH = os.getenv("ITEM_CF_PATH", "artifacts/item_cf.json")
SNAPSHOT_PATH = os.getenv("SNAPSHOT_PATH", "snapshots/watch_events.csv")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TEAM_NAME = os.getenv("TEAM_NAME", "team")

TOPIC_WATCH = os.getenv("KAFKA_TOPIC_WATCH", f"{TEAM_NAME}.watch")
TOPIC_RATE = os.getenv("KAFKA_TOPIC_RATE", f"{TEAM_NAME}.rate")
TOPIC_RECO_REQUESTS = os.getenv("KAFKA_TOPIC_RECO_REQUESTS", f"{TEAM_NAME}.reco_requests")
TOPIC_RECO_RESPONSES = os.getenv("KAFKA_TOPIC_RECO_RESPONSES", f"{TEAM_NAME}.reco_responses")