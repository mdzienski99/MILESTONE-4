import os
from pathlib import Path

ARTIFACT_DIR = os.getenv("ARTIFACT_DIR", "artifacts")
Path(ARTIFACT_DIR).mkdir(exist_ok=True)

MODEL_VERSION = os.getenv("MODEL_VERSION", "m4")

POPULARITY_PATH = os.getenv("POPULARITY_PATH", f"{ARTIFACT_DIR}/popularity.json")
ITEM_CF_PATH = os.getenv("ITEM_CF_PATH", f"{ARTIFACT_DIR}/item_cf.json")
SNAPSHOT_PATH = os.getenv("SNAPSHOT_PATH", "snapshots/watch_events.csv")

REGISTRY_PATH = os.getenv("REGISTRY_PATH", f"{ARTIFACT_DIR}/model_registry.json")
TRACE_PATH = os.getenv("TRACE_PATH", f"{ARTIFACT_DIR}/prediction_traces.jsonl")
VERSION_LOG_PATH = os.getenv("VERSION_LOG_PATH", f"{ARTIFACT_DIR}/model_versions.log")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ENABLE_KAFKA = os.getenv("ENABLE_KAFKA", "true").lower() == "true"
TEAM_NAME = os.getenv("TEAM_NAME", "team")

TOPIC_WATCH = os.getenv("KAFKA_TOPIC_WATCH", f"{TEAM_NAME}.watch")
TOPIC_RATE = os.getenv("KAFKA_TOPIC_RATE", f"{TEAM_NAME}.rate")
TOPIC_RECO_REQUESTS = os.getenv("KAFKA_TOPIC_RECO_REQUESTS", f"{TEAM_NAME}.reco_requests")
TOPIC_RECO_RESPONSES = os.getenv("KAFKA_TOPIC_RECO_RESPONSES", f"{TEAM_NAME}.reco_responses")

GIT_SHA = os.getenv("GIT_SHA", "local")
CONTAINER_IMAGE_DIGEST = os.getenv("CONTAINER_IMAGE_DIGEST", "local-image")