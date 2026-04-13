from fastapi import FastAPI, Query
from fastapi.responses import Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from collections import Counter as PyCounter
import pandas as pd
import json
import os
import time

from service.config import (
    MODEL_VERSION,
    POPULARITY_PATH,
    ITEM_CF_PATH,
    SNAPSHOT_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_RECO_REQUESTS,
    TOPIC_RECO_RESPONSES,
)

try:
    from confluent_kafka import Producer
    print("confluent_kafka import OK")
except ImportError as e:
    print("confluent_kafka import FAILED:", e)
    Producer = None

app = FastAPI()

reqs = Counter("recommend_requests_total", "requests", ["status"])
lat = Histogram("recommend_latency_seconds", "latency")
kafka_drop_counter = Counter("kafka_dropped_events_total", "dropped kafka events", ["topic"])

PRODUCER = None


def _create_producer():
    global PRODUCER
    if PRODUCER is not None:
        return PRODUCER
    if Producer is None:
        return None

    PRODUCER = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "linger.ms": 20,
            "batch.num.messages": 1000,
            "queue.buffering.max.messages": 10000,
        }
    )
    return PRODUCER


def _publish_event(topic, event):
    producer = _create_producer()
    if producer is None:
        return False

    try:
        producer.produce(topic, json.dumps(event).encode("utf-8"))
        producer.flush()
        return True
    except Exception as e:
        print("Kafka publish failed", topic, e)
        return False


def load_json(path):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_user_history(user_id: int):
    if not os.path.exists(SNAPSHOT_PATH):
        return []

    df = pd.read_csv(SNAPSHOT_PATH)

    if "user_id" not in df.columns:
        return []

    movie_col = "movie_id" if "movie_id" in df.columns else "item_id"
    if movie_col not in df.columns:
        return []

    return df[df["user_id"] == user_id][movie_col].tolist()


def get_popularity_recs(k: int):
    artifact = load_json(POPULARITY_PATH)
    if not artifact:
        raise FileNotFoundError("popularity.json not found")
    return artifact["items"][:k]


def get_item_cf_recs(user_id: int, k: int):
    artifact = load_json(ITEM_CF_PATH)
    if not artifact:
        raise FileNotFoundError("item_cf.json not found")

    history = get_user_history(user_id)

    if not history:
        return get_popularity_recs(k)

    counts = PyCounter()
    neighbors = artifact["neighbors"]

    for movie_id in history:
        for related in neighbors.get(str(movie_id), []):
            if related not in history:
                counts[related] += 1

    recs = [movie for movie, _ in counts.most_common(k)]

    if not recs:
        return get_popularity_recs(k)

    return recs[:k]


@app.get("/healthz")
def healthz():
    return {"status": "ok", "version": MODEL_VERSION}


@app.get("/recommend/{user_id}")
@lat.time()
def recommend(user_id: int, k: int = 10, model: str = Query(default="popularity")):
    request_event = {
        "user_id": user_id,
        "k": k,
        "model": model,
        "event_ts": time.time(),
        "source": "api",
    }
    _publish_event(TOPIC_RECO_REQUESTS, request_event)

    try:
        if model == "popularity":
            recs = get_popularity_recs(k)
        elif model == "item_cf":
            recs = get_item_cf_recs(user_id, k)
        else:
            raise ValueError("model must be 'popularity' or 'item_cf'")

        response_event = {
            "user_id": user_id,
            "k": k,
            "model": model,
            "recommendations": recs,
            "event_ts": time.time(),
            "source": "api",
        }
        _publish_event(TOPIC_RECO_RESPONSES, response_event)

        producer = _create_producer()
        if producer is not None:
            producer.flush(0.2)

        reqs.labels("200").inc()
        return {
            "user_id": user_id,
            "model_version": MODEL_VERSION,
            "model": model,
            "recommendations": recs,
        }

    except Exception as e:
        reqs.labels("500").inc()
        return {"error": str(e)}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)