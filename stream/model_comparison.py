"""
Model comparison metrics: HR@K, NDCG@K, latency, training cost, model size.
"""

import json
import os
import time
import pandas as pd
from collections import Counter, defaultdict
import numpy as np

POPULARITY_PATH = "artifacts/popularity.json"
ITEM_CF_PATH = "artifacts/item_cf.json"
SNAPSHOT_PATH = "snapshots/watch_events.csv"

# Hyperparams
K_VALUES = [5, 10, 20]
TEST_SPLIT = 0.2


def load_data():
    if not os.path.exists(SNAPSHOT_PATH):
        raise FileNotFoundError(f"Missing snapshot file: {SNAPSHOT_PATH}")
    df = pd.read_csv(SNAPSHOT_PATH)
    required_cols = {"user_id", "movie_id", "event_ts"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"CSV must contain columns: {required_cols}")
    return df


def train_test_split(df, test_split=0.2):
    """Split into train and test by user (temporal)."""
    df_sorted = df.sort_values("event_ts").reset_index(drop=True)
    split_idx = int(len(df_sorted) * (1 - test_split))
    return df_sorted[:split_idx], df_sorted[split_idx:]


def hr_at_k(true_items, predicted_items, k):
    """Hit Rate@K: did any true item appear in top-k predictions?"""
    pred_set = set(predicted_items[:k])
    for item in true_items:
        if item in pred_set:
            return 1.0
    return 0.0


def ndcg_at_k(true_items, predicted_items, k):
    """Normalized Discounted Cumulative Gain@K."""
    pred_set = set(predicted_items[:k])
    true_set = set(true_items)
    
    dcg = 0.0
    for i, item in enumerate(predicted_items[:k], start=1):
        if item in true_set:
            dcg += 1.0 / np.log2(i + 1)
    
    # IDCG: perfect ranking (all true items ranked first)
    idcg = sum(1.0 / np.log2(i + 1) for i in range(1, min(len(true_items), k) + 1))
    
    return dcg / idcg if idcg > 0 else 0.0


def evaluate_popularity_model(df_test, model_artifact):
    """Evaluate popularity model on test set."""
    popular_items = model_artifact["items"]
    
    metrics = defaultdict(list)
    
    for k in K_VALUES:
        hr_scores = []
        ndcg_scores = []
        
        for user_id in df_test["user_id"].unique():
            user_test = df_test[df_test["user_id"] == user_id]["movie_id"].tolist()
            if not user_test:
                continue
            
            # For popularity, always return the same items (non-personalized)
            predictions = popular_items.copy()
            
            hr = hr_at_k(user_test, predictions, k)
            ndcg = ndcg_at_k(user_test, predictions, k)
            
            hr_scores.append(hr)
            ndcg_scores.append(ndcg)
        
        if hr_scores:
            metrics[f"HR@{k}"].append(np.mean(hr_scores))
            metrics[f"NDCG@{k}"].append(np.mean(ndcg_scores))
    
    return dict(metrics)


def evaluate_item_cf_model(df_test, df_train, model_artifact):
    """Evaluate item-CF model on test set."""
    neighbors = model_artifact["neighbors"]
    
    # Build train history for item-CF
    train_user_movies = df_train.groupby("user_id")["movie_id"].apply(list).to_dict()
    
    metrics = defaultdict(list)
    
    for k in K_VALUES:
        hr_scores = []
        ndcg_scores = []
        
        for user_id in df_test["user_id"].unique():
            user_test = df_test[df_test["user_id"] == user_id]["movie_id"].tolist()
            if not user_test:
                continue
            
            # Get user's train history
            history = train_user_movies.get(user_id, [])
            if not history:
                # Fallback to popularity if no history
                predictions = model_artifact.get("items", [])[:k]
            else:
                counts = Counter()
                seen = set(history)
                for movie_id in history:
                    for related in neighbors.get(str(movie_id), []):
                        if related not in seen:
                            counts[related] += 1
                
                predictions = [movie for movie, _ in counts.most_common(k)]
            
            hr = hr_at_k(user_test, predictions, k)
            ndcg = ndcg_at_k(user_test, predictions, k)
            
            hr_scores.append(hr)
            ndcg_scores.append(ndcg)
        
        if hr_scores:
            metrics[f"HR@{k}"].append(np.mean(hr_scores))
            metrics[f"NDCG@{k}"].append(np.mean(ndcg_scores))
    
    return dict(metrics)


def get_model_size(path):
    """Get artifact file size in MB."""
    if os.path.exists(path):
        size_mb = os.path.getsize(path) / (1024 * 1024)
        return size_mb
    return 0.0


def benchmark_latency(model_type, num_calls=100):
    """Measure inference latency locally."""
    import time
    import requests
    
    latencies = []
    for i in range(num_calls):
        user_id = 1001 + (i % 100)
        start = time.time()
        try:
            resp = requests.get(
                f"http://localhost:8000/recommend/{user_id}",
                params={"k": 10, "model": model_type},
                timeout=5
            )
            latency = (time.time() - start) * 1000  # ms
            if resp.status_code == 200:
                latencies.append(latency)
        except Exception as e:
            print(f"Latency benchmark failed for {model_type}: {e}")
            return None
    
    if latencies:
        return {
            "min_ms": min(latencies),
            "max_ms": max(latencies),
            "mean_ms": np.mean(latencies),
            "p95_ms": np.percentile(latencies, 95)
        }
    return None


def main():
    print("=" * 80)
    print("MODEL COMPARISON: Metrics & Performance Analysis")
    print("=" * 80)
    
    # Load data
    df = load_data()
    print(f"\nLoaded {len(df)} events, {df['user_id'].nunique()} users, {df['movie_id'].nunique()} movies")
    
    df_train, df_test = train_test_split(df, test_split=TEST_SPLIT)
    print(f"Train: {len(df_train)} events | Test: {len(df_test)} events")
    
    # Load models
    pop_artifact = json.load(open(POPULARITY_PATH))
    cf_artifact = json.load(open(ITEM_CF_PATH))
    
    print(f"\n--- POPULARITY MODEL ---")
    pop_start = time.time()
    pop_metrics = evaluate_popularity_model(df_test, pop_artifact)
    pop_time = time.time() - pop_start
    print(f"Evaluation time: {pop_time:.3f}s")
    for metric, values in sorted(pop_metrics.items()):
        print(f"  {metric}: {values[0]:.4f}")
    
    print(f"\n--- ITEM-CF MODEL ---")
    cf_start = time.time()
    cf_metrics = evaluate_item_cf_model(df_test, df_train, cf_artifact)
    cf_time = time.time() - cf_start
    print(f"Evaluation time: {cf_time:.3f}s")
    for metric, values in sorted(cf_metrics.items()):
        print(f"  {metric}: {values[0]:.4f}")
    
    # Model sizes
    pop_size = get_model_size(POPULARITY_PATH)
    cf_size = get_model_size(ITEM_CF_PATH)
    print(f"\n--- MODEL SIZE ---")
    print(f"Popularity: {pop_size:.6f} MB")
    print(f"Item-CF: {cf_size:.6f} MB")
    
    # Latency benchmarks (requires API running)
    print(f"\n--- INFERENCE LATENCY (100 calls, requires API on localhost:8000) ---")
    pop_latency = benchmark_latency("popularity", num_calls=100)
    if pop_latency:
        print(f"Popularity:")
        for k, v in pop_latency.items():
            print(f"  {k}: {v:.2f} ms")
    
    cf_latency = benchmark_latency("item_cf", num_calls=100)
    if cf_latency:
        print(f"Item-CF:")
        for k, v in cf_latency.items():
            print(f"  {k}: {v:.2f} ms")
    
    # Summary table
    print(f"\n" + "=" * 80)
    print("COMPARISON TABLE")
    print("=" * 80)
    print(f"{'Metric':<20} {'Popularity':<20} {'Item-CF':<20}")
    print("-" * 80)
    
    for k in K_VALUES:
        if f"HR@{k}" in pop_metrics:
            pop_hr = pop_metrics[f"HR@{k}"][0]
            cf_hr = cf_metrics[f"HR@{k}"][0]
            print(f"{'HR@' + str(k):<20} {pop_hr:<20.4f} {cf_hr:<20.4f}")
        
        if f"NDCG@{k}" in pop_metrics:
            pop_ndcg = pop_metrics[f"NDCG@{k}"][0]
            cf_ndcg = cf_metrics[f"NDCG@{k}"][0]
            print(f"{'NDCG@' + str(k):<20} {pop_ndcg:<20.4f} {cf_ndcg:<20.4f}")
    
    print("-" * 80)
    print(f"{'Model Size (MB)':<20} {pop_size:<20.6f} {cf_size:<20.6f}")
    
    if pop_latency and cf_latency:
        print(f"{'Mean Latency (ms)':<20} {pop_latency['mean_ms']:<20.2f} {cf_latency['mean_ms']:<20.2f}")
        print(f"{'P95 Latency (ms)':<20} {pop_latency['p95_ms']:<20.2f} {cf_latency['p95_ms']:<20.2f}")
    
    print("=" * 80)


if __name__ == "__main__":
    main()
