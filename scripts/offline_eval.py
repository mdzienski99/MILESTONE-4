import json
import math
import os
from collections import Counter, defaultdict

import pandas as pd

SNAPSHOT_PATH = "snapshots/watch_events.csv"
OUT_PATH = "artifacts/offline_eval.json"


def load_df():
    if not os.path.exists(SNAPSHOT_PATH):
        raise FileNotFoundError(f"Missing {SNAPSHOT_PATH}")

    df = pd.read_csv(SNAPSHOT_PATH)

    movie_col = "movie_id" if "movie_id" in df.columns else "item_id"
    ts_col = "event_ts" if "event_ts" in df.columns else "timestamp"

    required = {"user_id", movie_col, ts_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    df = df.rename(columns={movie_col: "movie_id", ts_col: "event_ts"})
    df = df.sort_values(["user_id", "event_ts"]).reset_index(drop=True)
    return df


def build_train_test(df):
    train_parts = []
    test_parts = []

    for _, group in df.groupby("user_id"):
        n = len(group)
        if n < 2:
            continue

        split_idx = max(1, int(n * 0.8))
        if split_idx >= n:
            split_idx = n - 1

        train_parts.append(group.iloc[:split_idx])
        test_parts.append(group.iloc[split_idx:])

    train_df = pd.concat(train_parts).reset_index(drop=True)
    test_df = pd.concat(test_parts).reset_index(drop=True)
    return train_df, test_df


def hr_at_k(true_items, predicted, k=10):
    pred = set(predicted[:k])
    return 1.0 if any(x in pred for x in true_items) else 0.0


def ndcg_at_k(true_items, predicted, k=10):
    true_set = set(true_items)
    dcg = 0.0

    for i, item in enumerate(predicted[:k], start=1):
        if item in true_set:
            dcg += 1.0 / math.log2(i + 1)

    ideal_hits = min(len(true_set), k)
    if ideal_hits == 0:
        return 0.0

    idcg = sum(1.0 / math.log2(i + 1) for i in range(1, ideal_hits + 1))
    return dcg / idcg


def build_popularity(train_df):
    return train_df["movie_id"].value_counts().index.tolist()


def build_neighbors(train_df):
    neighbors = defaultdict(Counter)

    for _, group in train_df.groupby("user_id"):
        movies = list(dict.fromkeys(group["movie_id"].tolist()))
        for m in movies:
            for n in movies:
                if m != n:
                    neighbors[m][n] += 1

    return {str(k): [x for x, _ in v.most_common(50)] for k, v in neighbors.items()}


def predict_item_cf(user_history, neighbors, popularity, k=10):
    if not user_history:
        return popularity[:k]

    seen = set(user_history)
    counts = Counter()

    for movie in user_history:
        for related in neighbors.get(str(movie), []):
            if related not in seen:
                counts[related] += 1

    recs = [m for m, _ in counts.most_common(k)]
    if not recs:
        return popularity[:k]

    return recs[:k]


def evaluate(train_df, test_df):
    popularity = build_popularity(train_df)
    neighbors = build_neighbors(train_df)
    train_hist = train_df.groupby("user_id")["movie_id"].apply(list).to_dict()

    results = {
        "overall": {},
        "subpopulations": {
            "cold_users": {},
            "warm_users": {},
        },
        "notes": [
            "Chronological split per user (no leakage)",
            "Metrics: HR@10, NDCG@10",
        ],
    }

    for model in ["popularity", "item_cf"]:
        overall_hr, overall_ndcg = [], []
        cold_hr, cold_ndcg = [], []
        warm_hr, warm_ndcg = [], []

        for user_id, group in test_df.groupby("user_id"):
            true_items = group["movie_id"].tolist()
            history = train_hist.get(user_id, [])

            if model == "popularity":
                pred = popularity[:10]
            else:
                pred = predict_item_cf(history, neighbors, popularity, k=10)

            hr = hr_at_k(true_items, pred)
            ndcg = ndcg_at_k(true_items, pred)

            overall_hr.append(hr)
            overall_ndcg.append(ndcg)

            if len(history) < 5:
                cold_hr.append(hr)
                cold_ndcg.append(ndcg)
            else:
                warm_hr.append(hr)
                warm_ndcg.append(ndcg)

        results["overall"][model] = {
            "HR@10": round(sum(overall_hr) / len(overall_hr), 4),
            "NDCG@10": round(sum(overall_ndcg) / len(overall_ndcg), 4),
        }

        results["subpopulations"]["cold_users"][model] = {
            "HR@10": round(sum(cold_hr) / len(cold_hr), 4) if cold_hr else 0,
            "NDCG@10": round(sum(cold_ndcg) / len(cold_ndcg), 4) if cold_ndcg else 0,
        }

        results["subpopulations"]["warm_users"][model] = {
            "HR@10": round(sum(warm_hr) / len(warm_hr), 4) if warm_hr else 0,
            "NDCG@10": round(sum(warm_ndcg) / len(warm_ndcg), 4) if warm_ndcg else 0,
        }

    return results


def main():
    os.makedirs("artifacts", exist_ok=True)

    df = load_df()
    train_df, test_df = build_train_test(df)

    results = evaluate(train_df, test_df)

    with open(OUT_PATH, "w") as f:
        json.dump(results, f, indent=2)

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()