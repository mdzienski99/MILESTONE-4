import json
import os
from collections import Counter, defaultdict

import pandas as pd

SNAPSHOT_PATH = "snapshots/watch_events.csv"
ARTIFACT_DIR = "artifacts"
POPULARITY_PATH = os.path.join(ARTIFACT_DIR, "popularity.json")
ITEM_CF_PATH = os.path.join(ARTIFACT_DIR, "item_cf.json")

os.makedirs(ARTIFACT_DIR, exist_ok=True)


def load_data():
    if not os.path.exists(SNAPSHOT_PATH):
        raise FileNotFoundError(f"Missing snapshot file: {SNAPSHOT_PATH}")

    df = pd.read_csv(SNAPSHOT_PATH)

    required_cols = {"user_id", "movie_id", "event_ts"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"CSV must contain columns: {required_cols}")

    return df


def train_popularity(df: pd.DataFrame):
    counts = df["movie_id"].value_counts().astype(int)
    popular_items = counts.index.tolist()

    with open(POPULARITY_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {
                "model_name": "popularity",
                "items": popular_items
            },
            f,
            indent=2
        )


def train_item_cf(df: pd.DataFrame):
    user_movies = df.groupby("user_id")["movie_id"].apply(list)

    co_counts = defaultdict(Counter)

    for movies in user_movies:
        unique_movies = list(dict.fromkeys(movies))
        for i in unique_movies:
            for j in unique_movies:
                if i != j:
                    co_counts[str(i)][str(j)] += 1

    item_cf = {}
    for item, related in co_counts.items():
        ranked = [int(movie_id) for movie_id, _ in related.most_common(20)]
        item_cf[item] = ranked

    with open(ITEM_CF_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {
                "model_name": "item_cf",
                "neighbors": item_cf
            },
            f,
            indent=2
        )


def main():
    df = load_data()
    train_popularity(df)
    train_item_cf(df)

    print("Saved artifacts:")
    print(f" - {POPULARITY_PATH}")
    print(f" - {ITEM_CF_PATH}")


if __name__ == "__main__":
    main()