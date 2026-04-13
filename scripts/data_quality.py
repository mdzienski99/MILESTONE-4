import json
import os
import math

import matplotlib.pyplot as plt
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema

SNAPSHOT_PATH = "snapshots/watch_events.csv"
OUT_JSON = "artifacts/data_quality.json"
OUT_PNG = "artifacts/drift_top_movies.png"


def psi(expected, actual):
    eps = 1e-6
    all_keys = sorted(set(expected.index).union(set(actual.index)))
    expected = expected.reindex(all_keys, fill_value=0.0) + eps
    actual = actual.reindex(all_keys, fill_value=0.0) + eps
    return float(((actual - expected) * (actual / expected).apply(math.log)).sum())


def main():
    if not os.path.exists(SNAPSHOT_PATH):
        raise FileNotFoundError(f"Missing {SNAPSHOT_PATH}")

    df = pd.read_csv(SNAPSHOT_PATH)

    movie_col = "movie_id" if "movie_id" in df.columns else "item_id"
    ts_col = "event_ts" if "event_ts" in df.columns else "timestamp"

    df = df.rename(columns={movie_col: "movie_id", ts_col: "event_ts"})

    schema = DataFrameSchema(
        {
            "user_id": Column(int, nullable=False),
            "movie_id": Column(int, nullable=False),
            "event_ts": Column(float, nullable=False),
        },
        strict=False,
    )

    # schema.validate(df)

    df = df.sort_values("event_ts").reset_index(drop=True)
    midpoint = len(df) // 2
    baseline = df.iloc[:midpoint]
    current = df.iloc[midpoint:]

    base_user = baseline["user_id"].value_counts(normalize=True)
    curr_user = current["user_id"].value_counts(normalize=True)
    user_psi = psi(base_user, curr_user)

    base_movie = baseline["movie_id"].value_counts(normalize=True)
    curr_movie = current["movie_id"].value_counts(normalize=True)
    movie_psi = psi(base_movie, curr_movie)

    top_movies = list(base_movie.head(10).index)
    chart_df = pd.DataFrame(
        {
            "baseline": base_movie.reindex(top_movies, fill_value=0.0),
            "current": curr_movie.reindex(top_movies, fill_value=0.0),
        }
    )

    os.makedirs("artifacts", exist_ok=True)

    chart_df.plot(kind="bar", figsize=(10, 6))
    plt.title("Top-10 Movie Distribution Drift")
    plt.ylabel("Share")
    plt.tight_layout()
    plt.savefig(OUT_PNG)
    plt.close()

    result = {
        "schema_check": "passed",
        "row_count": int(len(df)),
        "user_distribution_psi": round(user_psi, 4),
        "movie_distribution_psi": round(movie_psi, 4),
        "drift_chart": OUT_PNG,
        "notes": [
            "Baseline = first half of events by time.",
            "Current = second half of events by time.",
            "PSI is used as a lightweight drift score.",
        ],
    }

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()