import json
import os
from datetime import timedelta

import pandas as pd

RECO_PATH = "artifacts/reco_responses.jsonl"
WATCH_PATH = "artifacts/watch.jsonl"
OUT_PATH = "artifacts/online_kpi.json"

WINDOW_MINUTES = 30


def read_jsonl(path):
    rows = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict) and obj:
                    rows.append(obj)
            except Exception:
                continue
    return rows


def main():
    if not os.path.exists(RECO_PATH):
        raise FileNotFoundError(f"Missing {RECO_PATH}")

    reco_rows = read_jsonl(RECO_PATH)
    if not reco_rows:
        raise ValueError("No valid reco response rows found in reco_responses.jsonl")

    reco = pd.DataFrame(reco_rows)

    required_reco_cols = {"user_id", "recommendations", "event_ts"}
    missing_reco = required_reco_cols - set(reco.columns)
    if missing_reco:
        raise ValueError(f"Reco file missing required columns: {missing_reco}")

    reco["event_ts"] = pd.to_datetime(reco["event_ts"], unit="s", errors="coerce")
    reco = reco.dropna(subset=["event_ts"])

    if not os.path.exists(WATCH_PATH):
        result = {
            "metric": f"watched_any_recommended_within_{WINDOW_MINUTES}_minutes",
            "numerator": 0,
            "denominator": int(len(reco)),
            "success_rate": 0.0,
            "note": "watch.jsonl not found, so no watch conversions could be matched.",
        }
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)
        print(json.dumps(result, indent=2))
        return

    watch_rows = read_jsonl(WATCH_PATH)

    if not watch_rows:
        result = {
            "metric": f"watched_any_recommended_within_{WINDOW_MINUTES}_minutes",
            "numerator": 0,
            "denominator": int(len(reco)),
            "success_rate": 0.0,
            "note": "watch.jsonl exists but contains no valid watch events.",
        }
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)
        print(json.dumps(result, indent=2))
        return

    watch = pd.DataFrame(watch_rows)

    watch_item_col = "movie_id" if "movie_id" in watch.columns else "item_id"
    required_watch_cols = {"user_id", "event_ts", watch_item_col}
    missing_watch = required_watch_cols - set(watch.columns)
    if missing_watch:
        result = {
            "metric": f"watched_any_recommended_within_{WINDOW_MINUTES}_minutes",
            "numerator": 0,
            "denominator": int(len(reco)),
            "success_rate": 0.0,
            "note": f"watch.jsonl is missing required columns: {sorted(missing_watch)}",
        }
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)
        print(json.dumps(result, indent=2))
        return

    watch["event_ts"] = pd.to_datetime(watch["event_ts"], unit="s", errors="coerce")
    watch = watch.dropna(subset=["event_ts"])

    successes = 0
    total = 0

    for _, row in reco.iterrows():
        user_id = row["user_id"]
        rec_time = row["event_ts"]
        recs = row["recommendations"]

        if not isinstance(recs, list):
            continue

        total += 1
        window_end = rec_time + timedelta(minutes=WINDOW_MINUTES)

        watched = watch[
            (watch["user_id"] == user_id)
            & (watch["event_ts"] >= rec_time)
            & (watch["event_ts"] <= window_end)
            & (watch[watch_item_col].isin(recs))
        ]

        if len(watched) > 0:
            successes += 1

    result = {
        "metric": f"watched_any_recommended_within_{WINDOW_MINUTES}_minutes",
        "numerator": int(successes),
        "denominator": int(total),
        "success_rate": round(successes / total, 4) if total else 0.0,
    }

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()