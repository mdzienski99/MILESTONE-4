import json
import os

TRACE_PATH = "artifacts/prediction_traces.jsonl"
OUT_PATH = "artifacts/provenance_example.json"


def main():
    if not os.path.exists(TRACE_PATH):
        raise FileNotFoundError(f"Missing {TRACE_PATH}")

    last_valid = None
    with open(TRACE_PATH, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict) and obj:
                    last_valid = obj
            except Exception:
                continue

    if not last_valid:
        raise ValueError("No valid trace rows found")

    out = {
        "request_id": last_valid.get("request_id"),
        "user_id": last_valid.get("user_id"),
        "model_served": last_valid.get("model_served"),
        "ab_group": last_valid.get("ab_group"),
        "model_version": last_valid.get("model_version"),
        "data_snapshot_id": last_valid.get("data_snapshot_id"),
        "pipeline_git_sha": last_valid.get("pipeline_git_sha"),
        "container_image_digest": last_valid.get("container_image_digest"),
        "recommendations": last_valid.get("recommendations"),
        "event_ts": last_valid.get("event_ts"),
    }

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()