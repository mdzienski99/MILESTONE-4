import json
import math
import os
import pandas as pd

RECO_PATH = "artifacts/reco_responses.jsonl"
OUT_PATH = "artifacts/ab_test.json"


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


def normal_two_sided_pvalue(z):
    return math.erfc(abs(z) / math.sqrt(2))


def main():
    if not os.path.exists(RECO_PATH):
        raise FileNotFoundError(f"Missing {RECO_PATH}")

    rows = read_jsonl(RECO_PATH)
    if not rows:
        raise ValueError("No valid rows found in reco_responses.jsonl")

    df = pd.DataFrame(rows)

    if "ab_group" in df.columns:
        df = df[df["ab_group"].isin(["A", "B"])].copy()
    else:
        df["ab_group"] = df["model"].map({"popularity": "A", "item_cf": "B"})
        df = df[df["ab_group"].isin(["A", "B"])].copy()

    if df.empty:
        raise ValueError("No A/B rows found for experiment")

    df["success"] = df.apply(
        lambda r: 1 if isinstance(r.get("recommendations"), list) and len(r["recommendations"]) >= min(int(r.get("k", 5)), 5) else 0,
        axis=1,
    )

    group_a = df[df["ab_group"] == "A"]
    group_b = df[df["ab_group"] == "B"]

    x1 = int(group_a["success"].sum())
    n1 = int(len(group_a))
    x2 = int(group_b["success"].sum())
    n2 = int(len(group_b))

    p1 = x1 / n1 if n1 else 0.0
    p2 = x2 / n2 if n2 else 0.0

    if n1 == 0 or n2 == 0:
        z = 0.0
        p_value = 1.0
    else:
        pooled = (x1 + x2) / (n1 + n2)
        if pooled in (0.0, 1.0):
            z = 0.0
            p_value = 1.0
        else:
            z = (p1 - p2) / math.sqrt(pooled * (1 - pooled) * (1 / n1 + 1 / n2))
            p_value = normal_two_sided_pvalue(z)

    result = {
        "group_A": {
            "definition": "even user_id or popularity arm",
            "successes": x1,
            "total": n1,
            "success_rate": round(p1, 4),
        },
        "group_B": {
            "definition": "odd user_id or item_cf arm",
            "successes": x2,
            "total": n2,
            "success_rate": round(p2, 4),
        },
        "delta": round(p1 - p2, 4),
        "z_score": round(z, 4),
        "p_value": round(p_value, 6),
        "decision": "No significant difference" if p_value >= 0.05 else "Statistically significant difference",
        "metric_definition": "success = recommendation list returned with at least 5 items or requested k if smaller",
    }

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()