import json
import os
import time
import requests
import matplotlib.pyplot as plt

BASE_URL = os.getenv("API_URL", "http://localhost:8000")
OUT_JSON = "artifacts/monitoring_report.json"
OUT_SAMPLES = "artifacts/monitoring_samples.jsonl"
OUT_PNG = "artifacts/latency_timeseries.png"


def percentile(values, p):
    if not values:
        return 0.0
    values = sorted(values)
    idx = int(round((p / 100) * (len(values) - 1)))
    return values[idx]


def main():
    os.makedirs("artifacts", exist_ok=True)

    samples = []
    latencies = []
    successes = 0
    total = 20

    for i in range(total):
        user_id = (i % 4) + 1
        start = time.time()
        try:
            r = requests.get(
                f"{BASE_URL}/recommend/{user_id}",
                params={"k": 5, "model": "auto"},
                timeout=5,
            )
            latency_ms = (time.time() - start) * 1000
            ok = r.status_code == 200
            if ok:
                successes += 1
                latencies.append(latency_ms)
            samples.append({
                "sample_id": i + 1,
                "status_code": r.status_code,
                "latency_ms": round(latency_ms, 2),
            })
        except Exception:
            latency_ms = (time.time() - start) * 1000
            samples.append({
                "sample_id": i + 1,
                "status_code": 500,
                "latency_ms": round(latency_ms, 2),
            })

    with open(OUT_SAMPLES, "w", encoding="utf-8") as f:
        for row in samples:
            f.write(json.dumps(row) + "\n")

    availability = successes / total if total else 0.0
    error_rate = 1.0 - availability
    p95_latency = percentile(latencies, 95)

    plt.figure(figsize=(10, 5))
    plt.plot([s["sample_id"] for s in samples], [s["latency_ms"] for s in samples], marker="o")
    plt.title("Latency Timeseries")
    plt.xlabel("Sample")
    plt.ylabel("Latency (ms)")
    plt.tight_layout()
    plt.savefig(OUT_PNG)
    plt.close()

    result = {
        "total_samples": total,
        "successful_samples": successes,
        "availability": round(availability, 4),
        "error_rate": round(error_rate, 4),
        "p95_latency_ms": round(p95_latency, 2),
        "metrics_endpoint": f"{BASE_URL}/metrics",
        "alert_policy": {
            "latency": "alert if p95_latency_ms > 1000",
            "errors": "alert if error_rate > 0.30",
            "availability": "alert if availability < 0.70",
        },
    }

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()