import json
import os

MONITOR_PATH = "artifacts/monitoring_report.json"
OUT_PATH = "artifacts/availability_report.json"


def main():
    if not os.path.exists(MONITOR_PATH):
        raise FileNotFoundError(f"Missing {MONITOR_PATH}")

    with open(MONITOR_PATH, "r", encoding="utf-8") as f:
        monitor = json.load(f)

    availability = float(monitor.get("availability", 0.0))

    result = {
        "availability": round(availability, 4),
        "threshold": 0.70,
        "meets_requirement": availability >= 0.70,
        "formula": "successful_samples / total_samples",
        "note": "This was measured over the local demo observation window. The same formula applies to the required 7-day availability window.",
    }

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()