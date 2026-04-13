import json
import time
import sys
import os
import argparse
import requests
from datetime import datetime
from collections import Counter

DEFAULT_URL = os.getenv("API_URL", "http://localhost:8000")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
TEAM = os.getenv("TEAM_NAME", "team")
TOPIC_RECO_REQUESTS = os.getenv("KAFKA_TOPIC_RECO_REQUESTS", f"{TEAM}.reco_requests")
TOPIC_RECO_RESPONSES = os.getenv("KAFKA_TOPIC_RECO_RESPONSES", f"{TEAM}.reco_responses")

# Try to import Kafka Client for publishing
try:
    from confluent_kafka import Producer, Consumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


class ProbeRunner:
    def __init__(self, api_url=DEFAULT_URL, num_users=50, calls_per_user=2):
        self.api_url = api_url
        self.num_users = num_users
        self.calls_per_user = calls_per_user
        self.results = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "personalized_responses": 0,
            "non_personalized_responses": 0,
            "latencies": [],
            "models_used": Counter(),
            "errors": []
        }
    
    def is_personalized(self, recommendations):
        """
        Heuristic: response is personalized if recs are diverse or user has history.
        For simplicity, if all recs are from popularity model baseline, it's not personalized.
        """
        return len(set(recommendations)) == len(recommendations) 
    def run_probe(self):
        print(f"[{datetime.now()}] Starting probe: {self.num_users} users x {self.calls_per_user} calls")
        print(f"API URL: {self.api_url}")
        
        for user_id in range(1000, 1000 + self.num_users):
            for call_num in range(self.calls_per_user):
                model = "popularity" if call_num % 2 == 0 else "item_cf"
                k = 10
                
                try:
                    start = time.time()
                    response = requests.get(
                        f"{self.api_url}/recommend/{user_id}",
                        params={"k": k, "model": model},
                        timeout=5
                    )
                    latency_ms = (time.time() - start) * 1000
                    
                    self.results["total_requests"] += 1
                    self.results["latencies"].append(latency_ms)
                    
                    if response.status_code == 200:
                        self.results["successful_requests"] += 1
                        data = response.json()
                        
                        if "recommendations" in data:
                            recs = data["recommendations"]
                            if self.is_personalized(recs):
                                self.results["personalized_responses"] += 1
                            else:
                                self.results["non_personalized_responses"] += 1
                            
                            self.results["models_used"][model] += 1
                            
                            # Log sample
                            if self.results["total_requests"] % 10 == 0:
                                print(f"  ✓ user={user_id} model={model} k={k} recs={len(recs)} latency={latency_ms:.1f}ms")
                    else:
                        self.results["failed_requests"] += 1
                        self.results["errors"].append(f"Status {response.status_code} for user {user_id}")
                
                except Exception as e:
                    self.results["failed_requests"] += 1
                    self.results["errors"].append(str(e))
        
        return self.results
    
    def report(self):
        """Print summary report."""
        print("\n" + "=" * 80)
        print(f"PROBE REPORT [{datetime.now()}]")
        print("=" * 80)
        
        total = self.results["total_requests"]
        success = self.results["successful_requests"]
        failed = self.results["failed_requests"]
        personalized = self.results["personalized_responses"]
        non_personalized = self.results["non_personalized_responses"]
        
        print(f"\nRequests:")
        print(f"  Total: {total}")
        print(f"  Successful: {success} ({100*success/total if total > 0 else 0:.1f}%)")
        print(f"  Failed: {failed}")
        
        print(f"\nPersonalization:")
        print(f"  Personalized responses: {personalized} ({100*personalized/success if success > 0 else 0:.1f}%)")
        print(f"  Non-personalized (baseline): {non_personalized}")
        
        print(f"\nModel Usage:")
        for model, count in self.results["models_used"].items():
            print(f"  {model}: {count}")
        
        if self.results["latencies"]:
            import numpy as np
            latencies = np.array(self.results["latencies"])
            print(f"\nLatency Statistics (ms):")
            print(f"  Min: {latencies.min():.2f}")
            print(f"  Max: {latencies.max():.2f}")
            print(f"  Mean: {latencies.mean():.2f}")
            print(f"  P95: {np.percentile(latencies, 95):.2f}")
        
        if self.results["errors"]:
            print(f"\nErrors ({len(self.results['errors'])}):")
            for err in self.results["errors"][:5]:
                print(f"  - {err}")
        
        print("=" * 80)
        
        return {
            "timestamp": datetime.now().isoformat(),
            "total_requests": total,
            "successful_requests": success,
            "personalized_pct": 100 * personalized / success if success > 0 else 0,
            "mean_latency_ms": float(np.mean(self.results["latencies"]) if self.results["latencies"] else 0),
        }


def main():
    parser = argparse.ArgumentParser(description="Probe API and track metrics")
    parser.add_argument("--url", default=DEFAULT_URL, help="API base URL")
    parser.add_argument("--num-users", type=int, default=50, help="Number of unique users to probe")
    parser.add_argument("--calls-per-user", type=int, default=2, help="Calls per user")
    parser.add_argument("--json", action="store_true", help="Output JSON summary")
    
    args = parser.parse_args()
    
    runner = ProbeRunner(api_url=args.url, num_users=args.num_users, calls_per_user=args.calls_per_user)
    runner.run_probe()
    summary = runner.report()
    
    if args.json:
        print("\nJSON Summary:")
        print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
