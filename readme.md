# Real-Time Movie Recommender

## Michal Dzienski & Rayan Rabbi

## Quick Start

1. **Setup**
   ```bash
   git clone <repo-url>
   cd Milestone-Project-COT-6930
   python -m venv .venv
   # activate venv
   pip install -r requirements.txt
   ```

2. **Run Locally**
   ```bash
   uvicorn service.app:app --reload --port 8000
   ```
   Endpoints:
   - `/healthz` — health check
   - `/recommend/{user_id}` — get recommendations
   - `/metrics` — Prometheus metrics

3. **Kafka (local)**
   ```bash
   docker compose up -d
   powershell -File scripts/kafka_start_and_init.ps1
   kcat -b localhost:9092 -L
   ```

4. **Start Pipeline**
   ```bash
   python stream/kafka_ingest.py
   ```

5. **Test API**
   ```bash
   curl "http://localhost:8000/recommend/1001?k=10&model=popularity"
   ```

## Deploy to Cloud Run

1. **Build & Push Image**
   ```bash
   docker build -t gcr.io/<GCP_PROJECT_ID>/recommender-api:latest .
   docker push gcr.io/<GCP_PROJECT_ID>/recommender-api:latest
   ```

2. **Deploy**
   ```bash
   gcloud run deploy recommender-api \
     --image gcr.io/<GCP_PROJECT_ID>/recommender-api:latest \
     --platform managed \
     --region us-central1 \
     --allow-unauthenticated \
     --set-env-vars "KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092,TEAM_NAME=team"
   ```

3. **Test Deployed API**
   ```bash
   curl "https://<service-url>/healthz"
   curl "https://<service-url>/recommend/1?k=10&model=popularity"
   ```

---

## Model Comparison
Run:
```bash
python stream/model_comparison.py
```

## Probe Pipeline
Run:
```bash
python scripts/probe.py --num-users 50 --calls-per-user 2 --json
```
