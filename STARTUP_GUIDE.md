# Pipeline Startup Guide

## One-Command Startup

Simply run **one** of these commands based on your OS:

### Windows

```powershell
.\start_pipeline.bat
```

or

```powershell
.\start_pipeline.ps1
```

### Linux/Mac

```bash
bash start_pipeline.sh
```

---

## What Gets Started

The startup script automatically launches:

1. **Docker Containers** (via docker-compose)
   - Kafka (message broker)
   - Redis (live state store)
   - Cassandra (historical analytics)
   - Zookeeper (Kafka coordination)
   - Flink JobManager & TaskManager (stream processor)

2. **Python Services** (background processes)
   - **Producer**: Streams ride/driver events to Kafka
   - **Redis Sink**: Consumes Kafka → stores in Redis
   - **Cassandra Sink**: Consumes Kafka → archives to Cassandra
   - **FastAPI**: REST API server
   - **Dashboard**: Opens in your default browser

---

## Access Points

Once running, access these services:

| Service          | URL                        | Purpose                                        |
| ---------------- | -------------------------- | ---------------------------------------------- |
| **Dashboard**    | http://127.0.0.1:5500      | Real-time surge pricing visualization          |
| **Flink UI**     | http://localhost:8081      | Monitor Flink job metrics                      |
| **FastAPI Docs** | http://localhost:8000/docs | Interactive API documentation                  |
| **Redis CLI**    | N/A (Docker)               | Access via `docker exec surge_redis redis-cli` |

---

## Stopping the Pipeline

Press **Ctrl+C** in the terminal where the startup script is running.

The script will:

- Stop all background processes (producer, sinks, API)
- Keep Docker containers running (use `docker-compose down` to stop them)

---

## Advanced Options (PowerShell)

```powershell
# Skip opening dashboard in browser
.\start_pipeline.ps1 -SkipBrowser

# Skip starting dashboard entirely (FastAPI still runs)
.\start_pipeline.ps1 -SkipDashboard
```

---

## Logs

All output is logged to:

- **Pipeline log**: `pipeline.log`
- **Producer**: `/tmp/producer.log` (Linux/Mac) or background
- **FastAPI**: `/tmp/fastapi.log` (Linux/Mac) or background

---

## Troubleshooting

### Services fail to start

1. Check if ports are already in use:
   - Redis: 6379
   - Kafka: 9092, 29092
   - Cassandra: 9042
   - Flink: 8081
   - FastAPI: 8000

2. Check Docker is running: `docker ps`

3. Review logs in `pipeline.log` for detailed errors

### Flink job won't start

Check Flink UI: http://localhost:8081/jobmanager/jobs

### Dashboard shows zero demand/supply

Wait 30-60 seconds for data to flow through the pipeline. Check:

```bash
docker exec surge_redis redis-cli GET "surge:225"
```

### Python dependencies missing

Ensure virtual environment is activated:

```powershell
.\.venv\Scripts\Activate.ps1  # Windows
source .venv/bin/activate      # Linux/Mac
```

---

## Manual Alternative

If you prefer to run components separately, see [Running the Pipeline → Manual Setup](README.md#manual-setup-alternative) in README.md
