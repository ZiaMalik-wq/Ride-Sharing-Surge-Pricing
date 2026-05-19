# Ride-Sharing Surge Pricing Engine

A real-time, distributed surge pricing engine designed to handle high-throughput ride requests and driver availability updates. This project demonstrates modern data engineering practices using streaming, windowed processing, and low-latency storage.

## System Architecture

The engine follows a robust microservices architecture:
Data Generator → Apache Kafka → Apache Flink → Redis → FastAPI → Dashboard

Historical archive path: Apache Flink → Kafka → Cassandra Sink → Apache Cassandra

- Data Generator: Simulates real-time ride requests and driver telemetry using Python.
- Apache Kafka: Message backbone for events.
- Apache Flink: Stateful processing for windowed aggregations.
- Redis Sink: Bridges Kafka → Redis for low-latency state persistence.
- Cassandra Sink: Archives surge history for offline analytics.
- FastAPI: Exposes computed surge pricing via REST.
- Frontend Dashboard: Visual heatmap of surge multipliers.

## Key Features

- Real-time processing with hopping windows.
- Scalable partitioning by `zone_id`.
- Low-latency reads from Redis for live pricing.
- Optional archival to Cassandra for historical analysis.

## Tech Stack

- Python 3.10+
- Kafka, Flink (PyFlink), Redis, Cassandra
- FastAPI + Uvicorn
- Docker & Docker Compose
- Simple static frontend (HTML/CSS/JS)

## Getting Started (Local Development)

These instructions get a developer environment running on Windows, macOS, or Linux.

### Prerequisites

- Docker & Docker Compose
- Python 3.10+

### Quick Setup (recommended)

1. Clone the repository:

```bash
git clone https://github.com/ZiaMalik-wq/Ride-Sharing-Surge-Pricing.git
cd Ride-Sharing-Surge-Pricing
```

2. Create and activate a virtual environment, then install Python deps:

Windows (PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

macOS / Linux:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. (Optional) Download sample datasets into the local `data/` folder:

```bash
python src/scripts/download_data.py
```

Note: The `data/` folder is intentionally excluded from Git (see `.gitignore`).

4. Start required services with Docker Compose:

```bash
docker-compose -f docker/docker-compose.yml up -d
```

5. Start the full pipeline (convenience script):

Windows (recommended):

```powershell
.\start_pipeline.ps1
```

Linux / macOS:

```bash
bash start_pipeline.sh
```

The startup script will:

- start Docker containers
- initialize Cassandra schema
- submit the Flink job
- start the data producer and sinks
- launch the FastAPI server

### Run components manually

If you prefer running components in separate terminals:

- Start the data generator:

```powershell
python src/generator/producer.py
```

- Submit the Flink job (runs inside the Flink container):

```bash
docker exec -it surge_flink_jobmanager flink run -py /opt/flink/project/src/processor/processor.py
```

- Run Redis sink:

```powershell
python src/processor/redis_sink.py
```

- Run Cassandra sink (optional):

```powershell
python src/processor/cassandra_sink.py
```

- Start the API server:

```powershell
uvicorn src.api.main:app --reload --port 8000
```

- Open the dashboard at `frontend/index.html` in your browser to view live multipliers.

## Notes

- The `data/` directory is excluded from version control to prevent committing large or sensitive datasets. Use `src/scripts/download_data.py` to fetch sample input data locally.
- If you modify Docker or network ports, update the compose file and any corresponding client URLs in `frontend/index.html` or `src/api/config.py`.

---

_Developed as a demonstration of production-grade real-time data engineering pipelines._
