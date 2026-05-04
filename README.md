# Ride-Sharing Surge Pricing Engine

A real-time, distributed surge pricing engine designed to handle high-throughput ride requests and driver availability updates. This project demonstrates modern data engineering practices using streaming, windowed processing, and low-latency storage.

## System Architecture

The engine follows a robust microservices architecture:
**Data Generator** → **Apache Kafka** → **Apache Flink** → **Redis** → **FastAPI** → **Dashboard**

- **Data Generator**: Simulates real-time ride requests and driver telemetry using the `Faker` library.
- **Apache Kafka**: Acts as the high-throughput message backbone, ensuring event durability and ordering.
- **Apache Flink**: The core processing engine. It performs stateful windowed aggregations (10s hopping windows) to calculate live demand-to-supply ratios using Flink SQL.
- **Redis Sink**: A dedicated microservice that bridges Kafka and Redis for low-latency state persistence.
- **Redis**: Provides sub-millisecond access to current surge multipliers.
- **FastAPI**: Serves the computed surge pricing via a RESTful API.
- **Frontend Dashboard**: A professional operations console for real-time visualization of city-wide surge status.

## Key Features

- **Real-time Processing**: Sub-second price adjustments based on live streaming data.
- **Windowed Aggregation**: Uses hopping windows to provide responsive pricing updates.
- **Scalable Design**: Topics are partitioned by `zone_id` to allow horizontal scaling of the Flink cluster.
- **Dynamic Pricing**: Implementation of supply-demand equilibrium algorithms.
- **Live Dashboard**: Visual heatmap of surge multipliers across different operational zones.

## Tech Stack

- **Language**: Python 3.10+
- **Streaming**: Apache Kafka (Confluent Platform)
- **Processing**: Apache Flink (PyFlink Table API)
- **Storage**: Redis 7
- **API**: FastAPI & Uvicorn
- **Infrastructure**: Docker & Docker Compose
- **Frontend**: Vanilla HTML5, CSS3 (Enterprise Modern), JavaScript

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.10+

### Setup
1. **Clone the repository**:
   ```bash
   git clone git@github.com:ZiaMalik-wq/Ride-Sharing-Surge-Pricing.git
   cd Ride-Sharing-Surge-Pricing
   ```

2. **Initialize Python Environment**:
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```

3. **Launch Infrastructure**:
   ```bash
   docker-compose -f docker/docker-compose.yml up -d
   ```

## Running the Pipeline

To run the complete system, you need to start the following components in separate terminals:

1. **Start Data Generator**:
   ```powershell
   python src/generator/producer.py
   ```

2. **Submit Flink Job**:
   ```powershell
   docker exec -it surge_flink_jobmanager flink run -py /opt/flink/project/src/processor/processor.py
   ```

3. **Start Redis Sink**:
   ```powershell
   python src/processor/redis_sink.py
   ```

4. **Start API Service**:
   ```powershell
   uvicorn src.api.main:app --reload --port 8000
   ```

5. **View Dashboard**:
   Open `frontend/index.html` in your web browser.

## Pricing Logic

The base surge multiplier is calculated using:
`surge = max(1.0, demand / (supply + 1))`

The system captures windows of data and computes the ratio of ride requests (demand) to available drivers (supply) per zone, updating the price dynamically as new data arrives.

---
*Developed as a demonstration of production-grade real-time data engineering pipelines.*
