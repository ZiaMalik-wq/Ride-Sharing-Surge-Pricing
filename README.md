# Ride-Sharing Surge Pricing Engine 🚀

A real-time, distributed surge pricing engine designed to handle high-throughput ride requests and driver availability updates. This project demonstrates modern data engineering practices using streaming, windowed processing, and low-latency storage.

## 🏗️ System Architecture

The engine follows a streaming-first architecture:
**Data Generator** → **Apache Kafka** → **Apache Flink** → **Redis** → **FastAPI**

- **Data Generator**: Simulates real-time ride requests and driver telemetry.
- **Apache Kafka**: Acts as the high-throughput message backbone.
- **Apache Flink**: Performs stateful windowed aggregations to calculate demand/supply.
- **Redis**: Provides sub-millisecond access to current surge multipliers.
- **FastAPI**: Serves the computed surge pricing via a RESTful API.

## ⚡ Key Features

- **Real-time Processing**: Sub-minute price adjustments based on live data.
- **Windowed Aggregation**: Uses sliding windows (30-60s) to smooth pricing volatility.
- **Scalable Design**: Partitioned by `zone_id` for horizontal scalability.
- **Dynamic Pricing**: Implementation of supply-demand equilibrium algorithms.

## 🛠️ Tech Stack

- **Language**: Python 3.10+
- **Streaming**: Apache Kafka
- **Processing**: Apache Flink (PyFlink)
- **Storage**: Redis
- **API**: FastAPI
- **Infrastructure**: Docker & Docker Compose

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.10+

### Setup (Coming Soon)
```bash
# Clone the repository
git clone git@github.com:ZiaMalik-wq/Ride-Sharing-Surge-Pricing.git
cd Ride-Sharing-Surge-Pricing

# Launch infrastructure
docker-compose up -d
```

## 📈 Pricing Logic

The base surge multiplier is calculated using:
`surge = max(1.0, demand / (supply + 1))`

---
*Developed as a demonstration of real-time data engineering pipelines.*
