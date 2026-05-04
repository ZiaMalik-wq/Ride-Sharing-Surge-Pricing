# Ride-Sharing Surge Pricing Engine

## 1. Project Overview

This project aims to build a real-time surge pricing engine similar to those used by ride-sharing platforms like Uber and Lyft. The system dynamically adjusts ride prices based on real-time supply and demand imbalances across different city zones.

## 2. Objective

The goal is to:

* Simulate real-time ride requests and driver availability
* Process streaming data using distributed systems
* Compute surge pricing dynamically
* Expose results through an API

---

## 3. System Architecture

Data Generator → Kafka → Flink → Redis → FastAPI

---

## 4. Functional Requirements

### 4.1 Data Simulation

* Generate events every 1–2 seconds
* Event types:

  * Ride request
  * Driver availability
* Each event should contain:

  * timestamp
  * zone_id
  * event_type
  * user_id / driver_id

### 4.2 Kafka (Streaming Layer)

* Topics:

  * ride_requests
  * driver_updates
* Partition by zone_id
* Acts as real-time data pipeline

### 4.3 Flink (Processing Layer)

* Consume data from Kafka
* Apply window processing (30–60 seconds)
* Compute:

  * Demand = number of ride requests
  * Supply = number of drivers
* Calculate surge multiplier

### 4.4 Storage Layer (Redis)

* Store latest surge value per zone
* Key format:
  surge:<zone_id>

### 4.5 API Layer (FastAPI)

* Endpoint:
  GET /surge/{zone_id}
* Returns:

  * zone_id
  * surge multiplier
  * timestamp

---

## 5. Surge Pricing Logic

Basic formula:

surge = max(1.0, demand / (supply + 1))

Optional enhancement:

* Cap surge at 3.0
* Smooth fluctuations using moving average

---

## 6. Tech Stack

* Python
* Apache Kafka
* Apache Flink
* Redis
* FastAPI
* Faker (data simulation)
* Docker (optional)

---

## 7. Data Flow

1. Data generator produces ride and driver events
2. Events are sent to Kafka topics
3. Flink consumes events from Kafka
4. Flink aggregates data per zone using time windows
5. Surge pricing is computed
6. Results are stored in Redis
7. FastAPI serves surge values via API

---

## 8. Project Timeline (7 Days)

### Day 1: Setup

* Install Kafka and dependencies
* Create topics

### Day 2: Data Generator

* Build Python producer
* Send simulated data to Kafka

### Day 3: Flink Setup

* Configure Flink
* Consume Kafka data

### Day 4: Processing Logic

* Implement windowing
* Compute demand and supply
* Calculate surge

### Day 5: Storage

* Setup Redis
* Store surge values

### Day 6: API

* Build FastAPI service
* Fetch data from Redis

### Day 7: Testing

* End-to-end integration
* Debugging and improvements

---

## 9. Deliverables

* Working codebase
* API endpoints
* Architecture diagram
* Documentation

---

## 10. Future Enhancements

* Dashboard visualization
* Machine learning-based surge prediction
* Geospatial clustering
* Historical analytics using Cassandra

---

## 11. Conclusion

This project demonstrates real-time data engineering concepts including streaming, distributed processing, and dynamic pricing systems. It provides hands-on experience with industry-grade tools and architectures.
