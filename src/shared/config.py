import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

class Config:
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    TOPIC_RIDE_REQUESTS = os.getenv("TOPIC_RIDE_REQUESTS", "ride_requests")
    TOPIC_DRIVER_UPDATES = os.getenv("TOPIC_DRIVER_UPDATES", "driver_updates")
    TOPIC_SURGE_PRICING = os.getenv("TOPIC_SURGE_PRICING", "surge_pricing")

    # Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    
    # Cassandra
    CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
    CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "surge_analytics")

    # API
    API_HOST = os.getenv("API_HOST", "127.0.0.1")
    API_PORT = int(os.getenv("API_PORT", "8000"))
    
    # Processing
    WINDOW_SIZE_SECONDS = int(os.getenv("WINDOW_SIZE_SECONDS", "300")) # 5 min
    WINDOW_SLIDE_SECONDS = int(os.getenv("WINDOW_SLIDE_SECONDS", "60")) # 1 min

config = Config()
