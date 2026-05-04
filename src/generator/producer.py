import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

# Configuration
# Using localhost:9092 because this script runs on the host machine
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_RIDE_REQUESTS = 'ride_requests'
TOPIC_DRIVER_UPDATES = 'driver_updates'
ZONES = ['zone_1', 'zone_2', 'zone_3', 'zone_4', 'zone_5']

# Kafka Producer Configuration
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'surge-pricing-generator'
}

producer = Producer(conf)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def generate_ride_request():
    return {
        'timestamp': datetime.now().isoformat(),
        'zone_id': random.choice(ZONES),
        'event_type': 'ride_request',
        'user_id': fake.uuid4()
    }

def generate_driver_update():
    return {
        'timestamp': datetime.now().isoformat(),
        'zone_id': random.choice(ZONES),
        'event_type': 'driver_available',
        'driver_id': fake.uuid4()
    }

if __name__ == '__main__':
    print(f"Starting data simulation... Connecting to {KAFKA_BOOTSTRAP_SERVERS}")
    print("Topics: ride_requests, driver_updates")
    print("Press Ctrl+C to stop.")
    
    try:
        while True:
            # Decide which event to produce (roughly equal distribution)
            if random.random() > 0.5:
                event = generate_ride_request()
                topic = TOPIC_RIDE_REQUESTS
            else:
                event = generate_driver_update()
                topic = TOPIC_DRIVER_UPDATES
            
            # Keying by zone_id ensures all events for a zone go to the same partition
            # This is important for stateful processing in Flink later
            producer.produce(
                topic, 
                key=event['zone_id'], 
                value=json.dumps(event), 
                callback=delivery_report
            )
            
            # Serve delivery reports (callbacks) from previous produce() calls
            producer.poll(0)
            
            # Wait 1-2 seconds as per requirements
            wait_time = random.uniform(1, 2)
            time.sleep(wait_time)
            
    except KeyboardInterrupt:
        print("\nStopping simulation...")
    finally:
        # Wait for any outstanding messages to be delivered and delivery reports received
        producer.flush()
        print("Simulation stopped.")
