import json
import uuid
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
from faker import Faker
from time import sleep
from datetime import datetime
import random
import os

# Load environment variables from .env file
dotenv_path = Path("/opt/app/.env")  
load_dotenv(dotenv_path=dotenv_path)

# Get topic from environment variables
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_block_ms=300000 
)

# Initialize Faker
faker = Faker()

def generate_purchase_event():
    """Generate a fake purchasing event using Faker."""
    event = {
        "event_id": str(uuid.uuid4()),
        "customer_id": faker.random_int(min=1, max=500),
        "item": faker.random_element(elements=("Laptop", "Phone", "Headphones", "Monitor")),
        "price": round(random.uniform(50.0, 1000.0), 2),
        "quantity": faker.random_int(min=1, max=5),
        "event_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    return event

# Continuously produce purchasing events
while True:
    event = generate_purchase_event()
    print(f"Producing Event: {event}", flush=True)
    producer.send(kafka_topic, value=event)
    producer.flush()
    sleep(3)  # Producing an event every 3 second
