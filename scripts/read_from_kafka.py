from confluent_kafka import Consumer, KafkaError
import os
import time
from google.protobuf.json_format import MessageToJson
import sys
from pathlib import Path

# Add the project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from bridge_service.proto.message_pb2 import Payload

# Print all environment variables for debugging
print("Environment variables:")
for key, value in os.environ.items():
    if 'KAFKA' in key:
        print(f"{key}: {value}")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Use localhost since we're running on host machine
KAFKA_TOPIC = 'proto_topic'
KAFKA_GROUP_ID = 'proto_consumer_group'

def setup_consumer():
    """Setup Kafka consumer"""
    return Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })

def process_message(msg):
    """Process a Kafka message"""
    try:
        # Parse the protobuf message
        proto_message = Payload()
        proto_message.ParseFromString(msg.value())
        
        # Convert to JSON for display
        json_message = MessageToJson(proto_message)
        print(f"Received message: {json_message}")
        
    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    print("\nStarting Kafka consumer...")
    print(f"Using Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    
    # Setup consumer
    consumer = setup_consumer()
    consumer.subscribe([KAFKA_TOPIC])
    
    print(f"Subscribed to topic: {KAFKA_TOPIC}")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Error: {msg.error()}")
            else:
                process_message(msg)
                
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main() 