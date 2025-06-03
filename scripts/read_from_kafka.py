from confluent_kafka import Consumer, KafkaError
import os
import time
import zlib
import csv
from pathlib import Path
import sys
from datetime import datetime

# Add the project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from bridge_service.proto.metrics_pb2 import MetricData, MetricDataBatch

# Print all environment variables for debugging
print("Environment variables:")
for key, value in os.environ.items():
    if 'KAFKA' in key:
        print(f"{key}: {value}")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Use localhost since we're running on host machine
KAFKA_TOPIC = 'sensor_metrics'
KAFKA_GROUP_ID = 'sensor_consumer_group'

def setup_consumer():
    """Setup Kafka consumer"""
    return Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })

def decompress_message(compressed_data):
    """Decompress message using zlib"""
    return zlib.decompress(compressed_data)

def write_to_csv(sensors, output_path):
    """Write sensor data to CSV file"""
    fieldnames = ['timestamp', 'value', 'source_id']
    print("writing to csv")
    print(sensors)
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        for sensor in sensors:
            writer.writerow({
                'timestamp': sensor.timestamp,
                'value': sensor.value,
                'source_id': sensor.source_id
            })

def process_message(msg):
    """Process a Kafka message"""
    try:
        # Decompress the message
        decompressed_data = decompress_message(msg.value())
        print(f"Decompressed size: {len(decompressed_data)} bytes")
        
        # Parse the protobuf message
        sensor_list = MetricDataBatch()
        sensor_list.ParseFromString(decompressed_data)
        
        # Print sensor data
        print("\nReceived sensor data:")
        # Write to CSV
        output_path = Path(project_root) / 'data' / 'sensor_output.csv'
        print(sensor_list)
        write_to_csv(sensor_list.metrics, output_path)
        print(f"Wrote {len(sensor_list.metrics)} sensor readings to {output_path}")
        
        # Commit the offset after successful processing
        return True
        
    except Exception as e:
        print(f"Error processing message: {e}")
        return False

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
                # Process message and commit if successful
                if process_message(msg):
                    consumer.commit(msg)
                
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main() 