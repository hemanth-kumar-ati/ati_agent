from confluent_kafka import Consumer, KafkaError
import os
import time
import zlib
import csv
from pathlib import Path
import sys

# Add the project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from bridge_service.proto.employee_pb2 import EmployeeList

# Print all environment variables for debugging
print("Environment variables:")
for key, value in os.environ.items():
    if 'KAFKA' in key:
        print(f"{key}: {value}")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Use localhost since we're running on host machine
KAFKA_TOPIC = 'employee_topic'
KAFKA_GROUP_ID = 'employee_consumer_group'

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

def write_to_csv(employees, output_path):
    """Write employee data to CSV file"""
    fieldnames = ['id', 'name', 'age', 'city', 'salary']
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for emp in employees:
            writer.writerow({
                'id': emp.id,
                'name': emp.name,
                'age': emp.age,
                'city': emp.city,
                'salary': emp.salary
            })

def process_message(msg):
    """Process a Kafka message"""
    try:
        # Decompress the message
        decompressed_data = decompress_message(msg.value())
        print(f"Decompressed size: {len(decompressed_data)} bytes")
        
        # Parse the protobuf message
        employee_list = EmployeeList()
        employee_list.ParseFromString(decompressed_data)
        
        # Write to CSV
        output_path = Path(project_root) / 'data' / 'output.csv'
        write_to_csv(employee_list.employees, output_path)
        print(f"Wrote {len(employee_list.employees)} employees to {output_path}")
        
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