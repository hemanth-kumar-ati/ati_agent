import csv
import zlib
import pika
import sys
from pathlib import Path
import time

# Add the project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from bridge_service.proto.sensor_pb2 import SensorData, SensorDataList

def read_csv(file_path):
    """Read CSV file and return list of dictionaries"""
    sensors = []
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sensors.append(row)
    return sensors

def create_proto_message(sensors):
    """Convert list of dictionaries to protobuf message"""
    sensor_list = SensorDataList()
    for sensor in sensors:
        sensor_data = sensor_list.sensors.add()
        sensor_data.sensor_id = sensor['sensor_id']
        sensor_data.sensor_type = sensor['sensor_type']
        sensor_data.value = float(sensor['value'])
        sensor_data.unit = sensor['unit']
        sensor_data.timestamp = int(sensor['timestamp'])
        sensor_data.location = sensor['location']
    return sensor_list

def compress_message(message):
    """Compress protobuf message using zlib"""
    serialized = message.SerializeToString()
    return zlib.compress(serialized)

def setup_rabbitmq():
    """Setup RabbitMQ connection and channel"""
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters(
        host='localhost',
        port=5672,
        credentials=credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    # Declare the queue
    channel.queue_declare(queue='sensor_queue', durable=True)
    return connection, channel

def main():
    # Read and process CSV
    csv_path = Path(project_root) / 'data' / 'sample.csv'
    print(f"Reading CSV from: {csv_path}")
    
    sensors = read_csv(csv_path)
    print(f"Read {len(sensors)} sensor readings from CSV")
    
    # Convert to protobuf
    proto_message = create_proto_message(sensors)
    print("Converted to protobuf message")
    
    # Compress
    compressed_data = compress_message(proto_message)
    print(f"Compressed size: {len(compressed_data)} bytes")
    
    # Setup RabbitMQ connection
    print("Connecting to RabbitMQ...")
    connection, channel = setup_rabbitmq()
    
    try:
        # Send to RabbitMQ
        channel.basic_publish(
            exchange='',
            routing_key='sensor_queue',
            body=compressed_data,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                content_type='application/x-protobuf',
                content_encoding='zlib'
            )
        )
        print("Message sent to RabbitMQ")
        
    finally:
        # Close the connection
        connection.close()
        print("RabbitMQ connection closed")

if __name__ == "__main__":
    main() 