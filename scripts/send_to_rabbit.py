import pika
import sys
import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from bridge_service.proto.message_pb2 import Payload

# RabbitMQ configuration
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_QUEUE = 'proto_queue'

def send_message(id, data):
    # Create protobuf message
    payload = Payload()
    payload.id = id
    payload.data = data
    
    # Print the message before serialization
    print(f"Message before serialization: {payload}")
    
    # Serialize the message
    serialized_message = payload.SerializeToString()
    print(f"Serialized message (hex): {serialized_message.hex()}")
    print(f"Serialized message (bytes): {serialized_message}")
    
    # Connect to RabbitMQ
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    # Declare queue
    channel.queue_declare(queue=RABBITMQ_QUEUE)
    
    # Publish message
    channel.basic_publish(
        exchange='',
        routing_key=RABBITMQ_QUEUE,
        body=serialized_message
    )
    
    print(f"Sent message - ID: {id}, Data: {data}")
    
    # Close connection
    connection.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python send_to_rabbit.py <id> <data>")
        sys.exit(1)
    
    message_id = sys.argv[1]
    message_data = sys.argv[2]
    send_message(message_id, message_data) 