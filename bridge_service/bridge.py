import pika
from confluent_kafka import Producer, KafkaError
import time
import os
import logging
from pika.exceptions import AMQPConnectionError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# RabbitMQ configuration
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_QUEUE = 'proto_queue'

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = 'proto_topic'

def wait_for_rabbitmq(max_retries=30, retry_interval=2):
    """Wait for RabbitMQ to be ready"""
    for i in range(max_retries):
        try:
            credentials = pika.PlainCredentials('guest', 'guest')
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials
            )
            connection = pika.BlockingConnection(parameters)
            connection.close()
            logger.info("RabbitMQ is ready!")
            return True
        except AMQPConnectionError:
            if i < max_retries - 1:
                logger.info(f"Waiting for RabbitMQ to be ready... (attempt {i+1}/{max_retries})")
                time.sleep(retry_interval)
            else:
                logger.error("Failed to connect to RabbitMQ after maximum retries")
                return False

def setup_rabbitmq():
    """Setup RabbitMQ connection and channel"""
    try:
        credentials = pika.PlainCredentials('guest', 'guest')
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE)
        logger.info(f"Successfully connected to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
        return connection, channel
    except Exception as e:
        logger.error(f"Failed to setup RabbitMQ connection: {e}")
        raise

def setup_kafka():
    """Setup Kafka producer"""
    try:
        logger.info(f"Setting up Kafka producer with bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
        producer = Producer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'bridge-service'
        })
        
        # Test the connection
        def delivery_callback(err, msg):
            if err:
                logger.error(f'Message delivery failed: {err}')
            else:
                logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')
        
        # Send a test message
        producer.produce(
            KAFKA_TOPIC,
            value=b'test',  # Send as bytes
            callback=delivery_callback
        )
        producer.poll(1)
        producer.flush()
        
        logger.info("Successfully connected to Kafka")
        return producer
    except Exception as e:
        logger.error(f"Failed to setup Kafka producer: {e}")
        raise

def process_message(ch, method, properties, body, kafka_producer):
    """Process message from RabbitMQ and send to Kafka"""
    try:
        logger.info(f"Received message from RabbitMQ, size: {len(body)} { body} bytes")
        
            
        # Send raw bytes to Kafka
        kafka_producer.produce(
            KAFKA_TOPIC,
            value=body,  # Send raw bytes
            callback=lambda err, msg: logger.info(f'Message relayed to Kafka: {msg.topic()} [{msg.partition()}]') if err is None else logger.error(f'Failed to relay message: {err}')
        )
        kafka_producer.poll(0)
        
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("Message processed successfully")
        
    except Exception as e:
        logger.error(f"Error relaying message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag)

def main():
    logger.info("Starting bridge service...")
    logger.info(f"Environment: RABBITMQ_HOST={RABBITMQ_HOST}, RABBITMQ_PORT={RABBITMQ_PORT}, KAFKA_BOOTSTRAP_SERVERS={KAFKA_BOOTSTRAP_SERVERS}")
    
    # Wait for RabbitMQ to be ready
    if not wait_for_rabbitmq():
        logger.error("Exiting due to RabbitMQ connection failure")
        return
    
    try:
        # Setup connections
        rabbitmq_conn, rabbitmq_channel = setup_rabbitmq()
        kafka_producer = setup_kafka()
        
        logger.info("Connected to RabbitMQ and Kafka")
        
        # Start consuming
        rabbitmq_channel.basic_consume(
            queue=RABBITMQ_QUEUE,
            on_message_callback=lambda ch, method, properties, body: process_message(ch, method, properties, body, kafka_producer)
        )
        
        logger.info("Started consuming from RabbitMQ")
        rabbitmq_channel.start_consuming()
        
    except Exception as e:
        logger.error(f"Fatal error in main loop: {e}")
        raise

if __name__ == "__main__":
    main() 