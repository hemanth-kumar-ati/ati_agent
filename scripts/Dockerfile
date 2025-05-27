FROM python:3.9-slim

WORKDIR /app

# Install protobuf compiler
RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the proto file and generate Python code
COPY bridge_service/proto/message.proto .
RUN protoc --python_out=. message.proto

# Copy the consumer script
COPY read_from_kafka.py .

# Run the consumer
CMD ["python", "read_from_kafka.py"] 