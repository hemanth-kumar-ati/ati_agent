FROM python:3.9-slim

WORKDIR /app

# Install protobuf compiler
RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the bridge service code
COPY bridge.py .

# Run the bridge service
CMD ["python", "bridge.py"] 