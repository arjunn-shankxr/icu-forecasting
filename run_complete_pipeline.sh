#!/bin/bash

echo "======================================"
echo "ICU FORECASTING - COMPLETE PIPELINE"
echo "With Kafka → Hadoop → Spark → ML"
echo "======================================"
echo

# Check if processed data exists
if [ ! -f "processed_icu_data.csv" ]; then
    echo "Step 1: Processing MIMIC data..."
    python src/data_loader.py
    echo
fi

# Start Docker services
echo "Step 2: Starting Kafka and Hadoop..."
docker-compose up -d

echo "Waiting for services to start (30 seconds)..."
sleep 30

# Check services
echo
echo "Checking services:"
docker ps --format "table {{.Names}}\t{{.Status}}"
echo

# Create Kafka topic
echo "Step 3: Creating Kafka topic..."
docker exec -it kafka kafka-topics --create \
    --topic icu-census \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1 \
    2>/dev/null || echo "Topic already exists"
echo

# Start Kafka producer in background
echo "Step 4: Starting Kafka producer (streaming data)..."
python src/kafka_producer.py &
PRODUCER_PID=$!
echo "Producer PID: $PRODUCER_PID"
echo

# Start Kafka to Hadoop consumer in background
echo "Step 5: Starting Kafka to Hadoop consumer..."
python src/kafka_to_hadoop.py &
CONSUMER_PID=$!
echo "Consumer PID: $CONSUMER_PID"
echo

# Wait for some data to accumulate
echo "Waiting for data to flow through pipeline (20 seconds)..."
sleep 20

# Process with Spark
echo
echo "Step 6: Processing data with Spark..."
python src/spark_processor.py
echo

# Train ML model
echo "Step 7: Training ML model..."
python src/ml_models.py
echo

# Kill background processes
echo "Stopping streaming processes..."
kill $PRODUCER_PID 2>/dev/null
kill $CONSUMER_PID 2>/dev/null

echo
echo "======================================"
echo "PIPELINE COMPLETE!"
echo "======================================"
echo
echo "You can now:"
echo "1. View Hadoop files at: http://localhost:9870"
echo "2. Launch dashboard: streamlit run src/dashboard.py"
echo "3. Stop services: docker-compose down"
echo