# 🏥 ICU Patient Load Forecasting System

A real-time ICU bed occupancy forecasting system using MIMIC-IV dataset, Apache Kafka, Hadoop, Spark, and Machine Learning.

## 📊 Project Overview

This system predicts ICU bed requirements by:
- Processing historical patient data from MIMIC-IV dataset
- Streaming data through Apache Kafka for real-time processing
- Storing in Hadoop/HDFS for distributed storage
- Processing with Apache Spark for feature engineering
- Training ML models for accurate predictions (MAE: 2.20 patients)
- Visualizing results in an interactive dashboard

**Model Performance:** 91.4% accuracy in predicting ICU occupancy

## 🏗️ Architecture

MIMIC-IV Dataset → Kafka Streaming → Hadoop Storage → Spark Processing → ML Models → Dashboard


## 🚀 Quick Start Guide

### 1️⃣ Clone Repository

git clone https://github.com/arjunn-shankxr/icu-forecasting.git
cd icu-forecasting

# Create virtual environment
python -m venv venv

# Activate environment
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start Kafka and Hadoop
docker-compose up -d

# Verify services are running
docker ps

# Process MIMIC data
python src/data_loader.py

python src/kafka_producer.py

python src/kafka_to_hadoop.py

python src/spark_processor.py

# Train ML model
python src/ml_models.py

# Launch dashboard
streamlit run src/dashboard.py

# File Structure
icu-forecasting/
├── src/
│   ├── data_loader.py         # Process MIMIC-IV data
│   ├── kafka_producer.py      # Stream data to Kafka
│   ├── kafka_to_hadoop.py     # Store streamed data
│   ├── spark_processor.py     # Spark processing
│   ├── ml_models.py           # Train forecasting models
│   └── dashboard.py           # Streamlit dashboard
├── data/
│   └── mimic-iv/              # MIMIC dataset (add yourself)
├── docker-compose.yml         # Docker services config
├── requirements.txt           # Python dependencies
├── README.md                  # This file
└── .gitignore                # Git ignore rules
