import pandas as pd
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import sys

class MIMICKafkaProducer:
    """Stream MIMIC-IV data to Kafka in real-time simulation"""
    
    def __init__(self):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
            )
            print("✓ Connected to Kafka")
        except Exception as e:
            print(f"✗ Failed to connect to Kafka: {e}")
            print("Make sure Kafka is running: docker-compose up -d")
            sys.exit(1)
    
    def stream_icu_data(self, data_file='processed_icu_data.csv'):
        """Stream ICU data to Kafka"""
        print(f"\nStreaming data from {data_file} to Kafka...")
        
        try:
            # Load processed data
            df = pd.read_csv(data_file)
            print(f"Loaded {len(df)} records")
            
            # Stream each row
            for idx, row in df.iterrows():
                # Create message
                message = {
                    'timestamp': datetime.now().isoformat(),
                    'hour': int(row['hour']),
                    'day_of_week': int(row['day_of_week']),
                    'icu_count': int(row['icu_count']),
                    'record_id': idx
                }
                
                # Send to Kafka topic
                self.producer.send('icu-census', value=message)
                
                print(f"Sent record {idx+1}/{len(df)}: ICU Count = {row['icu_count']}")
                
                # Simulate real-time streaming (1 record per second)
                time.sleep(1)
            
            # Ensure all messages are sent
            self.producer.flush()
            print("\n✓ Finished streaming all data to Kafka")
            
        except FileNotFoundError:
            print(f"✗ File {data_file} not found. Run data_loader.py first!")
        except Exception as e:
            print(f"✗ Error streaming data: {e}")

if __name__ == "__main__":
    producer = MIMICKafkaProducer()
    producer.stream_icu_data()