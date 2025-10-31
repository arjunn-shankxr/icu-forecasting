from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
import sys

class KafkaHadoopPipeline:
    """Consume from Kafka and store in Hadoop/Local filesystem"""
    
    def __init__(self):
        """Initialize Kafka consumer and storage"""
        
        # Initialize Kafka consumer
        try:
            self.consumer = KafkaConsumer(
                'icu-census',
                bootstrap_servers=['localhost:9092'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='hadoop-consumer'
            )
            print("✓ Connected to Kafka consumer")
        except Exception as e:
            print(f"✗ Failed to connect to Kafka: {e}")
            sys.exit(1)
        
        # Try HDFS but don't fail if not available
        self.use_hdfs = False
        try:
            from hdfs import InsecureClient
            self.hdfs_client = InsecureClient('http://localhost:9870', user='root')
            # Test connection
            self.hdfs_client.list('/')
            print("✓ Connected to Hadoop HDFS")
            self.use_hdfs = True
        except Exception as e:
            print(f"ℹ HDFS not available (this is OK on Windows)")
            print("  Will save to local filesystem instead")
            self.use_hdfs = False
        
        # Create local output directory
        self.local_output_dir = 'data/hadoop_output'
        os.makedirs(self.local_output_dir, exist_ok=True)
        print(f"✓ Local output directory ready: {self.local_output_dir}")
        
        self.buffer = []
        self.buffer_size = 10  # Flush every 10 records
        
    def consume_and_store(self):
        """Consume messages from Kafka and store them"""
        print("\n📡 Starting to consume messages...")
        print("Press Ctrl+C to stop\n")
        
        message_count = 0
        
        try:
            for message in self.consumer:
                # Extract data
                data = message.value
                data['kafka_offset'] = message.offset
                data['kafka_partition'] = message.partition
                
                # Add to buffer
                self.buffer.append(data)
                message_count += 1
                
                print(f"✓ Received message {message_count}: ICU Count = {data['icu_count']}")
                
                # Flush buffer when full
                if len(self.buffer) >= self.buffer_size:
                    self.flush_buffer()
                    
        except KeyboardInterrupt:
            print("\n\n⏹ Stopping consumer...")
            # Flush remaining data
            if self.buffer:
                self.flush_buffer()
            print(f"✓ Processed {message_count} messages total")
            print(f"✓ Data saved to: {self.local_output_dir}")
    
    def flush_buffer(self):
        """Write buffered data to storage"""
        if not self.buffer:
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(self.buffer)
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Always save locally (works everywhere)
        self.save_locally(df, timestamp)
        
        # Try HDFS if available
        if self.use_hdfs:
            self.try_hdfs_save(df, timestamp)
        
        # Clear buffer
        self.buffer = []
    
    def save_locally(self, df, timestamp):
        """Save data locally"""
        local_path = os.path.join(self.local_output_dir, f'batch_{timestamp}.csv')
        df.to_csv(local_path, index=False)
        print(f"  💾 Saved {len(df)} records locally: {local_path}")
    
    def try_hdfs_save(self, df, timestamp):
        """Try to save to HDFS (optional)"""
        try:
            hdfs_path = f'/icu_data/batch_{timestamp}.csv'
            
            # Create directory if not exists
            try:
                self.hdfs_client.makedirs('/icu_data')
            except:
                pass
            
            # Write to HDFS
            with self.hdfs_client.write(hdfs_path, encoding='utf-8') as writer:
                df.to_csv(writer, index=False)
            
            print(f"  ☁️ Also saved to HDFS: {hdfs_path}")
        except Exception as e:
            # Don't fail - HDFS is optional
            pass

if __name__ == "__main__":
    print("="*60)
    print("KAFKA TO STORAGE PIPELINE")
    print("="*60)
    
    pipeline = KafkaHadoopPipeline()
    pipeline.consume_and_store()