from kafka import KafkaProducer
import json
import time
import pandas as pd
import numpy as np
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder for numpy types"""
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        return super(NumpyEncoder, self).default(obj)

def create_kafka_producer():
    """Create and return a Kafka producer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda x: json.dumps(x, cls=NumpyEncoder).encode('utf-8'),
            acks='all',
            retries=3,
            request_timeout_ms=30000,
            api_version_auto_timeout_ms=30000
        )
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise

def read_parquet_file(file_path):
    """Read the parquet file and return a pandas DataFrame."""
    try:
        df = pd.read_parquet(file_path)
        return df
    except Exception as e:
        logger.error(f"Failed to read parquet file: {e}")
        raise

def convert_numpy_types(record):
    """Convert numpy types to Python native types."""
    converted = {}
    for key, value in record.items():
        if isinstance(value, np.ndarray):
            converted[key] = value.tolist()
        elif isinstance(value, np.integer):
            converted[key] = int(value)
        elif isinstance(value, np.floating):
            converted[key] = float(value)
        elif isinstance(value, pd.Timestamp):
            converted[key] = value.isoformat()
        else:
            converted[key] = value
    return converted

def send_reviews_to_kafka(producer, df, topic_name, batch_size=100, delay=0.1):
    """
    Send reviews to Kafka topic with rate limiting.
    
    Args:
        producer: Kafka producer instance
        df: DataFrame containing reviews
        topic_name: Name of the Kafka topic
        batch_size: Number of records to process in each batch
        delay: Delay between batches in seconds
    """
    total_records = len(df)
    logger.info(f"Starting to send {total_records} reviews to Kafka topic: {topic_name}")
    
    for i in range(0, total_records, batch_size):
        batch = df.iloc[i:i+batch_size]
        
        for _, row in batch.iterrows():
            record = row.to_dict()
            # Add timestamp
            record['kafka_timestamp'] = datetime.now().isoformat()
            
            # Debug: Print the record structure
            print(f"Sending record: {record}")
            
            try:
                producer.send(topic_name, value=record)
            except Exception as e:
                logger.error(f"Failed to send record to Kafka: {e}")
        
        # Flush after each batch to ensure messages are sent
        producer.flush()
        
        logger.info(f"Sent batch of {len(batch)} records. Progress: {min(i+batch_size, total_records)}/{total_records}")
        
        # Add delay between batches to control the rate
        time.sleep(delay)

def main():
    # Configuration
    KAFKA_TOPIC = "amazon-reviews"
    PARQUET_FILE = "/data/processed/test_data.parquet"
    BATCH_SIZE = 4
    DELAY = 1  # 1000ms delay between batches
    
    try:
        # Create Kafka producer
        producer = create_kafka_producer()
        
        # Read parquet file
        df = read_parquet_file(PARQUET_FILE)
        
        # Send reviews to Kafka
        send_reviews_to_kafka(
            producer=producer,
            df=df,
            topic_name=KAFKA_TOPIC,
            batch_size=BATCH_SIZE,
            delay=DELAY
        )
        
        logger.info("Successfully completed sending all reviews to Kafka")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if 'producer' in locals():
            producer.close()

if __name__ == "__main__":
    main()