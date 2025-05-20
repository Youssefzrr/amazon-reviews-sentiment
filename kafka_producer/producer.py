from confluent_kafka import Producer
import json
import time
import pandas as pd
from pyspark.sql import SparkSession
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
KAFKA_TOPIC = 'amazon-reviews'
TEST_DATA_PATH = '/data/processed/test_data.parquet'

def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def create_kafka_producer():
    """Create and return a Kafka producer instance."""
    try:
        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'amazon-reviews-producer',
            'acks': 'all',
            'retries': 3
        }
        return Producer(conf)
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise

def read_parquet_file(file_path):
    """Read the parquet file using Spark and convert to pandas DataFrame."""
    try:
        spark = SparkSession.builder \
            .appName("Kafka Producer - Amazon Reviews") \
            .getOrCreate()
        
        df = spark.read.parquet(file_path)
        pandas_df = df.toPandas()
        spark.stop()
        return pandas_df
    except Exception as e:
        logger.error(f"Failed to read parquet file: {e}")
        raise

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
            # Create message as JSON
            message = {
                'reviewerID': row.get('reviewerID', ''),
                'asin': row.get('asin', ''),
                'reviewerName': row.get('reviewerName', ''),
                'reviewText': row.get('reviewText', ''),
                'overall': float(row.get('overall', 0.0)),
                'summary': row.get('summary', ''),
                'unixReviewTime': int(row.get('unixReviewTime', 0)),
                'reviewTime': row.get('reviewTime', ''),
                'kafka_timestamp': datetime.now().isoformat()
            }
            
            # Convert to JSON string
            message_json = json.dumps(message)
            
            try:
                # Send to Kafka
                producer.produce(
                    topic_name,
                    message_json.encode('utf-8'),
                    callback=delivery_report
                )
                
                # Trigger any available delivery callbacks
                producer.poll(0)
                
            except Exception as e:
                logger.error(f"Failed to send record to Kafka: {e}")
        
        # Flush after each batch to ensure messages are sent
        producer.flush()
        
        logger.info(f"Sent batch of {len(batch)} records. Progress: {min(i+batch_size, total_records)}/{total_records}")
        
        # Add delay between batches to control the rate
        time.sleep(delay)

def main():
    try:
        # Create Kafka producer
        producer = create_kafka_producer()
        
        # Read parquet file
        df = read_parquet_file(TEST_DATA_PATH)
        
        # Send reviews to Kafka
        send_reviews_to_kafka(
            producer=producer,
            df=df,
            topic_name=KAFKA_TOPIC,
            batch_size=100,
            delay=0.1
        )
        
        logger.info("Successfully completed sending all reviews to Kafka")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if 'producer' in locals():
            producer.flush()

if __name__ == "__main__":
    main()