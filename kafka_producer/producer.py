#!/usr/bin/env python3
"""
Kafka Producer for Amazon Reviews Data
This script reads Amazon reviews data and sends it to a Kafka topic.
"""

import json
import time
import pandas as pd
import argparse
from kafka import KafkaProducer
from datetime import datetime


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Kafka Producer for Amazon Reviews')
    parser.add_argument('--data-path', type=str, default='/data/raw/data.json',
                        help='Path to the Amazon reviews data file')
    parser.add_argument('--bootstrap-servers', type=str, default='kafka:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, default='amazon-reviews',
                        help='Kafka topic to send messages to')
    parser.add_argument('--delay', type=float, default=0.1,
                        help='Delay between messages in seconds')
    parser.add_argument('--test-mode', action='store_true',
                        help='Use 10% of data for testing')
    return parser.parse_args()


def create_producer(bootstrap_servers):
    """Create and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 1)
    )


def stream_data(producer, data_df, topic, delay, test_mode=False):
    """Stream data to Kafka topic."""
    if test_mode:
        # Use 10% of data for testing
        data_df = data_df.sample(frac=0.1)

    print(f"Streaming {len(data_df)} reviews to topic '{topic}'...")

    for index, review in data_df.iterrows():
        # Convert review to dictionary
        review_dict = review.to_dict()

        # Add timestamp for real-time visualization
        review_dict['stream_timestamp'] = datetime.now().isoformat()

        # Send to Kafka
        producer.send(topic, review_dict)

        if index % 100 == 0:
            print(f"Sent {index} messages to Kafka")

        # Simulate real-time streaming with delay
        time.sleep(delay)

    producer.flush()
    print("Finished streaming data.")


def main():
    """Main function."""
    args = parse_arguments()

    try:
        # Load Amazon reviews data
        print(f"Loading data from {args.data_path}...")
        reviews_df = pd.read_json(args.data_path, lines=True)
        print(f"Loaded {len(reviews_df)} reviews.")

        # Create Kafka producer
        producer = create_producer(args.bootstrap_servers)

        # Stream data to Kafka
        stream_data(producer, reviews_df, args.topic, args.delay, args.test_mode)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'producer' in locals():
            producer.close()


if __name__ == "__main__":
    main()