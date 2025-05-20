import requests
import time
import json
from pymongo import MongoClient
from kafka import KafkaProducer, KafkaConsumer
import logging
import pytest

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
KAFKA_TOPIC = 'amazon-reviews'
MONGODB_URI = 'mongodb://admin:adminpassword@mongodb:27017/'
WEB_DASHBOARD_URL = 'http://web_dashboard:5000'

class PipelineTester:
    def __init__(self):
        self.kafka_producer = None
        self.kafka_consumer = None
        self.mongo_client = None
        self.db = None

    def setup(self):
        """Initialize connections to all services."""
        try:
            # Initialize Kafka producer
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )

            # Initialize Kafka consumer
            self.kafka_consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )

            # Initialize MongoDB connection
            self.mongo_client = MongoClient(MONGODB_URI)
            self.db = self.mongo_client['amazon_reviews']

            logger.info("Successfully initialized all connections")
        except Exception as e:
            logger.error(f"Failed to initialize connections: {e}")
            raise

    def teardown(self):
        """Close all connections."""
        if self.kafka_producer:
            self.kafka_producer.close()
        if self.kafka_consumer:
            self.kafka_consumer.close()
        if self.mongo_client:
            self.mongo_client.close()

    def test_kafka_producer(self):
        """Test sending messages to Kafka."""
        test_message = {
            "review_id": "test_review_1",
            "product_id": "B00004Y2UT",
            "user_id": "test_user",
            "review_text": "This is a test review. The product is excellent!",
            "review_title": "Test Review",
            "review_date": "2024-01-01",
            "overall": 5.0
        }

        try:
            self.kafka_producer.send(KAFKA_TOPIC, value=test_message)
            self.kafka_producer.flush()
            logger.info("Successfully sent test message to Kafka")
            return True
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            return False

    def test_spark_processing(self):
        """Test if Spark is processing messages and storing in MongoDB."""
        try:
            # Wait for processing
            time.sleep(10)

            # Check MongoDB for the processed review
            result = self.db.predictions.find_one({"review_id": "test_review_1"})
            
            if result:
                logger.info("Successfully found processed review in MongoDB")
                return True
            else:
                logger.error("Processed review not found in MongoDB")
                return False
        except Exception as e:
            logger.error(f"Failed to verify Spark processing: {e}")
            return False

    def test_web_dashboard(self):
        """Test web dashboard endpoints."""
        endpoints = [
            '/api/recent-predictions',
            '/api/sentiment-trends',
            '/api/top-products'
        ]

        for endpoint in endpoints:
            try:
                response = requests.get(f"{WEB_DASHBOARD_URL}{endpoint}")
                if response.status_code == 200:
                    logger.info(f"Successfully accessed {endpoint}")
                else:
                    logger.error(f"Failed to access {endpoint}: {response.status_code}")
                    return False
            except Exception as e:
                logger.error(f"Failed to test {endpoint}: {e}")
                return False
        return True

    def test_visualizations(self):
        """Test if visualizations are working correctly."""
        try:
            # Test sentiment trends
            response = requests.get(f"{WEB_DASHBOARD_URL}/api/sentiment-trends")
            trends_data = response.json()
            
            if not trends_data:
                logger.error("No sentiment trends data available")
                return False

            # Test top products
            response = requests.get(f"{WEB_DASHBOARD_URL}/api/top-products")
            products_data = response.json()
            
            if not products_data:
                logger.error("No top products data available")
                return False

            logger.info("Successfully verified visualization data")
            return True
        except Exception as e:
            logger.error(f"Failed to test visualizations: {e}")
            return False

    def run_all_tests(self):
        """Run all pipeline tests."""
        try:
            self.setup()
            
            tests = [
                ("Kafka Producer", self.test_kafka_producer),
                ("Spark Processing", self.test_spark_processing),
                ("Web Dashboard", self.test_web_dashboard),
                ("Visualizations", self.test_visualizations)
            ]

            results = []
            for test_name, test_func in tests:
                logger.info(f"Running {test_name} test...")
                result = test_func()
                results.append((test_name, result))
                logger.info(f"{test_name} test {'passed' if result else 'failed'}")

            return all(result for _, result in results)
        finally:
            self.teardown()

def main():
    tester = PipelineTester()
    success = tester.run_all_tests()
    
    if success:
        logger.info("All pipeline tests passed successfully!")
    else:
        logger.error("Some pipeline tests failed!")
        exit(1)

if __name__ == "__main__":
    main() 