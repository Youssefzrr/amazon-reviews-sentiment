from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.ml import PipelineModel
import logging
from datetime import datetime
import os
import joblib
from pymongo import MongoClient
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Download required NLTK data
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'  # Updated port

# Update the model path
MODEL_PATH = '/app/data/models/sentiment_model'  # This matches the Docker volume mount

class ReviewProcessor:
    def __init__(self):
        self.stop_words = set(stopwords.words('english'))
        self.lemmatizer = WordNetLemmatizer()
        self.sentiment_model = None
        self.tfidf_model = None
        self.mongo_client = None
        self.db = None
        
    def load_models(self, spark):
        """Load the Spark MLlib models."""
        try:
            # Load the sentiment model
            self.sentiment_model = PipelineModel.load(MODEL_PATH)
            
            # Load the TF-IDF model
            self.tfidf_model = PipelineModel.load('/models/tfidf_model')
            
            logger.info("Successfully loaded Spark MLlib models")
        except Exception as e:
            logger.error(f"Failed to load models: {e}")
            raise

    def connect_mongodb(self):
        """Connect to MongoDB."""
        try:
            self.mongo_client = MongoClient('mongodb://mongodb:27017/')
            self.db = self.mongo_client['amazon_reviews']
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def preprocess_text(self, text):
        """Apply the same preprocessing steps used during training."""
        if not isinstance(text, str):
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove special characters and digits
        text = re.sub(r'[^a-zA-Z\s]', '', text)
        
        # Tokenize
        tokens = nltk.word_tokenize(text)
        
        # Remove stopwords and lemmatize
        tokens = [self.lemmatizer.lemmatize(token) for token in tokens 
                 if token not in self.stop_words]
        
        return ' '.join(tokens)

    def predict_sentiment(self, spark, text):
        """Predict sentiment using Spark MLlib models."""
        try:
            # Preprocess the text
            processed_text = self.preprocess_text(text)
            
            # Create a DataFrame with the processed text
            text_df = spark.createDataFrame([(processed_text,)], ["text"])
            
            # Apply TF-IDF transformation
            tfidf_features = self.tfidf_model.transform(text_df)
            
            # Predict sentiment
            prediction = self.sentiment_model.transform(tfidf_features)
            
            # Extract prediction and probability
            result = prediction.select("prediction", "probability").first()
            
            return {
                'sentiment': int(result.prediction),
                'confidence': float(max(result.probability))
            }
        except Exception as e:
            logger.error(f"Error in sentiment prediction: {e}")
            return {'sentiment': -1, 'confidence': 0.0}

    def store_in_mongodb(self, review_data, prediction):
        """Store the review and prediction in MongoDB."""
        try:
            document = {
                'review_id': review_data.get('review_id', ''),
                'product_id': review_data.get('product_id', ''),
                'user_id': review_data.get('user_id', ''),
                'review_text': review_data.get('review_text', ''),
                'review_title': review_data.get('review_title', ''),
                'review_date': review_data.get('review_date', ''),
                'kafka_timestamp': review_data.get('kafka_timestamp', ''),
                'processed_timestamp': datetime.now().isoformat(),
                'sentiment': prediction['sentiment'],
                'confidence': prediction['confidence']
            }
            
            self.db.predictions.insert_one(document)
            logger.info(f"Stored review {document['review_id']} in MongoDB")
        except Exception as e:
            logger.error(f"Failed to store in MongoDB: {e}")

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("AmazonReviewsSentimentAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .getOrCreate()
    
    # Initialize processor
    processor = ReviewProcessor()
    
    try:
        # Load models
        processor.load_models(spark)
        
        # Connect to MongoDB
        processor.connect_mongodb()
        
        # Define schema for Kafka messages
        schema = StructType([
            StructField("review_id", StringType()),
            StructField("product_id", StringType()),
            StructField("user_id", StringType()),
            StructField("review_text", StringType()),
            StructField("review_title", StringType()),
            StructField("review_date", StringType()),
            StructField("kafka_timestamp", StringType())
        ])
        
        # Read from Kafka
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", "amazon-reviews") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON from Kafka
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Process the stream
        def process_batch(batch_df, batch_id):
            # Convert to pandas for processing
            pandas_df = batch_df.toPandas()
            
            # Process each review
            for _, row in pandas_df.iterrows():
                review_data = row.to_dict()
                prediction = processor.predict_sentiment(spark, review_data['review_text'])
                processor.store_in_mongodb(review_data, prediction)
        
        # Start the streaming query
        query = parsed_df.writeStream \
            .foreachBatch(process_batch) \
            .outputMode("append") \
            .start()
        
        # Wait for the streaming to finish
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if 'processor' in locals() and processor.mongo_client:
            processor.mongo_client.close()
        spark.stop()

if __name__ == "__main__":
    main()