from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, ArrayType, IntegerType, LongType, IntegerType
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
            
            # Update TF-IDF model path to match the same directory as sentiment model
            self.tfidf_model = PipelineModel.load('/app/data/models/tfidf_model')
            
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
        
        # Remove special characters and digits - match your notebook preprocessing
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
            logger.info(f"Preprocessed text: {processed_text[:100]}...")
            
            # Create a DataFrame with the processed text
            text_df = spark.createDataFrame([(processed_text,)], ["reviewText"])
            logger.info(f"Created DataFrame with schema: {text_df.schema}")
            
            # Apply TF-IDF transformation
            tfidf_features = self.tfidf_model.transform(text_df)
            logger.info("Applied TF-IDF transformation")
            
            # Drop any intermediate columns that might cause conflicts
            columns_to_drop = ["words", "rawFeatures", "filtered_words", "features", "rawPrediction", "raw_features"]
            for col in columns_to_drop:
                if col in tfidf_features.columns:
                    tfidf_features = tfidf_features.drop(col)
            
            # Select only the features column for prediction
            if "features" in tfidf_features.columns:
                tfidf_features = tfidf_features.select("features")
            
            # Predict sentiment
            prediction = self.sentiment_model.transform(tfidf_features)
            logger.info("Applied sentiment model")
            
            # Extract prediction and probability
            result = prediction.select("prediction", "probability").first()
            logger.info(f"Prediction result: {result}")
            
            # Get the prediction and probability values
            pred_value = int(result.prediction)
            prob_values = result.probability.toArray()
            max_prob = float(max(prob_values))
            
            # Map prediction to sentiment label
            if pred_value == 0:
                sentiment_label = "negative"
            elif pred_value == 1:
                sentiment_label = "neutral"
            elif pred_value == 2:
                sentiment_label = "positive"
            else:
                sentiment_label = "unknown"
                logger.warning(f"Unexpected prediction value: {pred_value}")
            
            logger.info(f"Mapped prediction {pred_value} to label: {sentiment_label} with confidence: {max_prob}")
            
            return {
                'label': sentiment_label,
                'confidence': max_prob
            }
        except Exception as e:
            logger.error(f"Error in sentiment prediction: {e}")
            logger.error(f"Input text: {text[:100]}...")
            # Add more detailed error logging
            if 'tfidf_features' in locals():
                logger.error(f"Available columns after TF-IDF: {tfidf_features.columns}")
            if 'prediction' in locals():
                logger.error(f"Available columns after prediction: {prediction.columns}")
            return {'label': "unknown", 'confidence': 0.0}

    def store_in_mongodb(self, review_data, prediction):
        """Store the review and prediction in MongoDB."""
        try:
            document = {
                'asin': review_data.get('asin'),
                'helpful': review_data.get('helpful'),
                'overall': review_data.get('overall'),
                'reviewText': review_data.get('reviewText'),
                'reviewTime': review_data.get('reviewTime'),
                'reviewerID': review_data.get('reviewerID'),
                'reviewerName': review_data.get('reviewerName'),
                'summary': review_data.get('summary'),
                'unixReviewTime': review_data.get('unixReviewTime'),
                'kafka_timestamp': review_data.get('kafka_timestamp'),
                'processed_timestamp': datetime.now().isoformat(),
                'label': prediction['label'],
                'confidence': prediction['confidence'],
                'is_realtime': True
            }
            
            self.db.predictions.insert_one(document)
            logger.info(f"Stored review {document['asin']} in MongoDB")
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
        
        # Define schema for Kafka messages - match your producer's output
        schema = StructType([
            StructField("asin", StringType()),
            StructField("helpful", ArrayType(IntegerType())),
            StructField("overall", FloatType()),
            StructField("reviewText", StringType()),
            StructField("reviewTime", StringType()),
            StructField("reviewerID", StringType()),
            StructField("reviewerName", StringType()),
            StructField("summary", StringType()),
            StructField("unixReviewTime", LongType()),
            StructField("sentiment", IntegerType()),
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
            
            # Debug: Print the first row to see the structure
            if not pandas_df.empty:
                logger.info(f"First row structure: {pandas_df.iloc[0].to_dict()}")
            
            # Process each review
            for _, row in pandas_df.iterrows():
                review_data = row.to_dict()
                logger.info(f"Processing review data: {review_data}")  # Debug log
                
                try:
                    # Try to get the review text with different possible field names
                    review_text = review_data.get('reviewText') or review_data.get('review_text') or review_data.get('text')
                    if review_text is None:
                        logger.error(f"Review text not found. Available fields: {list(review_data.keys())}")
                        continue
                        
                    # Get prediction
                    prediction = processor.predict_sentiment(spark, review_text)
                    logger.info(f"Prediction result: {prediction}")  # Debug log
                    
                    # Create document for MongoDB
                    document = {
                        'asin': review_data.get('asin'),
                        'helpful': review_data.get('helpful'),
                        'overall': review_data.get('overall'),
                        'reviewText': review_text,
                        'reviewTime': review_data.get('reviewTime'),
                        'reviewerID': review_data.get('reviewerID'),
                        'reviewerName': review_data.get('reviewerName'),
                        'summary': review_data.get('summary'),
                        'unixReviewTime': review_data.get('unixReviewTime'),
                        'kafka_timestamp': review_data.get('kafka_timestamp'),
                        'processed_timestamp': datetime.now().isoformat(),
                        'label': prediction['label'],  # Use the label from prediction
                        'confidence': prediction['confidence'],  # Use the confidence from prediction
                        'is_realtime': True
                    }
                    
                    # Store in MongoDB
                    processor.db.predictions.insert_one(document)
                    logger.info(f"Stored review {document['asin']} in MongoDB with label: {document['label']} and confidence: {document['confidence']}")
                    
                except Exception as e:
                    logger.error(f"Error processing review: {e}")
                    logger.error(f"Review data: {review_data}")
        
        # Start the streaming query with checkpoint location
        query = parsed_df.writeStream \
            .foreachBatch(process_batch) \
            .outputMode("append") \
            .option("checkpointLocation", "/app/checkpoints") \
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