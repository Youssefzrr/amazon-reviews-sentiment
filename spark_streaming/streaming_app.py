#!/usr/bin/env python3
"""
Spark Streaming Application for Amazon Reviews Sentiment Analysis
This script consumes Amazon reviews from Kafka, predicts sentiment, and stores results in MongoDB.
"""

import os
import sys
import json
import pickle
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, IntegerType, TimestampType
from pymongo import MongoClient

sys.path.append(os.path.join(os.path.dirname(__file__), "utils"))
from preprocessing import preprocess_text
from mongo_utils import write_to_mongodb

# Define schema for Amazon reviews
review_schema = StructType([
    StructField("reviewerID", StringType()),
    StructField("asin", StringType()),
    StructField("reviewerName", StringType()),
    StructField("helpful", ArrayType(IntegerType())),
    StructField("reviewText", StringType()),
    StructField("overall", FloatType()),
    StructField("summary", StringType()),
    StructField("unixReviewTime", IntegerType()),
    StructField("reviewTime", StringType()),
    StructField("stream_timestamp", StringType())
])


def load_model_and_vectorizer():
    """Load the trained sentiment model and vectorizer."""
    model_path = "/app/models/sentiment_model.pkl"
    vectorizer_path = "/app/models/tfidf_vectorizer.pkl"

    with open(model_path, 'rb') as f:
        model = pickle.load(f)

    with open(vectorizer_path, 'rb') as f:
        vectorizer = pickle.load(f)

    return model, vectorizer


def create_sentiment_udf(model, vectorizer):
    """Create UDF for sentiment prediction."""

    def predict_sentiment(review_text, overall):
        try:
            # Skip prediction if no text
            if not review_text or len(review_text.strip()) == 0:
                # Determine sentiment based on overall rating
                if overall < 3:
                    return "negative"
                elif overall == 3:
                    return "neutral"
                else:
                    return "positive"

            # Preprocess text
            processed_text = preprocess_text(review_text)

            # Vectorize
            vectorized = vectorizer.transform([processed_text])

            # Predict
            prediction = model.predict(vectorized)[0]

            return prediction
        except Exception as e:
            print(f"Error in prediction: {e}")
            # Fallback to rating-based sentiment
            if overall < 3:
                return "negative"
            elif overall == 3:
                return "neutral"
            else:
                return "positive"

    return udf(predict_sentiment, StringType())


def create_spark_session():
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName("AmazonReviewsSentimentAnalysis") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,"
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()


def main():
    """Main function for the Spark streaming application."""
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Loading sentiment model and vectorizer...")
    model, vectorizer = load_model_and_vectorizer()

    # Create UDF for sentiment prediction
    sentiment_udf = create_sentiment_udf(model, vectorizer)

    print("Reading from Kafka...")
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "amazon-reviews") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON and apply schema
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), review_schema).alias("data")) \
        .select("data.*")

    # Add processing timestamp
    df_with_timestamp = parsed_df \
        .withColumn("processing_timestamp", current_timestamp())

    # Apply sentiment prediction
    result_df = df_with_timestamp \
        .withColumn("sentiment", sentiment_udf(col("reviewText"), col("overall")))

    # Select relevant columns for output
    output_df = result_df.select(
        "reviewerID", "asin", "reviewerName", "helpful", "reviewText",
        "overall", "summary", "unixReviewTime", "reviewTime",
        "stream_timestamp", "processing_timestamp", "sentiment"
    )

    print("Starting stream processing...")

    # Write to MongoDB
    mongo_query = output_df \
        .writeStream \
        .foreachBatch(write_to_mongodb) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()

    # Stream to console for debugging
    console_query = output_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    # Wait for termination
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()