#!/usr/bin/env python3
"""
MongoDB utilities for Amazon reviews sentiment analysis.
"""

from pymongo import MongoClient, errors
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_mongo_client():
    """
    Create and return a MongoDB client.

    Returns:
        MongoClient: MongoDB client
    """
    try:
        client = MongoClient('mongodb://mongodb:27017/')
        return client
    except errors.ConnectionFailure as e:
        logger.error(f"Could not connect to MongoDB: {e}")
        raise


def write_to_mongodb(batch_df, batch_id):
    """
    Write batch data to MongoDB.

    Args:
        batch_df: DataFrame containing the batch data
        batch_id: ID of the batch
    """
    try:
        # Convert Spark DataFrame to Pandas
        records = batch_df.toPandas().to_dict('records')

        if not records:
            logger.info(f"Batch {batch_id}: No records to write")
            return

        logger.info(f"Batch {batch_id}: Writing {len(records)} records to MongoDB")

        # Get MongoDB client and collection
        client = get_mongo_client()
        db = client['amazon_reviews']
        collection = db['predictions']

        # Insert records
        collection.insert_many(records)

        logger.info(f"Batch {batch_id}: Successfully wrote {len(records)} records to MongoDB")

        # Close connection
        client.close()
    except Exception as e:
        logger.error(f"Batch {batch_id}: Error writing to MongoDB: {e}")
        raise