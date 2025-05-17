#!/usr/bin/env python3
"""
Flask web application for Amazon reviews sentiment analysis dashboard.
"""

from flask import Flask, render_template, jsonify, request
from pymongo import MongoClient
from datetime import datetime, timedelta
import json
import os

app = Flask(__name__)


# MongoDB connection
def get_mongo_client():
    """Create and return a MongoDB client."""
    return MongoClient('mongodb://mongodb:27017/')


@app.route('/')
def index():
    """Render main dashboard page."""
    return render_template('index.html')


@app.route('/online')
def online():
    """Render online predictions page."""
    return render_template('online.html')


@app.route('/offline')
def offline():
    """Render offline analysis page."""
    return render_template('offline.html')


@app.route('/api/real-time-predictions')
def real_time_predictions():
    """Get the latest predictions for real-time dashboard."""
    client = get_mongo_client()
    db = client['amazon_reviews']
    collection = db['predictions']

    # Get the latest 10 predictions
    latest_predictions = list(collection.find().sort('processing_timestamp', -1).limit(10))

    # Convert ObjectId to string for JSON serialization
    for pred in latest_predictions:
        pred['_id'] = str(pred['_id'])

    client.close()
    return jsonify(latest_predictions)


@app.route('/api/sentiment-distribution')
def sentiment_distribution():
    """Get sentiment distribution for charts."""
    client = get_mongo_client()
    db = client['amazon_reviews']
    collection = db['predictions']

    # Get sentiment distribution
    pipeline = [
        {'$group': {'_id': '$sentiment', 'count': {'$sum': 1}}}
    ]
    result = list(collection.aggregate(pipeline))

    client.close()
    return jsonify(result)


@app.route('/api/product/<asin>')
def product_sentiment(asin):
    """Get sentiment for specific product."""
    client = get_mongo_client()
    db = client['amazon_reviews']
    collection = db['predictions']

    # Get sentiment for specific product
    pipeline = [
        {'$match': {'asin': asin}},
        {'$group': {'_id': '$sentiment', 'count': {'$sum': 1}}}
    ]
    result = list(collection.aggregate(pipeline))

    client.close()
    return jsonify(result)


@app.route('/api/predictions-by-time')
def predictions_by_time():
    """Get predictions grouped by time for time-series charts."""
    client = get_mongo_client()
    db = client['amazon_reviews']
    collection = db['predictions']

    # Group predictions by hour
    pipeline = [
        {'$project': {
            'date': {'$substr': ['$reviewTime', 0, 10]},
            'sentiment': 1
        }},
        {'$group': {
            '_id': {'date': '$date', 'sentiment': '$sentiment'},
            'count': {'$sum': 1}
        }},
        {'$sort': {'_id.date': 1}}
    ]
    result = list(collection.aggregate(pipeline))

    client.close()
    return jsonify(result)


@app.route('/api/recent-products')
def recent_products():
    """Get list of recent products for filtering."""
    client = get_mongo_client()
    db = client['amazon_reviews']
    collection = db['predictions']

    # Get recent distinct products
    distinct_products = collection.distinct('asin', limit=20)

    products = []
    for asin in distinct_products:
        # Get product info
        product = collection.find_one({'asin': asin}, {'asin': 1, 'summary': 1})
        if product:
            products.append({
                'asin': product['asin'],
                'title': product.get('summary', 'Unknown Product')
            })

    client.close()
    return jsonify(products)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)