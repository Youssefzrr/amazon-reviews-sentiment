#!/usr/bin/env python3
"""
Flask web application for Amazon reviews sentiment analysis dashboard.
"""

from flask import Flask, render_template, jsonify, request
from pymongo import MongoClient
from datetime import datetime, timedelta
import json
import os
import logging
from collections import Counter

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/dashboard.log'
)
logger = logging.getLogger(__name__)

# MongoDB connection
client = MongoClient('mongodb://mongodb:27017/')
db = client['amazon_reviews']

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


@app.route('/api/realtime')
def get_realtime_data():
    try:
        # Get real-time predictions from the last 5 minutes
        five_mins_ago = datetime.now() - timedelta(minutes=5)
        predictions = list(db.predictions.find({
            'is_realtime': True,
            'processed_timestamp': {'$gte': five_mins_ago.isoformat()}
        }).sort('processed_timestamp', -1))
        
        formatted_data = [{
            'review_text': p['review_text'],
            'sentiment': p['sentiment'],
            'confidence': p['confidence'],
            'timestamp': p['processed_timestamp']
        } for p in predictions]
        
        return jsonify(formatted_data)
    except Exception as e:
        logger.error(f"Error fetching real-time data: {e}")
        return jsonify([])


@app.route('/api/offline')
def get_offline_data():
    try:
        # Get all predictions
        predictions = list(db.predictions.find().sort('processed_timestamp', -1))
        
        # Calculate statistics
        total_reviews = len(predictions)
        sentiment_counts = Counter(p['sentiment'] for p in predictions)
        positive_count = sentiment_counts.get(1, 0)
        negative_count = sentiment_counts.get(0, 0)
        
        # Calculate average confidence
        avg_confidence = sum(p['confidence'] for p in predictions) / total_reviews if total_reviews > 0 else 0
        
        # Get recent reviews
        recent_reviews = [{
            'review_text': p['review_text'],
            'sentiment': p['sentiment'],
            'confidence': p['confidence'],
            'timestamp': p['processed_timestamp']
        } for p in predictions[:10]]  # Last 10 reviews
        
        # Calculate sentiment over time (last 24 hours)
        one_day_ago = datetime.now() - timedelta(days=1)
        hourly_sentiment = {}
        for p in predictions:
            if datetime.fromisoformat(p['processed_timestamp']) >= one_day_ago:
                hour = datetime.fromisoformat(p['processed_timestamp']).strftime('%Y-%m-%d %H:00')
                if hour not in hourly_sentiment:
                    hourly_sentiment[hour] = {'positive': 0, 'negative': 0}
                if p['sentiment'] == 1:
                    hourly_sentiment[hour]['positive'] += 1
                else:
                    hourly_sentiment[hour]['negative'] += 1
        
        return jsonify({
            'total_reviews': total_reviews,
            'positive_count': positive_count,
            'negative_count': negative_count,
            'avg_confidence': avg_confidence,
            'recent_reviews': recent_reviews,
            'hourly_sentiment': hourly_sentiment
        })
    except Exception as e:
        logger.error(f"Error fetching offline data: {e}")
        return jsonify({})


@app.route('/api/sentiment-distribution')
def sentiment_distribution():
    """Get sentiment distribution for charts."""
    collection = db['predictions']

    # Get sentiment distribution
    pipeline = [
        {'$group': {'_id': '$sentiment', 'count': {'$sum': 1}}}
    ]
    result = list(collection.aggregate(pipeline))

    return jsonify(result)


@app.route('/api/product/<asin>')
def product_sentiment(asin):
    """Get sentiment for specific product."""
    collection = db['predictions']

    # Get sentiment for specific product
    pipeline = [
        {'$match': {'asin': asin}},
        {'$group': {'_id': '$sentiment', 'count': {'$sum': 1}}}
    ]
    result = list(collection.aggregate(pipeline))

    return jsonify(result)


@app.route('/api/predictions-by-time')
def predictions_by_time():
    """Get predictions grouped by time for time-series charts."""
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

    return jsonify(result)


@app.route('/api/recent-products')
def recent_products():
    """Get list of recent products for filtering."""
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

    return jsonify(products)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)