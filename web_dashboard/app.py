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

@app.route('/api/realtime')
def get_realtime_data():
    try:
        five_mins_ago = datetime.now() - timedelta(minutes=5)
        predictions = list(db.predictions.find({
            'is_realtime': True,
            'processed_timestamp': {'$gte': five_mins_ago.isoformat()}
        }).sort('processed_timestamp', -1))
        
        formatted_data = [{
            'review_text': p.get('reviewText', ''),
            'sentiment': p.get('label', 'unknown'),
            'confidence': p.get('confidence', 0.0),
            'timestamp': p.get('processed_timestamp', ''),
            'product_id': p.get('asin', ''),
            'reviewer': p.get('reviewerName', ''),
            'summary': p.get('summary', ''),
            'rating': p.get('overall', 0.0)
        } for p in predictions]
        
        return jsonify(formatted_data)
    except Exception as e:
        logger.error(f"Error fetching real-time data: {e}")
        return jsonify([])

@app.route('/api/offline')
def get_offline_data():
    try:
        predictions = list(db.predictions.find().sort('processed_timestamp', -1))
        
        # Calculate statistics
        total_reviews = len(predictions)
        sentiment_counts = Counter(p.get('label', 'unknown') for p in predictions)
        positive_count = sentiment_counts.get('positive', 0)
        neutral_count = sentiment_counts.get('neutral', 0)
        negative_count = sentiment_counts.get('negative', 0)
        
        # Calculate average confidence and rating
        avg_confidence = sum(p.get('confidence', 0) for p in predictions) / total_reviews if total_reviews > 0 else 0
        avg_rating = sum(p.get('overall', 0) for p in predictions) / total_reviews if total_reviews > 0 else 0
        
        # Get recent reviews
        recent_reviews = [{
            'review_text': p.get('reviewText', ''),
            'sentiment': p.get('label', 'unknown'),
            'confidence': p.get('confidence', 0.0),
            'timestamp': p.get('processed_timestamp', ''),
            'product_id': p.get('asin', ''),
            'reviewer': p.get('reviewerName', ''),
            'summary': p.get('summary', ''),
            'rating': p.get('overall', 0.0)
        } for p in predictions[:10]]
        
        # Calculate hourly sentiment
        one_day_ago = datetime.now() - timedelta(days=1)
        hourly_sentiment = {}
        for p in predictions:
            if datetime.fromisoformat(p.get('processed_timestamp', '')) >= one_day_ago:
                hour = datetime.fromisoformat(p.get('processed_timestamp', '')).strftime('%Y-%m-%d %H:00')
                if hour not in hourly_sentiment:
                    hourly_sentiment[hour] = {'positive': 0, 'neutral': 0, 'negative': 0}
                sentiment = p.get('label', 'unknown')
                if sentiment in hourly_sentiment[hour]:
                    hourly_sentiment[hour][sentiment] += 1
        
        return jsonify({
            'total_reviews': total_reviews,
            'positive_count': positive_count,
            'neutral_count': neutral_count,
            'negative_count': negative_count,
            'avg_confidence': avg_confidence,
            'avg_rating': avg_rating,
            'recent_reviews': recent_reviews,
            'hourly_sentiment': hourly_sentiment
        })
    except Exception as e:
        logger.error(f"Error fetching offline data: {e}")
        return jsonify({})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)