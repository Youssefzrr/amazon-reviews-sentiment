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
from collections import Counter, defaultdict
import math

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

def bin_review_length(length):
    if length < 50:
        return '<50'
    elif length < 100:
        return '50-99'
    elif length < 200:
        return '100-199'
    elif length < 500:
        return '200-499'
    else:
        return '500+'

def safe_get(d, key, default=None):
    return d.get(key, default) if d else default

@app.route('/')
def index():
    """Render main dashboard page."""
    return render_template('index.html')

@app.route('/api/realtime')
def get_realtime_data():
    try:
        now = datetime.now()
        one_hour_ago = now - timedelta(hours=1)
        five_mins_ago = now - timedelta(minutes=5)
        predictions = list(db.predictions.find({
            'is_realtime': True,
            'processed_timestamp': {'$gte': five_mins_ago.isoformat()}
        }).sort('processed_timestamp', -1))

        # All real-time predictions in the last hour for time series
        all_rt_preds = list(db.predictions.find({
            'is_realtime': True,
            'processed_timestamp': {'$gte': one_hour_ago.isoformat()}
        }))

        # Metrics
        total_reviews = len(predictions)
        avg_confidence = sum(p.get('confidence', 0) for p in predictions) / total_reviews if total_reviews > 0 else 0
        sentiment_counts = Counter(p.get('label', 'unknown') for p in predictions)
        positive_count = sentiment_counts.get('positive', 0)
        neutral_count = sentiment_counts.get('neutral', 0)
        negative_count = sentiment_counts.get('negative', 0)

        # Sentiment over time (minute granularity, last hour)
        minute_sentiment = defaultdict(lambda: {'positive': 0, 'neutral': 0, 'negative': 0})
        for p in all_rt_preds:
            ts = p.get('processed_timestamp', '')
            if ts:
                minute = datetime.fromisoformat(ts).strftime('%H:%M')
                label = p.get('label', 'unknown')
                if label in minute_sentiment[minute]:
                    minute_sentiment[minute][label] += 1
        # Sort by time
        minute_sentiment = dict(sorted(minute_sentiment.items()))

        # Recent reviews (last 10)
        formatted_data = [{
            'review_text': p.get('reviewText', ''),
            'sentiment': p.get('label', 'unknown'),
            'confidence': p.get('confidence', 0.0),
            'timestamp': p.get('processed_timestamp', ''),
            'product_id': p.get('asin', ''),
            'reviewer': p.get('reviewerName', ''),
            'summary': p.get('summary', ''),
            'rating': p.get('overall', 0.0)
        } for p in predictions[:10]]

        return jsonify({
            'total_reviews': total_reviews,
            'avg_confidence': avg_confidence,
            'positive_count': positive_count,
            'neutral_count': neutral_count,
            'negative_count': negative_count,
            'minute_sentiment': minute_sentiment,
            'recent_reviews': formatted_data
        })
    except Exception as e:
        logger.error(f"Error fetching real-time data: {e}")
        return jsonify({})

@app.route('/api/offline')
def get_offline_data():
    try:
        predictions = list(db.predictions.find().sort('processed_timestamp', -1))
        total_reviews = len(predictions)
        sentiment_counts = Counter(p.get('label', 'unknown') for p in predictions)
        positive_count = sentiment_counts.get('positive', 0)
        neutral_count = sentiment_counts.get('neutral', 0)
        negative_count = sentiment_counts.get('negative', 0)
        avg_confidence = sum(p.get('confidence', 0) for p in predictions) / total_reviews if total_reviews > 0 else 0
        avg_rating = sum(p.get('overall', 0) for p in predictions) / total_reviews if total_reviews > 0 else 0
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

        # Sentiment over time (by reviewTime day)
        sentiment_time = defaultdict(lambda: {'positive': 0, 'neutral': 0, 'negative': 0})
        for p in predictions:
            review_time = p.get('reviewTime', '')
            label = p.get('label', 'unknown')
            if review_time and label in ['positive', 'neutral', 'negative']:
                sentiment_time[review_time][label] += 1
        sentiment_time = dict(sorted(sentiment_time.items()))

        # Sentiment by rating
        sentiment_rating = defaultdict(lambda: {'positive': 0, 'neutral': 0, 'negative': 0})
        for p in predictions:
            rating = str(p.get('overall', 'N/A'))
            label = p.get('label', 'unknown')
            if rating and label in ['positive', 'neutral', 'negative']:
                sentiment_rating[rating][label] += 1
        sentiment_rating = dict(sorted(sentiment_rating.items(), key=lambda x: float(x[0]) if x[0] != 'N/A' else -1))

        # Sentiment by product
        sentiment_product = defaultdict(lambda: {'positive': 0, 'neutral': 0, 'negative': 0})
        for p in predictions:
            asin = p.get('asin', 'N/A')
            label = p.get('label', 'unknown')
            if asin and label in ['positive', 'neutral', 'negative']:
                sentiment_product[asin][label] += 1
        # Top 10 products by review count
        product_counts = {k: sum(v.values()) for k, v in sentiment_product.items()}
        top_products = sorted(product_counts, key=product_counts.get, reverse=True)[:10]
        sentiment_product = {k: sentiment_product[k] for k in top_products}

        # Sentiment by reviewer
        sentiment_reviewer = defaultdict(lambda: {'positive': 0, 'neutral': 0, 'negative': 0})
        for p in predictions:
            reviewer = p.get('reviewerName', 'N/A')
            label = p.get('label', 'unknown')
            if reviewer and label in ['positive', 'neutral', 'negative']:
                sentiment_reviewer[reviewer][label] += 1
        # Top 10 reviewers by review count
        reviewer_counts = {k: sum(v.values()) for k, v in sentiment_reviewer.items()}
        top_reviewers = sorted(reviewer_counts, key=reviewer_counts.get, reverse=True)[:10]
        sentiment_reviewer = {k: sentiment_reviewer[k] for k in top_reviewers}

        # Sentiment by review length (binned)
        sentiment_length = defaultdict(lambda: {'positive': 0, 'neutral': 0, 'negative': 0})
        for p in predictions:
            review_text = p.get('reviewText', '')
            label = p.get('label', 'unknown')
            if review_text and label in ['positive', 'neutral', 'negative']:
                length_bin = bin_review_length(len(review_text))
                sentiment_length[length_bin][label] += 1
        sentiment_length = dict(sorted(sentiment_length.items(), key=lambda x: x[0]))

        return jsonify({
            'total_reviews': total_reviews,
            'positive_count': positive_count,
            'neutral_count': neutral_count,
            'negative_count': negative_count,
            'avg_confidence': avg_confidence,
            'avg_rating': avg_rating,
            'recent_reviews': recent_reviews,
            'sentiment_time': sentiment_time,
            'sentiment_rating': sentiment_rating,
            'sentiment_product': sentiment_product,
            'sentiment_reviewer': sentiment_reviewer,
            'sentiment_length': sentiment_length
        })
    except Exception as e:
        logger.error(f"Error fetching offline data: {e}")
        return jsonify({})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)