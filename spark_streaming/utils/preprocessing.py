#!/usr/bin/env python3
"""
Text preprocessing utilities for Amazon reviews sentiment analysis.
"""

import re
import string
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

# Download NLTK resources
nltk.download('punkt', quiet=True)
nltk.download('stopwords', quiet=True)
nltk.download('wordnet', quiet=True)

# Initialize lemmatizer
lemmatizer = WordNetLemmatizer()

# Get English stopwords
stop_words = set(stopwords.words('english'))


def preprocess_text(text):
    """
    Preprocess text data for sentiment analysis.

    Args:
        text (str): Raw text to preprocess

    Returns:
        str: Preprocessed text
    """
    if not text or not isinstance(text, str):
        return ""

    # Convert to lowercase
    text = text.lower()

    # Remove punctuation
    text = text.translate(str.maketrans('', '', string.punctuation))

    # Remove numbers
    text = re.sub(r'\d+', '', text)

    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    # Tokenize
    tokens = nltk.word_tokenize(text)

    # Remove stopwords and lemmatize
    tokens = [lemmatizer.lemmatize(word) for word in tokens if word not in stop_words and len(word) > 2]

    # Join tokens back into text
    preprocessed_text = ' '.join(tokens)

    return preprocessed_text


def create_target(overall_rating):
    """
    Create target labels from overall rating.

    Args:
        overall_rating (float): Overall rating (1-5)

    Returns:
        str: Sentiment label ('positive', 'neutral', or 'negative')
    """
    if overall_rating < 3:
        return 'negative'
    elif overall_rating == 3:
        return 'neutral'
    else:
        return 'positive'