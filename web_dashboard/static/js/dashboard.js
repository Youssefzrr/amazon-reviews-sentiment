// Global variables
let timeSeriesChart = null;
const API_ENDPOINT = '/api/stats';
const UPDATE_INTERVAL = 5000; // 5 seconds

// Initialize the dashboard
function initDashboard() {
    // Initial data load
    fetchData();
    
    // Set up periodic updates
    setInterval(fetchData, UPDATE_INTERVAL);
}

// Fetch data from the API
async function fetchData() {
    try {
        const response = await fetch(API_ENDPOINT);
        const data = await response.json();
        
        if (response.ok) {
            updateDashboard(data);
        } else {
            console.error('Error fetching data:', data.error);
            showError('Failed to fetch dashboard data');
        }
    } catch (error) {
        console.error('Error:', error);
        showError('Network error while fetching data');
    }
}

// Update dashboard with new data
function updateDashboard(data) {
    // Update statistics
    updateStats(data.total_count, data.sentiment_distribution);
    
    // Update charts
    updateCharts(data.sentiment_distribution);
    
    // Update recent predictions table
    updatePredictionsTable(data.recent_predictions);
}

// Update statistics display
function updateStats(totalCount, sentimentDist) {
    document.getElementById('total-reviews').textContent = totalCount;
    
    const sentimentCounts = {
        positive: 0,
        neutral: 0,
        negative: 0
    };
    
    sentimentDist.forEach(item => {
        sentimentCounts[item._id.toLowerCase()] = item.count;
    });
    
    document.getElementById('positive-count').textContent = sentimentCounts.positive;
    document.getElementById('neutral-count').textContent = sentimentCounts.neutral;
    document.getElementById('negative-count').textContent = sentimentCounts.negative;
}

// Update charts
function updateCharts(sentimentDist) {
    const sentimentData = {
        positive: 0,
        neutral: 0,
        negative: 0
    };
    
    sentimentDist.forEach(item => {
        sentimentData[item._id.toLowerCase()] = item.count;
    });
    
    // Update or create sentiment chart
    if (sentimentChart) {
        sentimentChart.data.datasets[0].data = [
            sentimentData.positive,
            sentimentData.neutral,
            sentimentData.negative
        ];
        sentimentChart.update();
    } else {
        sentimentChart = createSentimentChart(sentimentData, 'sentiment-chart');
    }
}

// Update predictions table
function updatePredictionsTable(predictions) {
    const tableBody = document.getElementById('predictions-table-body');
    tableBody.innerHTML = '';
    
    predictions.forEach(prediction => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${prediction.reviewText.substring(0, 100)}...</td>
            <td>${prediction.sentiment}</td>
            <td>${prediction.prediction}</td>
        `;
        tableBody.appendChild(row);
    });
}

// Show error message
function showError(message) {
    const errorDiv = document.getElementById('error-message');
    if (errorDiv) {
        errorDiv.textContent = message;
        errorDiv.style.display = 'block';
        setTimeout(() => {
            errorDiv.style.display = 'none';
        }, 5000);
    }
}

// Updated dashboard.js for three sentiment categories and modern chart logic
function sentimentIcon(sentiment) {
    if (sentiment === 'positive') return '<i class="fa-solid fa-face-smile text-success"></i>';
    if (sentiment === 'neutral') return '<i class="fa-solid fa-face-meh text-warning"></i>';
    if (sentiment === 'negative') return '<i class="fa-solid fa-face-frown text-danger"></i>';
    return '<i class="fa-solid fa-question text-secondary"></i>';
}

function sentimentLabel(sentiment) {
    if (sentiment === 'positive') return 'Positive';
    if (sentiment === 'neutral') return 'Neutral';
    if (sentiment === 'negative') return 'Negative';
    return 'Unknown';
}

function renderReviews(containerId, data) {
    const container = document.querySelector(containerId);
    container.innerHTML = '';
    if (!data || data.length === 0) {
        container.innerHTML = '<p class="text-muted">No reviews available.</p>';
        return;
    }
    data.forEach(review => {
        container.innerHTML += `
            <div class="mb-3 p-3 border rounded review-block">
                <div class="d-flex justify-content-between align-items-center mb-2">
                    <span>${sentimentIcon(review.sentiment)} <strong>Sentiment:</strong> ${sentimentLabel(review.sentiment)}</span>
                    <span class="badge bg-secondary">${new Date(review.timestamp).toLocaleString()}</span>
                </div>
                <div class="mb-2"><strong>Confidence:</strong> ${(review.confidence * 100).toFixed(2)}%</div>
                <div class="mb-2"><strong>Product:</strong> ${review.product_id || 'N/A'}</div>
                <div class="mb-2"><strong>Reviewer:</strong> ${review.reviewer || 'N/A'}</div>
                <div class="mb-2"><strong>Summary:</strong> ${review.summary || 'N/A'}</div>
                <div class="mb-2"><strong>Rating:</strong> ${review.rating || 'N/A'}</div>
                <div class="review-text">${review.review_text}</div>
            </div>
        `;
    });
}

function fetchRealtime() {
    fetch('/api/realtime').then(r => r.json()).then(data => {
        createSentimentTimeChart(data.minute_sentiment, 'rt-sentiment-time-chart', 'Minute');
        renderReviews('#realtime-container', data.recent_reviews);
    });
}

function fetchOffline() {
    fetch('/api/offline').then(r => r.json()).then(data => {
        document.getElementById('total-reviews').textContent = data.total_reviews;
        document.getElementById('avg-confidence').textContent = (data.avg_confidence * 100).toFixed(2) + '%';
        document.getElementById('avg-rating').textContent = data.avg_rating.toFixed(2);
        createSentimentDoughnutChart({
            positive: data.positive_count,
            neutral: data.neutral_count,
            negative: data.negative_count
        }, 'sentiment-chart');
        createSentimentTimeChart(data.sentiment_time, 'sentiment-time-chart', 'Date');
        createSentimentBarChart(data.sentiment_rating, 'sentiment-rating-chart', 'Rating');
        createSentimentBarChart(data.sentiment_product, 'sentiment-product-chart', 'Product');
        createSentimentBarChart(data.sentiment_reviewer, 'sentiment-reviewer-chart', 'Reviewer');
        createSentimentBarChart(data.sentiment_length, 'sentiment-length-chart', 'Review Length');
        renderReviews('#offline-container', data.recent_reviews);
    });
}

document.addEventListener('DOMContentLoaded', function() {
    fetchRealtime();
    fetchOffline();
    setInterval(fetchRealtime, 5000);
    setInterval(fetchOffline, 10000);
});

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', initDashboard)
