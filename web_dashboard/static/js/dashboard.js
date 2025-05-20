// Chart instances
let sentimentTrendsChart = null;
let topProductsChart = null;

// Initialize the dashboard
document.addEventListener('DOMContentLoaded', function() {
    initializeCharts();
    setupEventListeners();
    startLiveUpdates();
});

function initializeCharts() {
    // Initialize sentiment trends chart
    const sentimentTrendsCtx = document.getElementById('sentiment-trends-chart').getContext('2d');
    sentimentTrendsChart = new Chart(sentimentTrendsCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Positive Sentiment',
                data: [],
                borderColor: 'rgb(75, 192, 192)',
                tension: 0.1
            }, {
                label: 'Negative Sentiment',
                data: [],
                borderColor: 'rgb(255, 99, 132)',
                tension: 0.1
            }]
        },
        options: {
            responsive: true,
            animation: {
                duration: 0
            },
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });

    // Initialize top products chart
    const topProductsCtx = document.getElementById('top-products-chart').getContext('2d');
    topProductsChart = new Chart(topProductsCtx, {
        type: 'bar',
        data: {
            labels: [],
            datasets: [{
                label: 'Number of Reviews',
                data: [],
                backgroundColor: 'rgb(54, 162, 235)'
            }]
        },
        options: {
            responsive: true,
            animation: {
                duration: 0
            },
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

function setupEventListeners() {
    // Set up filter event listeners
    document.getElementById('sentiment-filter').addEventListener('change', updateFeed);
    document.getElementById('rating-filter').addEventListener('change', updateFeed);
}

function startLiveUpdates() {
    // Update stats every 5 seconds
    setInterval(updateStats, 5000);
    
    // Update charts every 10 seconds
    setInterval(updateCharts, 10000);
    
    // Update feed every 2 seconds
    setInterval(updateFeed, 2000);
}

async function updateStats() {
    try {
        const response = await fetch('/api/realtime-stats');
        const data = await response.json();
        
        // Update statistics
        document.getElementById('total-reviews-today').textContent = data.total_reviews;
        document.getElementById('positive-sentiment').textContent = 
            `${(data.positive_sentiment * 100).toFixed(1)}%`;
        document.getElementById('avg-rating').textContent = 
            data.avg_rating.toFixed(1);
        document.getElementById('processing-rate').textContent = 
            `${data.processing_rate}/min`;
        
    } catch (error) {
        console.error('Error updating stats:', error);
        showAlert('Failed to update statistics', 'error');
    }
}

async function updateCharts() {
    try {
        // Update sentiment trends
        const trendsResponse = await fetch('/api/sentiment-trends?period=hourly');
        const trendsData = await trendsResponse.json();
        updateSentimentTrendsChart(trendsData);

        // Update top products
        const productsResponse = await fetch('/api/top-products?limit=5');
        const productsData = await productsResponse.json();
        updateTopProductsChart(productsData);
        
    } catch (error) {
        console.error('Error updating charts:', error);
        showAlert('Failed to update charts', 'error');
    }
}

function updateSentimentTrendsChart(data) {
    const timestamps = data.map(d => new Date(d.timestamp).toLocaleTimeString());
    const positiveData = data.map(d => d.positive_count);
    const negativeData = data.map(d => d.negative_count);

    sentimentTrendsChart.data.labels = timestamps;
    sentimentTrendsChart.data.datasets[0].data = positiveData;
    sentimentTrendsChart.data.datasets[1].data = negativeData;
    sentimentTrendsChart.update();
}

function updateTopProductsChart(data) {
    const products = data.map(p => p.asin);
    const counts = data.map(p => p.review_count);

    topProductsChart.data.labels = products;
    topProductsChart.data.datasets[0].data = counts;
    topProductsChart.update();
}

async function updateFeed() {
    try {
        const sentimentFilter = document.getElementById('sentiment-filter').value;
        const ratingFilter = document.getElementById('rating-filter').value;
        
        const response = await fetch(`/api/recent-predictions?limit=10&sentiment=${sentimentFilter}&rating=${ratingFilter}`);
        const reviews = await response.json();
        
        const feedContainer = document.getElementById('reviews-feed');
        feedContainer.innerHTML = reviews.map(review => `
            <div class="review-card ${review.sentiment_prediction === 1 ? 'positive' : 'negative'}">
                <div class="review-header">
                    <span class="product-id">${review.asin}</span>
                    <span class="rating">${'★'.repeat(review.overall)}${'☆'.repeat(5-review.overall)}</span>
                    <span class="sentiment">${review.sentiment_prediction === 1 ? 'Positive' : 'Negative'}</span>
                </div>
                <p class="review-text">${review.reviewText}</p>
                <div class="review-footer">
                    <span class="timestamp">${new Date(review.timestamp).toLocaleString()}</span>
                    <span class="confidence">Confidence: ${(review.confidence_score * 100).toFixed(1)}%</span>
                </div>
            </div>
        `).join('');
        
    } catch (error) {
        console.error('Error updating feed:', error);
        showAlert('Failed to update review feed', 'error');
    }
}

function showAlert(message, type = 'info') {
    const alertsContainer = document.getElementById('alerts-container');
    const alert = document.createElement('div');
    alert.className = `alert ${type}`;
    alert.textContent = message;
    
    alertsContainer.insertBefore(alert, alertsContainer.firstChild);
    
    // Remove alert after 5 seconds
    setTimeout(() => {
        alert.remove();
    }, 5000);
}
