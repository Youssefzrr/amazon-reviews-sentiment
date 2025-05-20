// Global variables
let sentimentChart = null;
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

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', initDashboard);
