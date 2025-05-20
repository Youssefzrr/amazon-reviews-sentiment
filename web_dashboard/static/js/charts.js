// Chart.js configuration and utility functions
const chartColors = {
    positive: '#28a745',
    negative: '#dc3545',
    neutral: '#ffc107'
};

function createSentimentChart(data, elementId) {
    const ctx = document.getElementById(elementId).getContext('2d');
    return new Chart(ctx, {
        type: 'pie',
        data: {
            labels: ['Positive', 'Neutral', 'Negative'],
            datasets: [{
                data: [
                    data.positive || 0,
                    data.neutral || 0,
                    data.negative || 0
                ],
                backgroundColor: [
                    chartColors.positive,
                    chartColors.neutral,
                    chartColors.negative
                ]
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom'
                }
            }
        }
    });
}

function createTimeSeriesChart(data, elementId) {
    const ctx = document.getElementById(elementId).getContext('2d');
    return new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.labels,
            datasets: [{
                label: 'Predictions',
                data: data.values,
                borderColor: '#2c3e50',
                tension: 0.1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}
