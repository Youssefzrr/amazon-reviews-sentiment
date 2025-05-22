// Chart.js configuration and utility functions
const chartColors = {
    positive: '#28a745',
    neutral: '#ffc107',
    negative: '#dc3545'
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
    const timeLabels = Object.keys(data).sort();
    const positiveData = timeLabels.map(l => data[l].positive);
    const neutralData = timeLabels.map(l => data[l].neutral);
    const negativeData = timeLabels.map(l => data[l].negative);

    return new Chart(ctx, {
        type: 'line',
        data: {
            labels: timeLabels,
            datasets: [
                {
                    label: 'Positive',
                    data: positiveData,
                    borderColor: chartColors.positive,
                    backgroundColor: 'rgba(40,167,69,0.1)',
                    fill: true
                },
                {
                    label: 'Neutral',
                    data: neutralData,
                    borderColor: chartColors.neutral,
                    backgroundColor: 'rgba(255,193,7,0.1)',
                    fill: true
                },
                {
                    label: 'Negative',
                    data: negativeData,
                    borderColor: chartColors.negative,
                    backgroundColor: 'rgba(220,53,69,0.1)',
                    fill: true
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Count'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Hour'
                    }
                }
            }
        }
    });
}
