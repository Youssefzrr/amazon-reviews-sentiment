// Chart.js configuration and utility functions
const chartColors = {
    positive: '#28a745',
    neutral: '#ffc107',
    negative: '#dc3545'
};

let chartInstances = {};

function destroyChart(id) {
    if (chartInstances[id]) {
        chartInstances[id].destroy();
        delete chartInstances[id];
    }
}

function createSentimentDoughnutChart(data, elementId) {
    destroyChart(elementId);
    const ctx = document.getElementById(elementId).getContext('2d');
    chartInstances[elementId] = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Positive', 'Neutral', 'Negative'],
            datasets: [{
                data: [data.positive, data.neutral, data.negative],
                backgroundColor: [chartColors.positive, chartColors.neutral, chartColors.negative]
            }]
        },
        options: {
            plugins: { legend: { display: true, position: 'bottom' } },
            maintainAspectRatio: false
        }
    });
}

function createSentimentTimeChart(timeData, elementId, xLabel='Time') {
    destroyChart(elementId);
    const ctx = document.getElementById(elementId).getContext('2d');
    const labels = Object.keys(timeData);
    const positive = labels.map(l => timeData[l].positive);
    const neutral = labels.map(l => timeData[l].neutral);
    const negative = labels.map(l => timeData[l].negative);
    chartInstances[elementId] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                { label: 'Positive', data: positive, borderColor: chartColors.positive, backgroundColor: 'rgba(40,167,69,0.1)', fill: true },
                { label: 'Neutral', data: neutral, borderColor: chartColors.neutral, backgroundColor: 'rgba(255,193,7,0.1)', fill: true },
                { label: 'Negative', data: negative, borderColor: chartColors.negative, backgroundColor: 'rgba(220,53,69,0.1)', fill: true }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { title: { display: true, text: xLabel } },
                y: { beginAtZero: true, title: { display: true, text: 'Count' } }
            }
        }
    });
}

function createSentimentBarChart(groupData, elementId, xLabel='Category') {
    destroyChart(elementId);
    const ctx = document.getElementById(elementId).getContext('2d');
    const labels = Object.keys(groupData);
    const positive = labels.map(l => groupData[l].positive);
    const neutral = labels.map(l => groupData[l].neutral);
    const negative = labels.map(l => groupData[l].negative);
    chartInstances[elementId] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                { label: 'Positive', data: positive, backgroundColor: chartColors.positive },
                { label: 'Neutral', data: neutral, backgroundColor: chartColors.neutral },
                { label: 'Negative', data: negative, backgroundColor: chartColors.negative }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: true, position: 'top' } },
            scales: {
                x: { title: { display: true, text: xLabel } },
                y: { beginAtZero: true, title: { display: true, text: 'Count' } }
            }
        }
    });
}
