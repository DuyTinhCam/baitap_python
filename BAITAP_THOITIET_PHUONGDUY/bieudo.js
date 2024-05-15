// Hàm để tải dữ liệu từ URL JSON
google.charts.load('current', {'packages':['corechart']});
google.charts.setOnLoadCallback(drawCharts);
function drawCharts() {
    drawTemperatureChart();

}
function loadData(url, callback) {
    fetch(url)
        .then(response => response.json())
        .then(data => callback(data))
        .catch(error => console.error('Error loading data:', error));
}

// Hàm để vẽ biểu đồ nhiệt độ
function drawTemperatureChart() {
    loadData('http://127.0.0.1:1880/get-temp', function(data) {
        // Tạo mảng dữ liệu cho biểu đồ
        var chartData = [];
        chartData.push(['date', 'temp']); // Thêm tiêu đề cột

        // Lặp qua dữ liệu JSON và thêm vào mảng dữ liệu
        data.forEach(function(entry) {
            chartData.push([new Date(entry.date), entry.temp]);
        });

        // Tạo DataTable từ mảng dữ liệu
        var dataTable = google.visualization.arrayToDataTable(chartData);

        // Cấu hình tùy chọn cho biểu đồ
        var options = {
            title: 'Temperature Chart',
            curveType: 'function',
            legend: { position: 'bottom' }
        };

        // Vẽ biểu đồ
        var chart = new google.visualization.LineChart(document.getElementById('temperature_chart'));
        chart.draw(dataTable, options);
    });
}