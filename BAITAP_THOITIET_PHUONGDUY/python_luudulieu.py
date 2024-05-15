import requests
import pyodbc
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_weather_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logger.info("Successfully fetched data from the URL.")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from {url}: {e}")
        return None

def create_table_if_not_exists(cursor):
    create_table_query = '''
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='WeatherData' and xtype='U')
    CREATE TABLE WeatherData (
        id INT PRIMARY KEY IDENTITY(1,1),
        temperature FLOAT,
        city NVARCHAR(255),
        date DATE
    )
    '''
    cursor.execute(create_table_query)
    logger.info("Table WeatherData created or already exists.")

def insert_weather_data_to_db(data, connection_string):
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Tạo bảng nếu chưa tồn tại
        create_table_if_not_exists(cursor)

        # Câu lệnh SQL để chèn dữ liệu
        insert_query = '''
        INSERT INTO weather (temp, city, date)
        VALUES (?, ?, ?)
        '''

        for item in data:
            logger.info(f"Processing item: {item}")
            temperature = item.get('Temperature')  # Sử dụng 'Temperature' thay vì 'temperature'
            city = item.get('City')  # Sử dụng 'City' thay vì 'city'
            date = item.get('Date')  # Sử dụng 'Date' thay vì 'date'

            if temperature is not None and city is not None and date is not None:
                logger.info(f"Inserting record: Temperature={temperature}, City={city}, Date={date}")
                cursor.execute(insert_query, (temperature, city, date))
            else:
                logger.warning(f"Invalid record skipped: {item}")

        # Commit các thay đổi
        conn.commit()
        logger.info("Data inserted successfully.")
    except pyodbc.Error as e:
        logger.error(f"Database error: {e}")
    finally:
        # Đảm bảo rằng kết nối được đóng ngay cả khi có lỗi xảy ra
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    url = 'http://127.0.0.1:8000/weather'
    connection_string = (
        'DRIVER={SQL Server};'
        'SERVER=127.0.0.1,1433;'
        'DATABASE=thoitiet;'
        'UID=sa;'
        'PWD=123'
    )

    # Lấy dữ liệu thời tiết từ endpoint
    weather_data = fetch_weather_data(url)

    if weather_data:
        logger.info(f"Weather data fetched: {weather_data}")
        # Lưu dữ liệu vào cơ sở dữ liệu
        insert_weather_data_to_db(weather_data, connection_string)
    else:
        logger.error("Failed to fetch weather data.")

if __name__ == "__main__":
    main()
