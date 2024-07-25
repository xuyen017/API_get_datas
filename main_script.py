from kafka import KafkaConsumer
from sqlalchemy import create_engine
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

DB_HOST = 'postgres'
DB_PORT = '5432'
DB_NAME = 'lazada'
DB_USER = 'postgres'
DB_PASSWORD = '010701'

KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'lazada_data_topic'  # Tên topic bạn chọn

def consume_messages():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group'
    )
    for message in consumer:
        print(f"Received message: {message.value}")

def main():
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    print("Connected to PostgreSQL database!")

    df = pd.read_sql_table('product', con=engine)
    print("Data from PostgreSQL:", df)

    # Thiết lập Selenium
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    driver.get("https://example.com")
    print("Page title was:", driver.title)
    driver.quit()

    consume_messages()

if __name__ == "__main__":
    main()
