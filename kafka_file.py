from sqlalchemy import create_engine
import pandas as pd
from kafka import KafkaProducer
import json

# Thông tin kết nối đến PostgreSQL
DB_USER = 'postgres'
DB_PASSWORD = '010701'
DB_HOST = 'localhost'
DB_PORT = '5433'
DB_NAME = 'lazada'

# Tạo kết nối đến PostgreSQL
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Đọc dữ liệu từ bảng 'product'
df = pd.read_sql_table('product', con=engine)

# Tìm sản phẩm có lượt mua cao nhất
highest_sold_product = df.loc[df['sold'].idxmax()]

# Khởi tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Dữ liệu sản phẩm có lượt mua cao nhất
highest_sold_product_data = {
    'id': highest_sold_product['id'],
    'name': highest_sold_product['Name'],
    'price': highest_sold_product['price'],
    'link': highest_sold_product['link_item'],
    'sold': highest_sold_product['sold']
}

# Gửi dữ liệu vào Kafka topic
producer.send('product_data', highest_sold_product_data)
producer.flush()

print("Link sản phẩm có lượt mua cao nhất đã được gửi vào Kafka topic.")
