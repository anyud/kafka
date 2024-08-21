import json
import psycopg2
from kafka import KafkaConsumer
from psycopg2 import sql

# Kết nối đến PostgreSQL
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="20272027",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Đảm bảo rằng bảng stocks và stock_data đã được tạo
create_stocks_table = '''
CREATE TABLE IF NOT EXISTS stocks (
    stock_id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL
);
'''
create_stock_data_table = '''
CREATE TABLE IF NOT EXISTS stock_data (
    id SERIAL PRIMARY KEY,
    stock_id INT NOT NULL,
    datetime TIMESTAMP NOT NULL,
    open_price NUMERIC,
    close_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    volume BIGINT,
    FOREIGN KEY (stock_id) REFERENCES stocks (stock_id),
    UNIQUE (datetime, stock_id)
);
'''
cur.execute(create_stocks_table)
cur.execute(create_stock_data_table)
conn.commit()

# Hàm lấy stock_id từ ticker hoặc tạo mới nếu chưa tồn tại
def get_or_create_stock_id(ticker):
    cur.execute("SELECT stock_id FROM stocks WHERE ticker = %s", (ticker,))
    result = cur.fetchone()
    if result:
        return result[0]
    else:
        cur.execute("INSERT INTO stocks (ticker) VALUES (%s) RETURNING stock_id", (ticker,))
        conn.commit()
        return cur.fetchone()[0]

# Cấu hình Kafka Consumer cho các topic
tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NFLX', 'NVDA', 'FB', 'BABA', 'ORCL']
topics = [f'stock_{ticker}_topic' for ticker in tickers]
consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='stock-data-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Chèn dữ liệu vào PostgreSQL
insert_stock_data_query = '''
INSERT INTO stock_data (stock_id, datetime, open_price, close_price, high_price, low_price, volume)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (datetime, stock_id) DO NOTHING;
'''

try:
    for message in consumer:
        data = message.value
        print("Nhận dữ liệu từ Kafka:", data)

        # Lấy hoặc tạo stock_id cho ticker hiện tại
        stock_id = get_or_create_stock_id(data['ticker'])
        
        # Kiểm tra và xác nhận các giá trị có tồn tại trước khi chèn
        open_price = data.get('open_price')
        close_price = data.get('close_price')
        high_price = data.get('high_price')
        low_price = data.get('low_price')
        volume = data.get('volume')

        # Kiểm tra xem các giá trị có hợp lệ không
        if open_price is not None and close_price is not None and high_price is not None and low_price is not None and volume is not None:
            # In ra các giá trị cần chèn vào PostgreSQL để kiểm tra
            print(f"Chèn dữ liệu vào PostgreSQL: stock_id={stock_id}, datetime={data['datetime']}, open_price={open_price}, close_price={close_price}, high_price={high_price}, low_price={low_price}, volume={volume}")

            # Chèn dữ liệu vào bảng stock_data
            cur.execute(insert_stock_data_query, (
                stock_id,
                data['datetime'],
                open_price,
                close_price,
                high_price,
                low_price,
                volume
            ))
            conn.commit()
            print(f"Đã lưu dữ liệu cho {data['ticker']} vào PostgreSQL.")
        else:
            print(f"Dữ liệu không đầy đủ cho {data['ticker']} tại {data['datetime']}. Bỏ qua việc chèn.")
except Exception as e:
    print(f"Lỗi khi lưu dữ liệu: {e}")
finally:
    cur.close()
    conn.close()
