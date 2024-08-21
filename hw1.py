import yfinance as yf
import json
import pandas as pd
import time
from kafka import KafkaProducer
from datetime import datetime


# Cấu hình Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 10 mã cổ phiếu
tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NFLX', 'NVDA', 'FB', 'BABA', 'ORCL']

# Lưu trữ thời gian cuối cùng lấy dữ liệu
last_datetimes = {ticker: None for ticker in tickers}

# Hàm stream dữ liệu từ yfinance vào Kafka
def stream_yfinance_to_kafka():
    while True:
        for ticker in tickers:
            try:
                # Sử dụng Ticker Object thay vì download toàn bộ
                ticker_obj = yf.Ticker(ticker)
                
                # Lấy dữ liệu với khoảng thời gian 1 phút gần nhất
                stock_data = ticker_obj.history(period='1d', interval='1m')
                
                # Kiểm tra nếu không có dữ liệu
                if stock_data.empty:
                    print(f"Không có dữ liệu cho {ticker} vào thời điểm này.")
                    continue
                
                stock_data.reset_index(inplace=True)
                
                for index, row in stock_data.iterrows():
                    # Chỉ lấy dữ liệu mới sau lần cuối cùng lấy dữ liệu
                    if last_datetimes[ticker] is None or row['Datetime'] > last_datetimes[ticker]:
                        if pd.isna(row['Open']) or pd.isna(row['Close']):
                            print(f"Dữ liệu thiếu cho {ticker} tại {row['Datetime']}")
                            continue

                        data = {
                            'datetime': row['Datetime'].strftime('%Y-%m-%d %H:%M:%S'),
                            'open_price': row['Open'],
                            'close_price': row['Close'],
                            'high_price': row['High'],
                            'low_price': row['Low'],
                            'volume': row['Volume'],
                            'ticker': ticker
                        }
                        
                        topic = f'stock_{ticker}_topic'  # Tạo topic tương ứng với mã cổ phiếu
                        producer.send(topic, value=data)
                        producer.flush()
                        print(f"Đã gửi dữ liệu vào Kafka topic {topic}: {data}")
                        
                        # Cập nhật thời gian cuối cùng lấy dữ liệu
                        last_datetimes[ticker] = row['Datetime']
            
            except Exception as e:
                print(f"Lỗi khi lấy dữ liệu cho {ticker}: {e}")
        
        # Nghỉ 60 giây trước khi tiếp tục lấy dữ liệu mới
        time.sleep(60)

# Bắt đầu stream dữ liệu
stream_yfinance_to_kafka()
