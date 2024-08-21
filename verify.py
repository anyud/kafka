import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

# Kết nối đến PostgreSQL
engine = create_engine('postgresql+psycopg2://postgres:20272027@localhost:5432/postgres')

# Tạo schema mới 'silver' nếu chưa tồn tại
with engine.connect() as connection:
    try:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))
        print("Schema silver đã được tạo thành công.")
    except Exception as e:
        print(f"Lỗi khi tạo schema silver: {e}")
        connection.close()

# Đọc dữ liệu từ PostgreSQL với xử lý lỗi
try:
    stock_data = pd.read_sql("SELECT * FROM public.stock_data", engine)
    print("Đã truy vấn thành công bảng stock_data.")
except ProgrammingError as e:
    print(f"Lỗi khi truy vấn bảng stock_data: {e}")
    stock_data = None

try:
    stocks = pd.read_sql("SELECT * FROM public.stocks", engine)
    print("Đã truy vấn thành công bảng stocks.")
except ProgrammingError as e:
    print(f"Lỗi khi truy vấn bảng stocks: {e}")
    stocks = None

# Tiếp tục xử lý nếu các bảng tồn tại
if stock_data is not None and stocks is not None:
    # Xử lý các dữ liệu fact và dim
    stock_data['datetime'] = pd.to_datetime(stock_data['datetime'])

    # Tạo bảng date_dim từ cột datetime trong stock_data
    date_dim = pd.DataFrame({
        'date_id': stock_data['datetime'].drop_duplicates(),
        'day': stock_data['datetime'].dt.day,
        'month': stock_data['datetime'].dt.month,
        'year': stock_data['datetime'].dt.year,
        'weekday': stock_data['datetime'].dt.weekday,
        'quarter': stock_data['datetime'].dt.quarter
    }).drop_duplicates()

    # Tạo bảng fact từ stock_data và stocks
    stock_data_fact = stock_data.merge(stocks[['stock_id']], on='stock_id', how='left')
    stock_data_fact = stock_data_fact.merge(date_dim[['date_id']], left_on='datetime', right_on='date_id', how='left')

    # Chọn các cột cần thiết cho bảng fact
    stock_data_fact = stock_data_fact[['stock_id', 'date_id', 'open_price', 'close_price', 'high_price', 'low_price', 'volume']]

    # Lưu các bảng vào schema 'silver'
    try:
        with engine.connect() as connection:
            date_dim.to_sql('date_dim', con=connection, schema='silver', if_exists='replace', index=False)
            stock_data_fact.to_sql('stock_data_fact', con=connection, schema='silver', if_exists='replace', index=False)
            stocks.to_sql('stock_dim', con=connection, schema='silver', if_exists='replace', index=False)
            print(f"Các bảng đã được lưu vào schema 'silver'.")
    except ProgrammingError as e:
        print(f"Lỗi khi lưu bảng vào schema silver: {e}")
    except Exception as e:
        print(f"Đã xảy ra lỗi không mong muốn: {e}")

else:
    print("Không thể tiếp tục do bảng dữ liệu không tồn tại.")
