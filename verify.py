import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

# Kết nối đến PostgreSQL
engine = create_engine('postgresql+psycopg2://postgres:20272027@localhost:5432/postgres')

# Tạo truy vấn SQL
stock_data_query = "SELECT * FROM public.stock_data"
stocks_query = "SELECT * FROM public.stocks"

# Đọc dữ liệu từ PostgreSQL với xử lý lỗi
try:
    stock_data = pd.read_sql(stock_data_query, engine)
except ProgrammingError as e:
    print(f"Lỗi khi truy vấn bảng stock_data: {e}")
    stock_data = None

try:
    stocks = pd.read_sql(stocks_query, engine)
except ProgrammingError as e:
    print(f"Lỗi khi truy vấn bảng stocks: {e}")
    stocks = None

# Tiếp tục xử lý nếu các bảng tồn tại
if stock_data is not None and stocks is not None:
    stock_dim = stocks.copy()

    stock_data['datetime'] = pd.to_datetime(stock_data['datetime'])
    date_dim = pd.DataFrame({
        'date_id': stock_data['datetime'].drop_duplicates(),
        'day': stock_data['datetime'].dt.day,
        'month': stock_data['datetime'].dt.month,
        'year': stock_data['datetime'].dt.year,
        'weekday': stock_data['datetime'].dt.weekday,
        'quarter': stock_data['datetime'].dt.quarter
    }).drop_duplicates()

    stock_data_fact = stock_data.merge(stock_dim[['stock_id']], on='stock_id', how='left')
    stock_data_fact = stock_data_fact.merge(date_dim[['date_id']], left_on='datetime', right_on='date_id', how='left')

    stock_data_fact = stock_data_fact[['stock_id', 'date_id', 'open_price', 'close_price', 'high_price', 'low_price', 'volume']]

    stock_dim.to_sql('stock_dim', engine, if_exists='replace', index=False)
    date_dim.to_sql('date_dim', engine, if_exists='replace', index=False)
    stock_data_fact.to_sql('stock_data_fact', engine, if_exists='replace', index=False)

    # Xóa các bảng không cần thiết
    try:
        with engine.connect() as connection:
            connection.execute(text("DROP TABLE IF EXISTS public.stock_data CASCADE;"))
            print("Bảng stock_data đã được xóa thành công.")
            connection.execute(text("DROP TABLE IF EXISTS public.stocks CASCADE;"))
            print("Bảng stocks đã được xóa thành công.")
    except Exception as e:
        print(f"Lỗi khi xóa bảng: {e}")
else:
    print("Không thể tiếp tục do bảng dữ liệu không tồn tại.")
