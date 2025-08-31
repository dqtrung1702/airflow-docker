from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook  # Thêm PostgresHook
from datetime import datetime, timedelta
from vnstock3 import Vnstock
from io import StringIO

# Hàm lấy dữ liệu từ vnstock3 và insert vào PostgreSQL
def fetch_and_insert_stock_data():
    # Khởi tạo Vnstock
    vnstock = Vnstock().stock(symbol='VN30F1M', source='TCBS')

    # Sử dụng PostgresHook để kết nối
    pg_hook = PostgresHook(postgres_conn_id='vnstock_postgres')  # ID connection sẽ cấu hình trong UI
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Tạo bảng nếu chưa tồn tại
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            time TIMESTAMP,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume INTEGER,
            price_change_percent DOUBLE PRECISION
        )
    """)
    conn.commit()

    # Lấy dữ liệu lịch sử (ví dụ: 30 ngày gần nhất)
    df = vnstock.quote.history(start=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'), 
                         end=datetime.now().strftime('%Y-%m-%d'), interval='1D')

    # Streaming và transform dữ liệu theo batch
    batch_size = 5000
    for start in range(0, len(df), batch_size):
        batch = df.iloc[start:start + batch_size]
        
        # Transform: Tính phần trăm thay đổi giá
        batch['price_change_percent'] = ((batch['close'] - batch['open']) / batch['open'] * 100).fillna(0)
        
        # Chuyển dữ liệu thành định dạng cho COPY
        buffer = StringIO()
        for row in batch.itertuples():
            buffer.write('\t'.join(map(str, [
                row.time, row.open, row.high, row.low, row.close, row.volume, row.price_change_percent
            ])) + '\n')
        buffer.seek(0)

        # Insert bằng COPY
        cursor.copy_from(buffer, 'stock_prices', 
                         columns=('time', 'open', 'high', 'low', 'close', 'volume', 'price_change_percent'), 
                         sep='\t')
        conn.commit()

    cursor.close()
    conn.close()

# Định nghĩa DAG
with DAG(
    dag_id='vnstock_withPostgresHook',
    start_date=datetime(2025, 3, 1),
    schedule_interval='@daily',  # Chạy hàng ngày
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    fetch_task = PythonOperator(
        task_id='fetch_and_insert_stock_data',
        python_callable=fetch_and_insert_stock_data,
    )