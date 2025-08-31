from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from vnstock3 import Vnstock

# Macro tính ngày bắt đầu (3 ngày trước)
def tinh_ngay_bat_dau(ds):
    return (datetime.strptime(ds, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

# ✅ Task 1: Lấy danh sách mã chứng khoán từ DB
@task
def get_symbols():
    hook = PostgresHook(postgres_conn_id='vnstock_postgres')
    symbols = hook.get_records("SELECT symbol FROM flw_symbols")
    return [{"symbol": s[0]} for s in symbols]  # Trả về danh sách dictionary để expand_kwargs()

# ✅ Task 2: Lấy dữ liệu từ vnstock (chạy song song)
@task
def get_historical_data(symbol, start_date, end_date):
    print(f"Fetching data for {symbol}")
    vnstock = Vnstock().stock(symbol=symbol, source='TCBS')
    print(f"⏰:{start_date} - {end_date}")
    df = vnstock.quote.history(start=start_date, end=end_date, interval='1m')
    print(df)
    # 🛠 Thêm cột etl_timestamp với giá trị thời gian hiện tại
    df["etl_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    file_name = f'/tmp/{symbol}_data_{datetime.now().strftime("%Y%m%d")}.csv'
    df.to_csv(file_name, index=False)
    print(f"Saved data for {symbol} to {file_name}")

# ✅ Task 3: Lưu dữ liệu vào Postgres (chạy song song)
@task
def save_data(symbol, start_date, end_date):
    print(f"Save data for {symbol}")
    file_path = f"/tmp/{symbol}_data_{datetime.now().strftime('%Y%m%d')}.csv"
    
    sql_create_table = f"""
        CREATE TABLE IF NOT EXISTS {symbol}_historical_data (
            time timestamp,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            etl_timestamp timestamp
        );
        
        -- Tạo index nếu chưa tồn tại
        CREATE INDEX IF NOT EXISTS {symbol}_historical_data_time_idx 
        ON {symbol}_historical_data (time);
    """

    sql_delete_old = f"""
        DELETE FROM {symbol}_historical_data WHERE time >= date'{start_date}' AND time < date'{end_date}' + interval'1 days';
    """

    hook = PostgresHook(postgres_conn_id="vnstock_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        # Tạo bảng nếu chưa tồn tại
        cur.execute(sql_create_table)
        conn.commit()

        # Xóa dữ liệu cũ trước khi import dữ liệu mới
        print("Xóa dữ liệu cũ trước khi import dữ liệu mới:"+sql_delete_old)
        cur.execute(sql_delete_old)
        conn.commit()

        # Đẩy dữ liệu từ file CSV vào PostgreSQL bằng copy_expert
        with open(file_path, "r") as f:
            cur.copy_expert(f"COPY {symbol}_historical_data FROM STDIN WITH CSV HEADER", f)
        
        conn.commit()
        print(f"✅ Saved data for {symbol} to database")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error saving data for {symbol}: {e}")

    finally:
        cur.close()
        conn.close()


# Định nghĩa DAG
with DAG(
    dag_id='vnstock_symbols_flw',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=True,
    user_defined_macros={'tinh_ngay_truoc': tinh_ngay_bat_dau}
) as dag:

    # ✅ Task 1: Lấy danh sách mã chứng khoán
    symbols_list = get_symbols()

    # ✅ Task 2: Tải dữ liệu từ vnstock (song song)
    get_data_task = get_historical_data.expand_kwargs(
        symbols_list.map(lambda s: {
            "symbol": s["symbol"],
            # "start_date": "{{ tinh_ngay_truoc(ds) }}",
            # "end_date": "{{ ds }}"
            "start_date": "{{ dag_run.conf.get('start_date', tinh_ngay_truoc(ds)) }}",
            "end_date": "{{ dag_run.conf.get('end_date', ds) }}"
        })
    )

    # ✅ Task 3: Lưu dữ liệu vào Postgres (song song)
    save_data_task = save_data.expand_kwargs(
        symbols_list.map(lambda s: {
            "symbol": s["symbol"],
            # "start_date": "{{ tinh_ngay_truoc(ds) }}",
            # "end_date": "{{ ds }}"
            "start_date": "{{ dag_run.conf.get('start_date', tinh_ngay_truoc(ds)) }}",
            "end_date": "{{ dag_run.conf.get('end_date', ds) }}"
        })
    )

    # Thứ tự thực thi
    symbols_list >> get_data_task >> save_data_task
