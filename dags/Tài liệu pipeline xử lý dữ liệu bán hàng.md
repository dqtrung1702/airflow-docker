# Tài liệu Pipeline Xử lý Dữ liệu Bán Hàng

## 1. Tổng quan Hệ thống

### 1.1 Kiến trúc
- **Airflow**: Hệ thống điều phối pipeline (version 2.9.0)
- **PostgreSQL**: CSDL chính
- **Redis**: Message broker cho Celery
- **Docker**: Nền tảng container hóa

### 1.2 Container Services
1. **Redis**
   - Port: 6379
   - Healthcheck: Kiểm tra kết nối Redis
   - Dockerfile tùy chỉnh: Dockerfile.redis

2. **PostgreSQL**
   - Port: 5433
   - Database: airflow
   - Volume: postgres_data
   - Dockerfile tùy chỉnh: Dockerfile.postgres

3. **Airflow Services**
   - **airflow-webserver**: UI (port 8080)
   - **airflow-scheduler**: Lập lịch DAG
   - **airflow-worker-1, airflow-worker-2**: Xử lý task
   - Dockerfile tùy chỉnh: Dockerfile.airflow1, Dockerfile.airflow2

## 2. Cấu Trúc Database

### 2.1 Dữ liệu raw - Schema dev
Các bảng chính:

1. **customers**
   - Thông tin khách hàng
```sql
CREATE TABLE IF NOT EXISTS dev.customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    telephone VARCHAR(50),
    city VARCHAR(100),
    country VARCHAR(100),
    gender CHAR(1),
    date_of_birth DATE,
    job_title VARCHAR(255)
);
```
2. **transactions**
   - Dữ liệu giao dịch
   - Phân vùng theo tháng
```sql
CREATE TABLE IF NOT EXISTS dev.transactions (
    invoice_id VARCHAR(50) NOT NULL,
    line INT NOT NULL,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    size VARCHAR(10),
    color VARCHAR(50),
    unit_price NUMERIC(10, 2) NOT NULL,
    quantity INT NOT NULL,
    date TIMESTAMP NOT NULL,
    discount NUMERIC(5, 2) NOT NULL,
    line_total NUMERIC(10, 2) NOT NULL,
    store_id INT NOT NULL,
    employee_id INT NOT NULL,
    currency VARCHAR(10) NOT NULL,
    currency_symbol VARCHAR(5),
    sku VARCHAR(50),
    transaction_type VARCHAR(20) NOT NULL,
    payment_method VARCHAR(20) NOT NULL,
    invoice_total NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (invoice_id, line, date)
) PARTITION BY RANGE (date);

-- Create partitions for 2023
CREATE TABLE IF NOT EXISTS dev.transactions_2023_01 PARTITION OF dev.transactions FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2023_02 PARTITION OF dev.transactions FOR VALUES FROM ('2023-02-01') TO ('2023-03-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2023_03 PARTITION OF dev.transactions FOR VALUES FROM ('2023-03-01') TO ('2023-04-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2023_04 PARTITION OF dev.transactions FOR VALUES FROM ('2023-04-01') TO ('2023-05-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2023_05 PARTITION OF dev.transactions FOR VALUES FROM ('2023-05-01') TO ('2023-06-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2023_06 PARTITION OF dev.transactions FOR VALUES FROM ('2023-06-01') TO ('2023-07-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2023_07 PARTITION OF dev.transactions FOR VALUES FROM ('2023-07-01') TO ('2023-08-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2023_08 PARTITION OF dev.transactions FOR VALUES FROM ('2023-08-01') TO ('2023-09-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2023_09 PARTITION OF dev.transactions FOR VALUES FROM ('2023-09-01') TO ('2023-10-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2023_10 PARTITION OF dev.transactions FOR VALUES FROM ('2023-10-01') TO ('2023-11-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2023_11 PARTITION OF dev.transactions FOR VALUES FROM ('2023-11-01') TO ('2023-12-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2023_12 PARTITION OF dev.transactions FOR VALUES FROM ('2023-12-01') TO ('2024-01-01');

-- Create partitions for 2024
CREATE TABLE IF NOT EXISTS dev.transactions_2024_01 PARTITION OF dev.transactions FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2024_02 PARTITION OF dev.transactions FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2024_03 PARTITION OF dev.transactions FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2024_04 PARTITION OF dev.transactions FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2024_05 PARTITION OF dev.transactions FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2024_06 PARTITION OF dev.transactions FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2024_07 PARTITION OF dev.transactions FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2024_08 PARTITION OF dev.transactions FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2024_09 PARTITION OF dev.transactions FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2024_10 PARTITION OF dev.transactions FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2024_11 PARTITION OF dev.transactions FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2024_12 PARTITION OF dev.transactions FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

-- Create partitions for 2025
CREATE TABLE IF NOT EXISTS dev.transactions_2025_01 PARTITION OF dev.transactions FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2025_02 PARTITION OF dev.transactions FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2025_03 PARTITION OF dev.transactions FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2025_04 PARTITION OF dev.transactions FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2025_05 PARTITION OF dev.transactions FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2025_06 PARTITION OF dev.transactions FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2025_07 PARTITION OF dev.transactions FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2025_08 PARTITION OF dev.transactions FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2025_09 PARTITION OF dev.transactions FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2025_10 PARTITION OF dev.transactions FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2025_11 PARTITION OF dev.transactions FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2025_12 PARTITION OF dev.transactions FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

-- Create partitions for 2026
CREATE TABLE IF NOT EXISTS dev.transactions_2026_01 PARTITION OF dev.transactions FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2026_02 PARTITION OF dev.transactions FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2026_03 PARTITION OF dev.transactions FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2026_04 PARTITION OF dev.transactions FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2026_05 PARTITION OF dev.transactions FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2026_06 PARTITION OF dev.transactions FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2026_07 PARTITION OF dev.transactions FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2026_08 PARTITION OF dev.transactions FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2026_09 PARTITION OF dev.transactions FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2026_10 PARTITION OF dev.transactions FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2026_11 PARTITION OF dev.transactions FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE IF NOT EXISTS dev.transactions_2026_12 PARTITION OF dev.transactions FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');
```
3. **stores**
   - Thông tin cửa hàng
```sql
CREATE TABLE IF NOT EXISTS dev.stores (
    store_id SERIAL PRIMARY KEY,
    country VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    store_name VARCHAR(255) NOT NULL,
    number_of_employees INT NOT NULL,
    zip_code VARCHAR(20) NOT NULL,
    latitude NUMERIC(10, 6) NOT NULL,
    longitude NUMERIC(10, 6) NOT NULL
);
```
4. **products**
   - Catalog sản phẩm
```sql
CREATE TABLE dev.products (
    product_id INT PRIMARY KEY,
    category VARCHAR(50),
    sub_category VARCHAR(50),
    description_pt TEXT,
    description_de TEXT,
    description_fr TEXT,
    description_es TEXT,
    description_en TEXT,
    description_zh TEXT,
    color VARCHAR(20),
    sizes VARCHAR(100),
    production_cost DECIMAL(10,2)
);
```
5. **employees**
   - Thông tin nhân viên
```sql
CREATE TABLE IF NOT EXISTS dev.employees (
    employee_id SERIAL PRIMARY KEY,
    store_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    position VARCHAR(100) NOT NULL
);
```
6. **discounts**
   - Chương trình giảm giá
```sql
CREATE TABLE IF NOT EXISTS dev.discounts (
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    discount FLOAT NOT NULL,
    description TEXT,
    category VARCHAR(100),
    sub_category VARCHAR(100)
);
```
7. **weather_forecast**
   - Dữ liệu thời tiết
   - Phân vùng theo năm
```sql
CREATE TABLE IF NOT EXISTS dev.weather_forecast (
    id SERIAL,
    date DATE NOT NULL,
    temperature_2m FLOAT,
    rain FLOAT,
    weather_code INTEGER,
    cloud_cover FLOAT,
    wind_speed_10m FLOAT,
    soil_temperature_0cm FLOAT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, id)
) PARTITION BY RANGE (date);

-- Year partitions
CREATE TABLE IF NOT EXISTS dev.weather_forecast_2025 
    PARTITION OF dev.weather_forecast
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS dev.weather_forecast_2026 
    PARTITION OF dev.weather_forecast
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

-- Index
CREATE INDEX IF NOT EXISTS idx_weather_forecast_date ON dev.weather_forecast(date);
```   

### 2.1 Dữ liệu dataMart - Schema gold
Các bảng dim ap dụng SCD2
Bảng FACT lưu key từ các bảng Dim và Amount,number,...
1. **dim_store**
```sql
CREATE TABLE IF NOT EXISTS dev.dim_store (
    store_key SERIAL PRIMARY KEY,
    store_id INT NOT NULL,
    country VARCHAR(100),
    city VARCHAR(100),
    store_name VARCHAR(255),
    number_of_employees INT,
    zip_code VARCHAR(20),
    latitude NUMERIC(10, 6),
    longitude NUMERIC(10, 6),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    UNIQUE(store_id, valid_from)
);
```
2. **dim_product**
```sql
CREATE TABLE IF NOT EXISTS dev.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    category VARCHAR(50),
    sub_category VARCHAR(50),
    description_en TEXT,
    color VARCHAR(20),
    sizes VARCHAR(100),
    production_cost DECIMAL(10,2),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    UNIQUE(product_id, valid_from)
);
```
3. **dim_customer**
```sql
CREATE TABLE IF NOT EXISTS dev.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    name VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),
    gender CHAR(1),
    age_group VARCHAR(20),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    UNIQUE(customer_id, valid_from)
);
```
4. **dim_employee**
```sql
CREATE TABLE IF NOT EXISTS dev.employees (
    employee_key SERIAL PRIMARY KEY,
    employee_id INT NOT NULL,
    store_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    position VARCHAR(100) NOT NULL,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    UNIQUE(employee_id, valid_from),
);
```
5. **dim_date**
```sql
CREATE TABLE IF NOT EXISTS dev.dim_date (
    date_key INT PRIMARY KEY,
    date DATE NOT NULL,
    year INT,
    month INT,
    day INT,
    quarter INT,
    day_of_week INT,
    week_of_year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    season VARCHAR(10),
    UNIQUE(date)
);
```
6. **dim_currency**
7. **dim_weather**
7. **FCT_Sale_byWeather**
dựa vào bảng transaction có thể thấy các hóa đơn có nhiều line giao dịch cần phần tích nhiều hơn về nghiệp vụ để xây dựng các data_mart nên mình xin tạm dừng ở đây