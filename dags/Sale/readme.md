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

### 2.1 Schema dev
Các bảng chính:

1. **customers**
   - Thông tin khách hàng
   - Khóa chính: customer_id

2. **transactions**
   - Dữ liệu giao dịch
   - Phân vùng theo tháng
   - Khóa chính: (invoice_id, line, date)

3. **stores**
   - Thông tin cửa hàng
   - Bao gồm vị trí địa lý

4. **products**
   - Catalog sản phẩm
   - Mô tả đa ngôn ngữ

5. **employees**
   - Thông tin nhân viên
   - Liên kết với stores

6. **discounts**
   - Chương trình giảm giá
   - Theo thời gian và danh mục

7. **weather_forecast**
   - Dữ liệu thời tiết
   - Phân vùng theo năm


## 3. Cấu Trúc Code

### 3.1 Base Classes
1. **BaseProcessor**
   - Xử lý dữ liệu cơ bản
   - Connection pooling
   - Xử lý chunk
   - Retry mechanism

### 3.2 Specialized Processors
1. **TransactionProcessor**
   - Xử lý file lớn
   - Chia nhỏ file
   - Chunk size: 1000 rows

2. **CustomerProcessor**
   - Xử lý dữ liệu khách hàng
   - Validate email, phone

3. **StoreProcessor**
   - Quản lý thông tin cửa hàng
   - Xử lý tọa độ địa lý

4. **ProductProcessor**
   - Xử lý catalog sản phẩm
   - Hỗ trợ nhiều ngôn ngữ

5. **WeatherForecastProcessor**
   - Tích hợp API Open-Meteo
   - Tổng hợp dữ liệu theo ngày

### 3.3 Utility Classes
1. **FileSplitter**
   - Chia file lớn thành nhiều phần
   - Xử lý header CSV
   - Cleanup tự động

## 4. Luồng Xử Lý DAG

### 4.1 Các Nhóm Task
1. **Master Data**
   - Load customers
   - Load stores
   - Load products
   - Load employees

2. **Transaction Processing**
   - Split file
   - Process chunks
   - Cleanup

3. **Additional Data**
   - Weather forecast
   - Discounts

### 4.2 Quy Trình
1. Khởi tạo kết nối PostgreSQL
2. Load dữ liệu master song song
3. Xử lý transactions:
   - Chia file thành 10 phần
   - Xử lý song song các phần
   - Dọn dẹp file tạm
4. Load dữ liệu bổ sung

## 5. Cấu Hình Và Tối Ưu

### 5.1 Docker
- Memory limits cho workers
- Health checks cho services
- Volume mapping cho persistence
- Network configuration

### 5.2 Database
- Connection pooling
- Table partitioning
- Indexing
- Access control

### 5.3 Processing
- Chunk size: 1000 rows
- Worker threads: 4
- Retry mechanism
- Progress monitoring

## 6. Monitoring và Bảo Trì

### 6.1 Logs
- Progress tracking
- Error logging
- Performance metrics

### 6.2 Health Checks
- Service availability
- Database connections
- Worker status

## 7. Khuyến Nghị

### 7.1 Performance
- Tăng số lượng workers
- Optimize batch sizes
- Caching với Redis
- Query optimization

### 7.2 Reliability
- Circuit breakers
- Better error handling
- Data validation
- Backup strategy

### 7.3 Scaling
- Horizontal scaling
- Sharding
- Load balancing
- Resource management

## 8. Bảo Mật

### 8.1 Access Control
- Database credentials
- API authentication
- Service accounts

### 8.2 Data Protection
- Encryption at rest
- Secure connections
- Audit logging

## 9. Deployment

### 9.1 Requirements
- Docker và Docker Compose
- Minimum 8GB RAM
- 4 CPU cores
- 50GB storage

### 9.2 Setup Steps
1. Clone repository
2. Configure environment
3. Build Docker images
4. Initialize database
5. Start services
6. Verify deployment

### 9.3 Database Schema Documentation

#### Schema Overview

The database uses a schema named `dev` with appropriate permissions granted to the `airflow` user. The schema contains several tables designed to support a retail business system with weather data integration.

##### Permissions Setup

```sql
CREATE SCHEMA IF NOT EXISTS dev;

-- Grant permissions to airflow user
GRANT USAGE ON SCHEMA dev TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dev TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dev TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA dev GRANT ALL PRIVILEGES ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA dev GRANT ALL PRIVILEGES ON SEQUENCES TO airflow;
```

#### Table Structures

##### --1. Weather Forecast
--Partitioned table storing weather data by year.

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

##### --2. Customers
--Customer information storage.

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

##### --3. Discounts
--Promotional and discount information.

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

##### --4. Employees
--Employee records with store associations.

```sql
CREATE TABLE IF NOT EXISTS dev.employees (
    employee_id SERIAL PRIMARY KEY,
    store_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    position VARCHAR(100) NOT NULL
);
```

##### --5. Stores
--Store location and details.

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

##### --6. Transactions
--Monthly partitioned transaction records.

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

##### --7. Products
--Multi-language product catalog.

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

#### Partitioning Strategy
- Weather forecast: Yearly partitions
- Transactions: Monthly partitions
- Optimized for date-based queries and data management

#### Indexing
- Weather forecast: Date-based index
- All tables: Primary key constraints
- Additional indexes can be added based on query patterns