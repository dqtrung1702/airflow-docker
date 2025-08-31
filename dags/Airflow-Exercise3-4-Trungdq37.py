from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task, dag
from datetime import datetime, timedelta
import json
import os
import pandas as pd
import io

def generate_hourly_intervals():
    return [{'hour': hour} for hour in range(24)]

@task
def fetch_weather_data(hour, ds=None):
    today = ds if ds else datetime.now().strftime('%Y-%m-%d')
    save_path = f"/tmp/data/raw/{today}/weather_data_{today}_{hour}.json"
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    http_hook = HttpHook(method='GET', http_conn_id='weather_api')
    endpoint = f"archive-api.open-meteo.com/v1/archive?latitude=10.7769&longitude=106.7009&start_date={ds}&end_date={ds}&hourly=temperature_2m,wind_speed_10m,relative_humidity_2m"
    
    response = http_hook.run(endpoint)
    
    if response.status_code == 200:
        with open(save_path, 'w') as f:
            json.dump(response.json(), f)
    else:
        raise Exception(f"Failed to fetch weather data. Status code: {response.status_code}")

@task
def process_and_load_weather_data(hour):
    today = datetime.now().strftime('%Y-%m-%d')
    json_path = f"/tmp/data/raw/{today}/weather_data_{today}_{hour}.json"
    
    if not os.path.exists(json_path):
        return
    
    with open(json_path, 'r') as f:
        data = json.load(f)
    
    if 'hourly' not in data:
        raise ValueError("Missing 'hourly' key in API response")
    
    hourly_data = data['hourly']
    df = pd.DataFrame({
        'timestamp': hourly_data.get('time', []),
        'temperature': hourly_data.get('temperature_2m', []),
        'wind_speed': hourly_data.get('wind_speed_10m', []),
        'humidity': hourly_data.get('relative_humidity_2m', [])
    })
    
    if df.empty:
        return
    
    pg_hook = PostgresHook(postgres_conn_id='vnstock_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            timestamp TIMESTAMP PRIMARY KEY,
            temperature FLOAT,
            wind_speed FLOAT,
            humidity FLOAT
        );
    """)
    
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO weather_data (timestamp, temperature, wind_speed, humidity)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (timestamp) DO UPDATE 
            SET temperature = EXCLUDED.temperature,
                wind_speed = EXCLUDED.wind_speed,
                humidity = EXCLUDED.humidity;
        """, (row['timestamp'], row['temperature'], row['wind_speed'], row['humidity']))
    
    conn.commit()
    cursor.close()
    conn.close()

@task
def generate_report():
    pg_hook = PostgresHook(postgres_conn_id='vnstock_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_report (
            report_date DATE PRIMARY KEY,
            avg_temperature FLOAT,
            avg_wind_speed FLOAT
        );
    """)
    
    cursor.execute("""
        INSERT INTO weather_report (report_date, avg_temperature, avg_wind_speed)
        SELECT CURRENT_DATE, AVG(temperature), AVG(wind_speed)
        FROM weather_data
        WHERE timestamp >= NOW() - INTERVAL '7 days'
        ON CONFLICT (report_date) DO UPDATE
        SET avg_temperature = EXCLUDED.avg_temperature,
            avg_wind_speed = EXCLUDED.avg_wind_speed;
    """)
    
    conn.commit()
    cursor.close()
    conn.close()

@dag(schedule_interval='0 6 * * *', start_date=datetime(2025, 3, 15), catchup=True)
def weather_pipeline():
    intervals = generate_hourly_intervals()
    
    fetched_data = fetch_weather_data.expand_kwargs(intervals)
    processed_data = process_and_load_weather_data.expand_kwargs(intervals)
    
    fetched_data >> processed_data >> generate_report()

weather_pipeline()
