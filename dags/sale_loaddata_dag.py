from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from Sale.customer_processor import CustomerProcessor
from Sale.transaction_processor import TransactionProcessor
from Sale.stores_processor import StoreProcessor
from Sale.products_processor import ProductProcessor
from Sale.employee_processor import EmployeeProcessor
from Sale.discount_processor import DiscountProcessor
from Sale.weather_forecast import WeatherForecastProcessor
from Sale.file_splitter import FileSplitter
import os
from typing import List
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from Sale.config import CONFIG
postgres_conn_id = CONFIG["postgres_conn_id"]

# Get the directory of the current DAG file
DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))

def get_postgres_hook():
    """Initialize and return a PostgresHook."""
    return PostgresHook(postgres_conn_id=postgres_conn_id)


@task
def load_customer_data(pg_hook):
    """Task to process and load customer data."""
    file_path = os.path.join(DAGS_FOLDER, "FileStorage/customers.csv")
    processor = CustomerProcessor(file_path=file_path, pg_hook=pg_hook)
    processor.process_and_load()


@task
def load_store_data(pg_hook):
    """Task to process and load store data."""
    file_path = os.path.join(DAGS_FOLDER, "FileStorage/stores.csv")
    processor = StoreProcessor(file_path=file_path, pg_hook=pg_hook)
    processor.process_and_load()


@task
def load_product_data(pg_hook):
    """Task to process and load product data."""
    file_path = os.path.join(DAGS_FOLDER, "FileStorage/products.csv")
    processor = ProductProcessor(file_path=file_path, pg_hook=pg_hook)
    processor.process_and_load()


@task
def load_employee_data(pg_hook):
    """Task to process and load employee data."""
    file_path = os.path.join(DAGS_FOLDER, "FileStorage/employees.csv")
    processor = EmployeeProcessor(file_path=file_path, pg_hook=pg_hook)
    processor.process_and_load()


@task
def load_discount_data(pg_hook):
    """Task to process and load discount data."""
    file_path = os.path.join(DAGS_FOLDER, "FileStorage/discounts.csv")
    processor = DiscountProcessor(file_path=file_path, pg_hook=pg_hook)
    processor.process_and_load()

@task
def load_transaction_data(pg_hook):
    """Task to process and load discount data."""
    file_path = os.path.join(DAGS_FOLDER, "FileStorage/transactions_part_003.csv")
    processor = TransactionProcessor(file_path=file_path, pg_hook=pg_hook)
    processor.process_and_load()


@task
def load_weather_data(pg_hook):
    """Task to process and load weather forecast data."""
    processor = WeatherForecastProcessor(pg_hook=pg_hook)
    processor.process_and_load()


@task
def split_transaction_file() -> List[str]:
    """Split large transaction file into fixed number of chunks."""
    input_file = os.path.join(DAGS_FOLDER, "FileStorage/transactions.csv")
    output_dir = "/tmp/airflow/file_chunks"
    
    # Create directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Clear existing files in the output directory
    for filename in os.listdir(output_dir):
        file_path = os.path.join(output_dir, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
                print(f"Deleted existing file: {file_path}")
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")
    
    # Initialize splitter and split file
    splitter = FileSplitter(
        input_file=input_file,
        output_dir=output_dir,
        num_chunks=10
    )
    return splitter.split_file()


@task
def cleanup_chunk_files(file_paths: List[str]):
    """Clean up the split chunk files after processing."""
    for file_path in file_paths:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Removed temporary file: {file_path}")
        except Exception as e:
            print(f"Error removing file {file_path}: {e}")


@task
def process_chunk(file_paths: List[str], chunk_index: int, pg_hook) -> None:
    """Process a specific chunk of transaction data."""
    processor = TransactionProcessor(
        file_path=file_paths[chunk_index],
        pg_hook=pg_hook
    )
    processor.process_and_load()


@dag(
    dag_id="ingrest_data_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Combined DAG for loading data into PostgreSQL"
)
def ingrest_data_pipeline():
    # Initialize PostgresHook once
    pg_hook = get_postgres_hook()

    # Define tasks
    customer_task = load_customer_data(pg_hook)
    store_task = load_store_data(pg_hook)
    product_task = load_product_data(pg_hook)
    employee_task = load_employee_data(pg_hook)
    discount_task = load_discount_data(pg_hook)
    weather_task = load_weather_data(pg_hook)
    # transaction_task = load_transaction_data(pg_hook)
    # Create a task group for transaction processing
    with TaskGroup(group_id="transaction_processing") as transaction_group:
        # Split files and create tasks
        split_files = split_transaction_file()
        
        # Create fixed number of tasks with sequential dependencies
        chunk_tasks = []
        for i in range(10):
            task_id = f'load_transaction_chunk_{i+1}'
            chunk_task = process_chunk.override(task_id=task_id)(
                file_paths=split_files,
                chunk_index=i,
                pg_hook=pg_hook
            )
            chunk_tasks.append(chunk_task)

        cleanup = cleanup_chunk_files(split_files)

        # Alternative syntax using chain
        chain(split_files, *chunk_tasks, cleanup)

    # Define task dependencies
    [customer_task, store_task, product_task,
     employee_task, discount_task, weather_task, transaction_group] 
    # transaction_task
# Instantiate the DAG
dag = ingrest_data_pipeline()