from airflow import DAG
from airflow.decorators import task
from datetime import datetime

# Cấu hình DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 15),
}

# Khởi tạo DAG
with DAG(
    "parallel_processing_extended_dag",
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,  
    tags=["example"],
) as dag:

    @task
    def generate_list_1():
        """Tạo danh sách dữ liệu 1."""
        items = [{"item": f"task1_{i}"} for i in range(5)]
        print(f"Generated list 1: {items}")
        return items

    @task
    def generate_list_2():
        """Tạo danh sách dữ liệu 2."""
        items = [{"item": f"task3_{i}"} for i in range(5)]
        print(f"Generated list 2: {items}")
        return items

    @task
    def process_item_1(item):
        """Xử lý từng item từ danh sách 1."""
        print(f"Processing from list 1: {item}")

    @task
    def process_item_2(item):
        """Xử lý từng item từ danh sách 2."""
        print(f"Processing from list 2: {item}")

    # Tạo danh sách song song
    list1 = generate_list_1()
    list2 = generate_list_2()

    # Xử lý song song, nhưng đảm bảo đúng luồng chạy
    task1 = process_item_1.expand_kwargs(list1)
    task2 = process_item_2.expand_kwargs(list2)

    [list1 >> task1, list2 >> task2]  # Cấu trúc DAG đúng
