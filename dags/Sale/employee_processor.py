from Sale.config import CONFIG
from Sale.data_processor import BaseProcessor

class EmployeeProcessor(BaseProcessor):
    def __init__(self, file_path, pg_hook):
        config = CONFIG["employees"]
        chunk_size = CONFIG["chunk_size"]
        max_workers = CONFIG["max_workers"]
        super().__init__(file_path, 
                         config["table_name"], 
                         config["delete_sql"], 
                         config["insert_sql"], 
                         pg_hook,
                         chunk_size=chunk_size,  # Mỗi chunk gồm 5000 dòng
                         max_workers=max_workers     # Sử dụng 8 luồng xử lý song song
                        )
