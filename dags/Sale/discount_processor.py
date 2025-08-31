from Sale.data_processor import BaseProcessor
from Sale.config import CONFIG
import pandas as pd
import numpy as np
import os
from concurrent.futures import ThreadPoolExecutor

class DiscountProcessor(BaseProcessor):
    def __init__(self, file_path, pg_hook):
        """Initialize the DiscountProcessor."""
        config = CONFIG["discounts"]
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
        
        # Expected column mappings
        self.column_mappings = {
            'Start': 'start_date',
            'End': 'end_date',
            'Discont': 'discount',
            'Description': 'description',
            'Category': 'category',
            'Sub Category': 'sub_category'
        }

    def process_and_load(self):
        """Override process_and_load to add column validation"""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

        # Read and validate CSV structure
        try:
            # Read first few rows to check structure
            sample = pd.read_csv(self.file_path, nrows=1)

            # Check if we can map the columns
            missing_cols = []
            for csv_col, db_col in self.column_mappings.items():
                if csv_col not in sample.columns:
                    missing_cols.append(csv_col)

            if missing_cols:
                raise ValueError(f"Missing required columns in CSV: {missing_cols}")

            # Proceed with processing using mapped columns
            with pd.read_csv(self.file_path, chunksize=self.chunk_size) as reader:
                with self._get_db_connection() as conn:
                    cursor = conn.cursor()
                    try:
                        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                            futures = []
                            for chunk_index, chunk in enumerate(reader):
                                # Rename columns to match database
                                chunk = chunk.rename(columns=self.column_mappings)
                                chunk = chunk.replace({np.nan: None})
                                futures.append(executor.submit(self._process_chunk, chunk, cursor))

                            for future in futures:
                                future.result()

                        conn.commit()
                    except Exception as e:
                        conn.rollback()
                        raise
                    finally:
                        cursor.close()

        except Exception as e:
            raise

    def _delete_existing_records(self, chunk, cursor):
        """Custom delete logic for discounts"""
        try:
            start_date_col = 'start_date'
            end_date_col = 'end_date'
            
            if start_date_col not in chunk.columns or end_date_col not in chunk.columns:
                raise KeyError(f"Missing mapped columns: {start_date_col} or {end_date_col}")

            min_start_date = pd.to_datetime(chunk[start_date_col].min())
            max_end_date = pd.to_datetime(chunk[end_date_col].max())
            params = (min_start_date.date(), max_end_date.date())
            cursor.execute(self.delete_sql, params)
        except Exception as e:
            raise

    