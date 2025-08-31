import os
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED
import contextlib
from typing import Optional
import psycopg2


class BaseProcessor:
    connection_pool: Optional[SimpleConnectionPool] = None

    def __init__(self, file_path, table_name, delete_sql, insert_sql, pg_hook, chunk_size=1000, max_workers=4):
        self.file_path = file_path
        self.table_name = table_name
        self.delete_sql = delete_sql
        self.insert_sql = insert_sql
        self.pg_hook = pg_hook
        self.chunk_size = chunk_size
        self.max_workers = max_workers
        self.__initialize_pool(pg_hook)

    def __initialize_pool(self, pg_hook):
        if BaseProcessor.connection_pool is None:
            conn = pg_hook.get_conn()
            conn_params = {
                "host": conn.info.host,
                "port": conn.info.port,
                "user": conn.info.user,
                "password": conn.info.password,
                "database": conn.info.dbname,
            }
            BaseProcessor.connection_pool = SimpleConnectionPool(
                minconn=2,
                maxconn=max(16, self.max_workers * 2),
                **conn_params
            )

    @contextlib.contextmanager
    def _get_db_connection(self):
        conn = self._get_connection()
        yield conn
        self._release_connection(conn)

    def _get_connection(self):
        return BaseProcessor.connection_pool.getconn()

    def _release_connection(self, conn):
        if BaseProcessor.connection_pool:
            BaseProcessor.connection_pool.putconn(conn)
    
    def process_and_load(self):
        records_processed = 0
        
        try:
            # First pass to count total rows
            total_rows = sum(len(chunk) for chunk in pd.read_csv(self.file_path, chunksize=self.chunk_size))
            
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                
                try:
                    # Set session characteristics
                    cursor.execute("SET SESSION synchronous_commit = OFF")
                    
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = []
                        
                        for chunk in pd.read_csv(self.file_path, chunksize=self.chunk_size):
                            chunk = chunk.replace({np.nan: None})
                            records_processed += len(chunk)                            
                            futures.append(executor.submit(self._process_chunk, chunk, cursor))
                        
                        # Wait for all futures to complete
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
        
    def _process_chunk(self, chunk, cursor):
        if not chunk.empty:
            try:
                # Set isolation level
                cursor.connection.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
                
                # Start transaction
                cursor.connection.commit()
                
                rows = [tuple(row) for row in chunk.itertuples(index=False)]
                self._delete_existing_records(chunk, cursor)
                self._insert_new_records(rows, cursor)
                
                # Commit transaction
                cursor.connection.commit()
                
            except Exception as e:
                # Rollback on any error
                cursor.connection.rollback()
                raise

    def _delete_existing_records(self, chunk, cursor):
        try:
            unique_keys = list(set(chunk.iloc[:, 0]))
            cursor.execute(self.delete_sql, (unique_keys,))
        except psycopg2.Error as e:
            cursor.connection.rollback()
            raise

    def _insert_new_records(self, rows, cursor):
        modified_sql = self.insert_sql + " ON CONFLICT DO NOTHING"
        cursor.executemany(modified_sql, rows)
        

    def __del__(self):
        if BaseProcessor.connection_pool:
            BaseProcessor.connection_pool.closeall()
            BaseProcessor.connection_pool = None