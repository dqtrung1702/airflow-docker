import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED
import contextlib
from typing import Optional
import logging
from Sale.config import AppConfig
from Sale.error_handler import log_exceptions

logger = logging.getLogger(__name__)

class BaseProcessor:
    connection_pool: Optional[SimpleConnectionPool] = None

    def __init__(self, file_path, table_name, delete_sql, insert_sql, pg_hook, 
                 chunk_size=None, max_workers=None):
        self.file_path = file_path
        self.table_name = table_name
        self.delete_sql = delete_sql
        self.insert_sql = insert_sql
        self.pg_hook = pg_hook
        self.chunk_size = chunk_size or AppConfig.DEFAULT_CHUNK_SIZE
        self.max_workers = max_workers or AppConfig.DEFAULT_MAX_WORKERS
        self._initialize_pool(pg_hook)
        
    @classmethod
    def _initialize_pool(cls, pg_hook):
        if cls.connection_pool is None:
            conn = pg_hook.get_conn()
            conn_params = {
                "host": conn.info.host,
                "port": conn.info.port,
                "user": conn.info.user,
                "password": conn.info.password,
                "database": conn.info.dbname,
            }
            min_conn = AppConfig.DB_POOL_MIN_CONN
            max_conn = max(AppConfig.DB_POOL_MAX_CONN, cls.max_workers * 2)
            
            logger.info(f"Initializing connection pool with {min_conn}-{max_conn} connections")
            cls.connection_pool = SimpleConnectionPool(
                minconn=min_conn,
                maxconn=max_conn,
                **conn_params
            )

    @contextlib.contextmanager
    def _get_db_connection(self):
        conn = self._get_connection()
                try:
            yield conn
        finally:
            self._release_connection(conn)

    def _get_connection(self):
        if not BaseProcessor.connection_pool:
            self._initialize_pool(self.pg_hook)
        return BaseProcessor.connection_pool.getconn()

    def _release_connection(self, conn):
        if BaseProcessor.connection_pool:
            BaseProcessor.connection_pool.putconn(conn)
    
    @log_exceptions
    def process_and_load(self):
        """Process data from file and load into database."""
        if not os.path.exists(self.file_path):
            logger.error(f"File not found: {self.file_path}")
            raise FileNotFoundError(f"File not found: {self.file_path}")
            
        records_processed = 0
        start_time = pd.Timestamp.now()
        
        try:
            # Estimate total rows for progress reporting
            total_rows = sum(len(chunk) for chunk in pd.read_csv(self.file_path, chunksize=self.chunk_size))
            logger.info(f"Processing {total_rows} records from {self.file_path} for table {self.table_name}")
            
            with self._get_db_connection() as conn:
                with closing(conn.cursor()) as cursor:
                    try:
                        # Set session characteristics for better performance
                        cursor.execute("SET SESSION synchronous_commit = OFF")
                        
                        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                            futures = []
                            
                            # Process data in chunks
                            for chunk_index, chunk in enumerate(pd.read_csv(self.file_path, chunksize=self.chunk_size)):
                                chunk = chunk.replace({np.nan: None})
                                chunk_size = len(chunk)
                                records_processed += chunk_size
                                
                                logger.debug(f"Processing chunk {chunk_index+1} with {chunk_size} records")
                                futures.append(executor.submit(self._process_chunk, chunk, cursor))
                            
                            # Wait for all futures to complete
                            for future in futures:
                                future.result()
                    conn.commit()
                    logger.info("Data successfully loaded into database")
                        
                        duration = (pd.Timestamp.now() - start_time).total_seconds()
                        logger.info(f"Successfully processed {records_processed} records in {duration:.2f} seconds "
                                   f"({records_processed/duration:.2f} records/second)")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Database error: {str(e)}")
                        logger.error(f"Error processing data: {str(e)}")
                    raise
                        
        except Exception as e:
            logger.error(f"Failed to process {self.file_path}: {str(e)}")
            raise
        
    def _process_chunk(self, chunk, cursor):
        """Process a chunk of data."""
        if chunk.empty:
            return
            
        try:
            # Set isolation level
            cursor.connection.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
            
            # Start transaction
            cursor.connection.commit()
            
            # Convert DataFrame to tuples
            rows = [tuple(row) for row in chunk.itertuples(index=False)]
            
            # Delete existing records
            self._delete_existing_records(chunk, cursor)
            
            # Insert new records
            self._insert_new_records(rows, cursor)
            
            # Commit transaction
            cursor.connection.commit()
            
        except Exception as e:
            # Rollback on any error
            cursor.connection.rollback()
            logger.error(f"Error processing chunk: {str(e)}")
            raise

    def _delete_existing_records(self, chunk, cursor):
        """Delete existing records based on primary keys in chunk."""
        try:
            if chunk.empty:
                return
                
            # Extract unique primary keys
            unique_keys = list(set(chunk.iloc[:, 0]))
            if not unique_keys:
                return
                
            cursor.execute(self.delete_sql, (unique_keys,))
            
        except Exception as e:
            logger.error(f"Error deleting records: {str(e)}")
            cursor.connection.rollback()
            raise

    def _insert_new_records(self, rows, cursor):
        """Insert new records with conflict handling."""
        if not rows:
            return
            
        try:
            # Add ON CONFLICT clause for idempotent inserts
            modified_sql = self.insert_sql + " ON CONFLICT DO NOTHING"
            
            # Insert in batches for better performance
            batch_size = AppConfig.DB_BATCH_SIZE
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                cursor.executemany(modified_sql, batch)
                
        except Exception as e:
            logger.error(f"Error inserting records: {str(e)}")
            raise

    @classmethod
    def close_pool(cls):
        """Close the connection pool."""
        if cls.connection_pool:
            cls.connection_pool.closeall()
            cls.connection_pool = None
            logger.info("Database connection pool closed")

    def __del__(self):
        """Destructor to ensure resources are cleaned up."""
        # Note: We don't close the pool here as it's shared
        pass