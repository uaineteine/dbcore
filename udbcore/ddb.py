#base imports
import random
import time
import logging
from typing import Any, List, Optional, Dict
from contextlib import contextmanager

#package imports
import duckdb
from pandas import DataFrame

#dbcore imports
from .db import DB

class DuckDB(DB):
    """A DuckDB database file with enhanced functionality."""
    
    def __init__(self, path: str, name: str, enforce_name: bool = True, 
                 retry_attempts: int = 3, retry_delay: float = 1.0):
        # Validate that the path ends with .duckdb extension
        if not path.lower().endswith('.duckdb'):
            raise ValueError(f"Database file must have a .duckdb extension. Got: {path}")
        
        super().__init__(path, name, enforce_name)
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.logger = logging.getLogger(__name__)
        
        # Initialize health monitor as None - will be created on first use
        self._health_monitor = None

    def connect(self):
        """Connects to the DuckDB database and returns the connection object."""
        if not self.loaded:
            self.conn = duckdb.connect(self.path)
            self.loaded = True
            self.logger.debug(f"Connected to database: {self.path}")
        return self.conn

    def disconnect(self):
        """Disconnects from the DuckDB database if connected."""
        if getattr(self, 'loaded', False) and hasattr(self, 'conn') and self.conn is not None:
            try:
                self.conn.close()
                self.logger.debug(f"Disconnected from database: {self.path}")
            except Exception as e:
                self.logger.warning(f"Error during disconnect: {e}")
            finally:
                self.loaded = False
                self.conn = None

    def run_query(self, query: str, parameters: Optional[List[Any]] = None):
        """
        Runs a SQL query on the DuckDB database and returns the result.
        
        :param query: SQL query to execute
        :param parameters: Optional parameters for the query
        :return: Query results
        """
        self.connect()
        return self._execute_with_retry(
            lambda conn: conn.execute(query, parameters or []).fetchall(),
            query
        )
    
    def run_query_single(self, query: str, parameters: Optional[List[Any]] = None):
        """
        Runs a SQL query and returns a single result.
        
        :param query: SQL query to execute
        :param parameters: Optional parameters for the query
        :return: Single result or None
        """
        self.connect()
        return self._execute_with_retry(
            lambda conn: conn.execute(query, parameters or []).fetchone(),
            query
        )
    
    def execute_non_query(self, query: str, parameters: Optional[List[Any]] = None) -> bool:
        """
        Executes a non-query SQL statement (INSERT, UPDATE, DELETE).
        
        :param query: SQL statement to execute
        :param parameters: Optional parameters for the query
        :return: True if successful, False otherwise (DuckDB doesn't provide rowcount)
        """
        def execute_func(conn):
            conn.execute(query, parameters or [])
            # DuckDB doesn't provide meaningful rowcount, so we return True on success
            return True
        
        self.connect()
        return self._execute_with_retry(execute_func, query)
    
    def execute_many(self, query: str, parameters_list: List[List[Any]]) -> int:
        """
        Executes a query multiple times with different parameters.
        
        :param query: SQL query to execute
        :param parameters_list: List of parameter lists
        :return: Number of statements executed successfully
        """
        def execute_func(conn):
            success_count = 0
            for params in parameters_list:
                conn.execute(query, params)
                success_count += 1
            return success_count
        
        self.connect()
        return self._execute_with_retry(execute_func, query)
    
    def _execute_with_retry(self, execute_func, query: str):
        """
        Execute a function with retry logic and monitoring.
        
        :param execute_func: Function that takes a connection and executes the operation
        :param query: SQL query being executed (for logging)
        :return: Result of the execution
        """
        start_time = time.time()
        last_exception = None
        
        for attempt in range(self.retry_attempts):
            try:
                conn = self.connect()
                result = execute_func(conn)
                
                # Record successful query execution
                execution_time = time.time() - start_time
                if self._health_monitor:
                    self._health_monitor.record_query(query, execution_time)
                
                return result
                
            except Exception as e:
                last_exception = e
                self.logger.warning(f"Query attempt {attempt + 1} failed: {e}")
                
                # Close and reset connection on error
                try:
                    self.disconnect()
                except:
                    pass
                
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff
                else:
                    execution_time = time.time() - start_time
                    self.logger.error(f"Query failed after {self.retry_attempts} attempts: {query[:100]}...")
                    if self._health_monitor:
                        self._health_monitor.record_query(f"FAILED: {query}", execution_time)
        
        # All attempts failed
        self.logger.error(f"All retry attempts failed. Last exception: {last_exception}")
        raise last_exception
    
    def attach_additional_db(self, path: str, alias: str) -> bool:
        """
        Attaches an additional DuckDB database at the given path to the current connection with the given alias.
        
        :param path: Path to the database file to attach
        :param alias: Alias for the attached database
        :return: True if successful, False otherwise
        """
        self.connect()
        try:
            return self._execute_with_retry(
                lambda conn: conn.execute(f"ATTACH DATABASE '{path}' AS {alias};"),
                f"ATTACH DATABASE '{path}' AS {alias};"
            ) is not None
        except Exception as e:
            self.logger.error(f"Failed to attach database {path} as {alias}: {e}")
            return False
    
    def detach_database(self, alias: str) -> bool:
        """
        Detaches a previously attached database.
        
        :param alias: Alias of the database to detach
        :return: True if successful, False otherwise
        """
        if self.loaded is False:
            self.logger.warning("Database not loaded; cannot detach.")
            return False
        
        try:
            return self._execute_with_retry(
                lambda conn: conn.execute(f"DETACH DATABASE {alias};"),
                f"DETACH DATABASE {alias};"
            ) is not None
        except Exception as e:
            self.logger.error(f"Failed to detach database {alias}: {e}")
            return False
    
    def get_table_names(self) -> List[str]:
        """
        Get a list of all table names in the database.
        
        :return: List of table names
        """
        self.connect()
        try:
            result = self.run_query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            )
            return [row[0] for row in result]
        except Exception as e:
            self.logger.error(f"Failed to get table names: {e}")
            return []
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        :param table_name: Name of the table to check
        :return: True if table exists, False otherwise
        """
        self.connect()
        try:
            result = self.run_query_single(
                "SELECT 1 FROM information_schema.tables WHERE table_name=? AND table_schema='main'",
                [table_name]
            )
            return result is not None
        except Exception as e:
            self.logger.error(f"Failed to check if table {table_name} exists: {e}")
            return False
    
    def get_row_count(self, table_name: str) -> int:
        """
        Get the number of rows in a table.
        
        :param table_name: Name of the table
        :return: Number of rows, or -1 if error
        """
        self.connect()
        try:
            result = self.run_query_single(f"SELECT COUNT(*) FROM {table_name}")
            return result[0] if result else 0
        except Exception as e:
            self.logger.error(f"Failed to get row count for table {table_name}: {e}")
            return -1
    
    def vacuum_database(self) -> bool:
        """
        Vacuum the database to reclaim space and optimize performance.
        
        :return: True if successful, False otherwise
        """
        self.connect()
        try:
            self._execute_with_retry(
                lambda conn: conn.execute("VACUUM;"),
                "VACUUM;"
            )
            self.logger.info(f"Successfully vacuumed database: {self.path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to vacuum database: {e}")
            return False
    
    def analyze_database(self) -> bool:
        """
        Analyze the database to update query optimizer statistics.
        
        :return: True if successful, False otherwise
        """
        self.connect()
        try:
            self._execute_with_retry(
                lambda conn: conn.execute("ANALYZE;"),
                "ANALYZE;"
            )
            self.logger.info(f"Successfully analyzed database: {self.path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to analyze database: {e}")
            return False
    
    @staticmethod
    def generate_temp_table_name(prefix: str = "temp") -> str:
        """
        Generate a unique temporary table name.
        
        :param prefix: Prefix for the temporary table name
        :return: Unique temporary table name
        """
        unique_id = random.randint(0, 1000)
        return f"{prefix}_{unique_id}"

    def register_temp_table(self, df: DataFrame, prefix: str = "temp") -> str:
        """
        Register a Pandas DataFrame as a temporary table in the database.
        
        :param df: Pandas DataFrame to register
        :param prefix: Prefix for the temporary table name
        :return: Name of the registered temporary table
        """
        self.connect()
        table_name = DuckDB.generate_temp_table_name(prefix)
        try:
            self.conn.register(table_name, df)
            self.logger.debug(f"Registered temporary table: {table_name}")
            return table_name
        except Exception as e:
            self.logger.error(f"Failed to register temporary table: {e}")
            raise

    def unregister_temp_table(self, table_name: str) -> bool:
        """
        Unregister a previously registered temporary table.
        
        :param table_name: Name of the temporary table to unregister
        :return: True if successful, False otherwise
        """
        self.connect()
        try:
            self.conn.unregister(table_name)
            self.logger.debug(f"Unregistered temporary table: {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to unregister temporary table {table_name}: {e}")
            return False

    @contextmanager
    def transaction(self):
        """
        Context manager for database transactions.
        
        Usage:
            with db.transaction():
                db.execute_non_query("INSERT ...")
                db.execute_non_query("UPDATE ...")
        """
        conn = self.connect()
        conn.execute("BEGIN TRANSACTION;")
        try:
            yield self
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise
    
    def get_health_monitor(self):
        """
        Get or create a health monitor for this database.
        
        :return: HealthMonitor instance
        """
        if self._health_monitor is None:
            try:
                from .monitoring import HealthMonitor
                self._health_monitor = HealthMonitor(self)
            except ImportError:
                self.logger.warning("Health monitoring not available")
                return None
        return self._health_monitor
    
    def get_database_info(self) -> Dict[str, Any]:
        """
        Get comprehensive information about the database.
        
        :return: Dictionary with database information
        """
        info = {
            'path': self.path,
            'name': self.name,
            'loaded': self.loaded,
            'table_count': len(self.get_table_names()),
        }
        
        # Add health monitor stats if available
        health_monitor = self.get_health_monitor()
        if health_monitor:
            info['health_stats'] = health_monitor.get_database_statistics()
            info['query_stats'] = health_monitor.get_query_statistics()
        
        return info
