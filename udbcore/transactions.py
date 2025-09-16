"""
Transaction management utilities for DuckDB databases.
"""
import logging
from typing import List, Optional, Callable, Any
from contextlib import contextmanager
from .ddb import DuckDB


class TransactionManager:
    """
    Manages database transactions with commit/rollback capabilities.
    """
    
    def __init__(self, duckdb_instance: DuckDB):
        """
        Initialize the transaction manager.
        
        :param duckdb_instance: DuckDB instance to manage transactions for
        """
        self.duckdb = duckdb_instance
        self.in_transaction = False
        self.logger = logging.getLogger(__name__)
    
    def begin_transaction(self):
        """Begin a new transaction."""
        if self.in_transaction:
            raise RuntimeError("Transaction already in progress")
        
        conn = self.duckdb.connect()
        conn.execute("BEGIN TRANSACTION;")
        self.in_transaction = True
        self.logger.debug(f"Transaction started for {self.duckdb.path}")
    
    def commit(self):
        """Commit the current transaction."""
        if not self.in_transaction:
            raise RuntimeError("No transaction in progress")
        
        conn = self.duckdb.connect()
        conn.execute("COMMIT;")
        self.in_transaction = False
        self.logger.debug(f"Transaction committed for {self.duckdb.path}")
    
    def rollback(self):
        """Rollback the current transaction."""
        if not self.in_transaction:
            self.logger.warning("No transaction in progress to rollback")
            return
        
        try:
            conn = self.duckdb.connect()
            conn.execute("ROLLBACK;")
            self.in_transaction = False
            self.logger.debug(f"Transaction rolled back for {self.duckdb.path}")
        except Exception as e:
            self.logger.warning(f"Error during rollback: {e}")
            self.in_transaction = False  # Reset state anyway
    
    def execute_in_transaction(self, queries: List[str]) -> List[Any]:
        """
        Execute multiple queries within a single transaction.
        
        :param queries: List of SQL queries to execute
        :return: List of results from each query
        """
        results = []
        self.begin_transaction()
        
        try:
            conn = self.duckdb.connect()
            for query in queries:
                result = conn.execute(query).fetchall()
                results.append(result)
            self.commit()
        except Exception as e:
            self.rollback()
            self.logger.error(f"Transaction failed for {self.duckdb.path}: {e}")
            raise
        
        return results
    
    @contextmanager
    def transaction(self):
        """
        Context manager for automatic transaction handling.
        
        Usage:
            with transaction_manager.transaction():
                # Execute queries here
                conn.execute("INSERT ...")
                conn.execute("UPDATE ...")
                # Automatically commits on success, rolls back on exception
        """
        self.begin_transaction()
        try:
            yield self
            self.commit()
        except Exception as e:
            self.logger.error(f"Transaction failed for {self.duckdb.path}: {e}")
            try:
                self.rollback()
            except Exception as rollback_error:
                self.logger.error(f"Rollback also failed: {rollback_error}")
            raise


class BatchOperationManager:
    """
    Manages batch operations for efficient bulk data processing.
    """
    
    def __init__(self, duckdb_instance: DuckDB, batch_size: int = 1000):
        """
        Initialize the batch operation manager.
        
        :param duckdb_instance: DuckDB instance to perform operations on
        :param batch_size: Number of operations to batch together
        """
        self.duckdb = duckdb_instance
        self.batch_size = batch_size
        self.logger = logging.getLogger(__name__)
    
    def batch_insert(self, table_name: str, columns: List[str], data: List[List[Any]]) -> int:
        """
        Perform batch insert operations.
        
        :param table_name: Name of the table to insert into
        :param columns: List of column names
        :param data: List of rows to insert (each row is a list of values)
        :return: Total number of rows inserted
        """
        if not data:
            return 0
        
        conn = self.duckdb.connect()
        total_inserted = 0
        
        # Prepare the INSERT statement
        placeholders = ','.join(['?' for _ in columns])
        columns_str = ','.join(columns)
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        # Process data in batches
        for i in range(0, len(data), self.batch_size):
            batch = data[i:i + self.batch_size]
            
            try:
                # Use DuckDB's executemany for efficient batch insertion
                conn.executemany(insert_sql, batch)
                total_inserted += len(batch)
                self.logger.debug(f"Inserted batch of {len(batch)} rows into {table_name}")
            except Exception as e:
                self.logger.error(f"Failed to insert batch into {table_name}: {e}")
                raise
        
        return total_inserted
    
    def batch_update(self, table_name: str, set_clause: str, where_conditions: List[str], parameters: List[List[Any]]) -> int:
        """
        Perform batch update operations.
        
        :param table_name: Name of the table to update
        :param set_clause: SET clause for the UPDATE statement (with placeholders)
        :param where_conditions: List of WHERE conditions (one per update)
        :param parameters: List of parameter lists for each update
        :return: Number of update statements executed
        """
        if not parameters:
            return 0
        
        conn = self.duckdb.connect()
        statements_executed = 0
        
        # Process updates in batches
        for i in range(0, len(parameters), self.batch_size):
            batch_params = parameters[i:i + self.batch_size]
            batch_conditions = where_conditions[i:i + self.batch_size]
            
            try:
                # Execute each update in the batch
                for j, (params, condition) in enumerate(zip(batch_params, batch_conditions)):
                    update_sql = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
                    conn.execute(update_sql, params)
                    statements_executed += 1
                
                self.logger.debug(f"Updated batch of {len(batch_params)} statements in {table_name}")
            except Exception as e:
                self.logger.error(f"Failed to update batch in {table_name}: {e}")
                raise
        
        return statements_executed
    
    def batch_delete(self, table_name: str, where_conditions: List[str], parameters: List[List[Any]]) -> int:
        """
        Perform batch delete operations.
        
        :param table_name: Name of the table to delete from
        :param where_conditions: List of WHERE conditions (one per delete)
        :param parameters: List of parameter lists for each delete
        :return: Number of delete statements executed
        """
        if not parameters:
            return 0
        
        conn = self.duckdb.connect()
        statements_executed = 0
        
        # Process deletes in batches
        for i in range(0, len(parameters), self.batch_size):
            batch_params = parameters[i:i + self.batch_size]
            batch_conditions = where_conditions[i:i + self.batch_size]
            
            try:
                # Execute each delete in the batch
                for params, condition in zip(batch_params, batch_conditions):
                    delete_sql = f"DELETE FROM {table_name} WHERE {condition}"
                    conn.execute(delete_sql, params)
                    statements_executed += 1
                
                self.logger.debug(f"Deleted batch of {len(batch_params)} statements from {table_name}")
            except Exception as e:
                self.logger.error(f"Failed to delete batch from {table_name}: {e}")
                raise
        
        return statements_executed