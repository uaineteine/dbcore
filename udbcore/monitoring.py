"""
Database health monitoring and statistics utilities.
"""
import time
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from .ddb import DuckDB


@dataclass
class QueryStats:
    """Statistics for a database query."""
    query: str
    execution_time: float
    rows_affected: int
    timestamp: float


@dataclass
class DatabaseStats:
    """Overall database statistics."""
    file_size_bytes: int
    table_count: int
    total_rows: int
    connection_count: int
    last_accessed: float
    uptime_seconds: float


class HealthMonitor:
    """
    Monitors database health and performance metrics.
    """
    
    def __init__(self, duckdb_instance: DuckDB, max_query_history: int = 1000):
        """
        Initialize the health monitor.
        
        :param duckdb_instance: DuckDB instance to monitor
        :param max_query_history: Maximum number of queries to keep in history
        """
        self.duckdb = duckdb_instance
        self.max_query_history = max_query_history
        self.query_history: List[QueryStats] = []
        self.start_time = time.time()
        self.logger = logging.getLogger(__name__)
    
    def record_query(self, query: str, execution_time: float, rows_affected: int = 0):
        """
        Record a query execution for statistics.
        
        :param query: SQL query that was executed
        :param execution_time: Time taken to execute the query in seconds
        :param rows_affected: Number of rows affected by the query
        """
        query_stat = QueryStats(
            query=query,
            execution_time=execution_time,
            rows_affected=rows_affected,
            timestamp=time.time()
        )
        
        self.query_history.append(query_stat)
        
        # Trim history if it exceeds maximum size
        if len(self.query_history) > self.max_query_history:
            self.query_history = self.query_history[-self.max_query_history:]
        
        # Log slow queries (> 1 second)
        if execution_time > 1.0:
            self.logger.warning(f"Slow query detected: {execution_time:.2f}s - {query[:100]}...")
    
    def get_query_statistics(self, last_n_queries: Optional[int] = None) -> Dict[str, Any]:
        """
        Get statistics about recent query performance.
        
        :param last_n_queries: Number of recent queries to analyze (None for all)
        :return: Dictionary with query statistics
        """
        queries = self.query_history
        if last_n_queries:
            queries = queries[-last_n_queries:]
        
        if not queries:
            return {
                'total_queries': 0,
                'average_execution_time': 0,
                'min_execution_time': 0,
                'max_execution_time': 0,
                'slow_queries_count': 0,
                'total_rows_affected': 0
            }
        
        execution_times = [q.execution_time for q in queries]
        rows_affected = [q.rows_affected for q in queries]
        slow_queries = [q for q in queries if q.execution_time > 1.0]
        
        return {
            'total_queries': len(queries),
            'average_execution_time': sum(execution_times) / len(execution_times),
            'min_execution_time': min(execution_times),
            'max_execution_time': max(execution_times),
            'slow_queries_count': len(slow_queries),
            'total_rows_affected': sum(rows_affected),
            'queries_per_second': len(queries) / max(1, time.time() - queries[0].timestamp) if queries else 0
        }
    
    def get_database_statistics(self) -> Optional[DatabaseStats]:
        """
        Get overall database statistics.
        
        :return: DatabaseStats object or None if unable to get statistics
        """
        try:
            import os
            conn = self.duckdb.connect()
            
            # Get file size
            file_size = 0
            if os.path.exists(self.duckdb.path):
                file_size = os.path.getsize(self.duckdb.path)
            
            # Get table count
            table_result = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main'"
            ).fetchone()
            table_count = table_result[0] if table_result else 0
            
            # Get total row count across all tables
            tables_result = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
            
            total_rows = 0
            for table_row in tables_result:
                table_name = table_row[0]
                try:
                    count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                    total_rows += count_result[0] if count_result else 0
                except Exception:
                    # Skip tables that can't be counted
                    pass
            
            return DatabaseStats(
                file_size_bytes=file_size,
                table_count=table_count,
                total_rows=total_rows,
                connection_count=1,  # Single connection for now
                last_accessed=time.time(),
                uptime_seconds=time.time() - self.start_time
            )
        except Exception as e:
            self.logger.error(f"Failed to get database statistics: {e}")
            return None
    
    def get_slow_queries(self, threshold_seconds: float = 1.0) -> List[QueryStats]:
        """
        Get list of slow queries above the threshold.
        
        :param threshold_seconds: Minimum execution time to consider a query slow
        :return: List of slow queries
        """
        return [q for q in self.query_history if q.execution_time >= threshold_seconds]
    
    def get_table_statistics(self) -> Dict[str, Dict[str, Any]]:
        """
        Get statistics for each table in the database.
        
        :return: Dictionary mapping table names to their statistics
        """
        try:
            conn = self.duckdb.connect()
            
            # Get all tables
            tables_result = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
            
            table_stats = {}
            for table_row in tables_result:
                table_name = table_row[0]
                
                try:
                    # Get row count
                    count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                    row_count = count_result[0] if count_result else 0
                    
                    # Get column count
                    columns_result = conn.execute(
                        "SELECT COUNT(*) FROM information_schema.columns "
                        "WHERE table_name=? AND table_schema='main'",
                        [table_name]
                    ).fetchone()
                    column_count = columns_result[0] if columns_result else 0
                    
                    # Get approximate size (this is an estimate)
                    # DuckDB doesn't provide direct table size info, so we estimate
                    estimated_size = row_count * column_count * 10  # Very rough estimate
                    
                    table_stats[table_name] = {
                        'row_count': row_count,
                        'column_count': column_count,
                        'estimated_size_bytes': estimated_size
                    }
                except Exception as e:
                    self.logger.warning(f"Failed to get statistics for table {table_name}: {e}")
                    table_stats[table_name] = {
                        'row_count': 0,
                        'column_count': 0,
                        'estimated_size_bytes': 0,
                        'error': str(e)
                    }
            
            return table_stats
        except Exception as e:
            self.logger.error(f"Failed to get table statistics: {e}")
            return {}
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform a comprehensive health check of the database.
        
        :return: Dictionary with health check results
        """
        health_status = {
            'overall_status': 'healthy',
            'issues': [],
            'warnings': [],
            'timestamp': time.time()
        }
        
        try:
            # Test basic connectivity
            conn = self.duckdb.connect()
            conn.execute("SELECT 1").fetchone()
        except Exception as e:
            health_status['overall_status'] = 'unhealthy'
            health_status['issues'].append(f"Database connection failed: {e}")
            return health_status
        
        # Check for slow queries
        slow_queries = self.get_slow_queries(threshold_seconds=2.0)
        if slow_queries:
            health_status['warnings'].append(f"Found {len(slow_queries)} slow queries (>2s)")
        
        # Check query performance
        query_stats = self.get_query_statistics()
        if query_stats['average_execution_time'] > 0.5:
            health_status['warnings'].append(
                f"Average query time is high: {query_stats['average_execution_time']:.2f}s"
            )
        
        # Check database size
        db_stats = self.get_database_statistics()
        if db_stats:
            # Warn if database is over 1GB
            if db_stats.file_size_bytes > 1024 * 1024 * 1024:
                health_status['warnings'].append(
                    f"Database file is large: {db_stats.file_size_bytes / (1024*1024*1024):.1f}GB"
                )
        
        # Check table statistics
        table_stats = self.get_table_statistics()
        for table_name, stats in table_stats.items():
            if 'error' in stats:
                health_status['issues'].append(f"Table {table_name} has issues: {stats['error']}")
        
        # Set overall status based on issues
        if health_status['issues']:
            health_status['overall_status'] = 'unhealthy'
        elif health_status['warnings']:
            health_status['overall_status'] = 'warning'
        
        return health_status
    
    def clear_query_history(self):
        """Clear the query history."""
        self.query_history.clear()
        self.logger.info("Query history cleared")
    
    def export_statistics(self) -> Dict[str, Any]:
        """
        Export comprehensive statistics for analysis.
        
        :return: Dictionary with all available statistics
        """
        return {
            'database_stats': self.get_database_statistics(),
            'query_stats': self.get_query_statistics(),
            'table_stats': self.get_table_statistics(),
            'health_check': self.health_check(),
            'slow_queries': [
                {
                    'query': q.query[:200],  # Truncate long queries
                    'execution_time': q.execution_time,
                    'timestamp': q.timestamp
                }
                for q in self.get_slow_queries()
            ]
        }