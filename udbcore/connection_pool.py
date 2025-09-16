"""
Connection pooling utility for managing multiple DuckDB connections efficiently.
"""
import threading
import time
from typing import Dict, Optional, Callable
from .ddb import DuckDB


class ConnectionPool:
    """
    A connection pool for managing DuckDB connections efficiently.
    Prevents connection overhead by reusing connections and managing their lifecycle.
    """
    
    def __init__(self, max_connections: int = 10, connection_timeout: int = 300):
        """
        Initialize the connection pool.
        
        :param max_connections: Maximum number of connections to maintain in the pool
        :param connection_timeout: Timeout in seconds after which idle connections are closed
        """
        self.max_connections = max_connections
        self.connection_timeout = connection_timeout
        self._pool: Dict[str, Dict] = {}
        self._lock = threading.RLock()
        self._cleanup_thread = None
        self._running = True
        self._start_cleanup_thread()
    
    def _start_cleanup_thread(self):
        """Start the background cleanup thread."""
        self._cleanup_thread = threading.Thread(target=self._cleanup_idle_connections, daemon=True)
        self._cleanup_thread.start()
    
    def _cleanup_idle_connections(self):
        """Background thread to clean up idle connections."""
        while self._running:
            time.sleep(30)  # Check every 30 seconds
            current_time = time.time()
            with self._lock:
                paths_to_remove = []
                for path, connection_info in self._pool.items():
                    if current_time - connection_info['last_used'] > self.connection_timeout:
                        try:
                            connection_info['duckdb'].disconnect()
                        except Exception:
                            pass  # Ignore cleanup errors
                        paths_to_remove.append(path)
                
                for path in paths_to_remove:
                    del self._pool[path]
    
    def get_connection(self, path: str, name: str = None) -> DuckDB:
        """
        Get a DuckDB connection from the pool, creating one if necessary.
        
        :param path: Path to the database file
        :param name: Optional name for the database (defaults to filename)
        :return: DuckDB instance with an active connection
        """
        if name is None:
            name = path.split('/')[-1]
        
        with self._lock:
            if path in self._pool:
                connection_info = self._pool[path]
                connection_info['last_used'] = time.time()
                return connection_info['duckdb']
            
            # Create new connection if pool not full
            if len(self._pool) < self.max_connections:
                duckdb_instance = DuckDB(path, name)
                duckdb_instance.connect()
                self._pool[path] = {
                    'duckdb': duckdb_instance,
                    'last_used': time.time()
                }
                return duckdb_instance
            
            # Pool is full, find least recently used connection
            lru_path = min(self._pool.keys(), 
                          key=lambda p: self._pool[p]['last_used'])
            
            # Close the LRU connection and replace it
            try:
                self._pool[lru_path]['duckdb'].disconnect()
            except Exception:
                pass
            
            del self._pool[lru_path]
            
            # Create new connection
            duckdb_instance = DuckDB(path, name)
            duckdb_instance.connect()
            self._pool[path] = {
                'duckdb': duckdb_instance,
                'last_used': time.time()
            }
            return duckdb_instance
    
    def return_connection(self, path: str):
        """
        Return a connection to the pool (updates last used time).
        
        :param path: Path to the database file
        """
        with self._lock:
            if path in self._pool:
                self._pool[path]['last_used'] = time.time()
    
    def close_connection(self, path: str):
        """
        Explicitly close and remove a connection from the pool.
        
        :param path: Path to the database file
        """
        with self._lock:
            if path in self._pool:
                try:
                    self._pool[path]['duckdb'].disconnect()
                except Exception:
                    pass
                del self._pool[path]
    
    def close_all(self):
        """Close all connections and stop the cleanup thread."""
        self._running = False
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=5)
        
        with self._lock:
            for connection_info in self._pool.values():
                try:
                    connection_info['duckdb'].disconnect()
                except Exception:
                    pass
            self._pool.clear()
    
    def get_pool_stats(self) -> Dict:
        """
        Get statistics about the connection pool.
        
        :return: Dictionary with pool statistics
        """
        with self._lock:
            return {
                'active_connections': len(self._pool),
                'max_connections': self.max_connections,
                'connection_paths': list(self._pool.keys()),
                'last_used_times': {path: info['last_used'] for path, info in self._pool.items()}
            }


# Global connection pool instance
_global_pool: Optional[ConnectionPool] = None
_pool_lock = threading.Lock()


def get_global_pool() -> ConnectionPool:
    """Get the global connection pool instance, creating it if necessary."""
    global _global_pool
    if _global_pool is None:
        with _pool_lock:
            if _global_pool is None:
                _global_pool = ConnectionPool()
    return _global_pool


def close_global_pool():
    """Close the global connection pool."""
    global _global_pool
    if _global_pool is not None:
        _global_pool.close_all()
        _global_pool = None