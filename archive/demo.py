#!/usr/bin/env python3
"""
Demonstration of the enhanced udbcore package functionality.
"""

import os
import tempfile
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Import the enhanced udbcore functionality
from foundation_packages.udbcore import (
    DuckDB, ConnectionPool, TransactionManager, BatchOperationManager,
    QueryBuilder, SchemaManager, TableDefinition, ColumnDefinition, ColumnType,
    get_module_info
)

def main():
    """Demonstrate enhanced udbcore functionality."""
    print("=== Enhanced udbcore Package Demonstration ===")
    print()
    
    # Show module info
    info = get_module_info()
    print(f"Module: {info['name']} v{info['version']}")
    print(f"Description: {info['description']}")
    print()
    
    # Create temporary directory for demo databases
    temp_dir = tempfile.mkdtemp()
    print(f"Demo running in: {temp_dir}")
    
    try:
        # === 1. Enhanced DuckDB Usage ===
        print("=== 1. Enhanced DuckDB Usage ===")
        
        db_path = os.path.join(temp_dir, 'demo.duckdb')
        db = DuckDB(db_path, 'demo.duckdb', retry_attempts=3)
        
        # Create a test table using enhanced features
        success = db.execute_non_query("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                email VARCHAR UNIQUE,
                age INTEGER DEFAULT 0
            )
        """)
        print(f"Table creation successful: {success}")
        
        # Insert some test data
        users_data = [
            [1, 'Alice Johnson', 'alice@example.com', 28],
            [2, 'Bob Smith', 'bob@example.com', 34],
            [3, 'Carol Davis', 'carol@example.com', 26]
        ]
        
        rows_inserted = db.execute_many(
            "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
            users_data
        )
        print(f"Inserted {rows_inserted} users")
        
        # Query with parameters
        young_users = db.run_query("SELECT name, age FROM users WHERE age < ?", [30])
        print(f"Users under 30: {young_users}")
        
        # === 2. Schema Management ===
        print("\n=== 2. Schema Management ===")
        
        sm = SchemaManager(db)
        
        # Create a complex table using schema management
        columns = [
            ColumnDefinition('id', ColumnType.INTEGER, primary_key=True),
            ColumnDefinition('user_id', ColumnType.INTEGER, nullable=False),
            ColumnDefinition('title', ColumnType.VARCHAR, nullable=False),
            ColumnDefinition('content', ColumnType.TEXT),
            ColumnDefinition('status', ColumnType.VARCHAR, default='draft')
        ]
        
        posts_table = TableDefinition('posts', columns)
        sm.create_table(posts_table)
        print("Created posts table using schema manager")
        
        # List all tables
        tables = sm.list_tables()
        print(f"Database tables: {tables}")
        
        # === 3. Query Builder ===
        print("\n=== 3. Query Builder ===")
        
        qb = QueryBuilder()
        
        # Build a complex query
        query = (qb.select('u.name', 'u.email', 'COUNT(p.id) as post_count')
                 .from_table('users u')
                 .join('posts p', 'p.user_id = u.id')
                 .where('u.age > 25')
                 .group_by('u.id', 'u.name', 'u.email')
                 .order_by('post_count')
                 .build())
        
        print(f"Generated query: {query}")
        
        # === 4. Transaction Management ===
        print("\n=== 4. Transaction Management ===")
        
        tm = TransactionManager(db)
        
        # Insert posts using transactions
        with tm.transaction():
            db.execute_non_query("INSERT INTO posts (id, user_id, title, content) VALUES (?, ?, ?, ?)",
                                [1, 1, 'First Post', 'This is Alice\'s first post'])
            db.execute_non_query("INSERT INTO posts (id, user_id, title, content) VALUES (?, ?, ?, ?)",
                                [2, 2, 'Bob\'s Thoughts', 'Some interesting thoughts from Bob'])
            print("Inserted posts using transaction")
        
        # === 5. Batch Operations ===
        print("\n=== 5. Batch Operations ===")
        
        bm = BatchOperationManager(db, batch_size=2)
        
        more_posts = [
            [3, 1, 'Second Post', 'Alice\'s second post'],
            [4, 3, 'Carol\'s Ideas', 'Great ideas from Carol'],
            [5, 2, 'More Thoughts', 'Additional thoughts from Bob']
        ]
        
        inserted = bm.batch_insert('posts', ['id', 'user_id', 'title', 'content'], more_posts)
        print(f"Batch inserted {inserted} posts")
        
        # === 6. Connection Pooling ===
        print("\n=== 6. Connection Pooling ===")
        
        pool = ConnectionPool(max_connections=3)
        
        # Simulate accessing multiple databases
        for i in range(3):
            region_path = os.path.join(temp_dir, f'region_{i}.duckdb')
            region_db = pool.get_connection(region_path, f'region_{i}.duckdb')
            region_db.execute_non_query("CREATE TABLE cells (x INTEGER, y INTEGER, z INTEGER, tile INTEGER)")
            print(f"Created region {i} database")
            pool.return_connection(region_path)
        
        pool_stats = pool.get_pool_stats()
        print(f"Pool stats: {pool_stats['active_connections']} connections active")
        
        # === 7. Health Monitoring ===
        print("\n=== 7. Health Monitoring ===")
        
        monitor = db.get_health_monitor()
        if monitor:
            # Get database statistics
            db_stats = monitor.get_database_statistics()
            if db_stats:
                print(f"Database size: {db_stats.file_size_bytes} bytes")
                print(f"Tables: {db_stats.table_count}")
                print(f"Total rows: {db_stats.total_rows}")
            
            # Get query statistics
            query_stats = monitor.get_query_statistics()
            print(f"Queries executed: {query_stats['total_queries']}")
            print(f"Average execution time: {query_stats['average_execution_time']:.3f}s")
            
            # Health check
            health = monitor.health_check()
            print(f"Database health: {health['overall_status']}")
        
        # === 8. Database Info ===
        print("\n=== 8. Database Information ===")
        
        info = db.get_database_info()
        print(f"Database path: {info['path']}")
        print(f"Tables in database: {info['table_count']}")
        print(f"Connection loaded: {info['loaded']}")
        
        # Close connections
        pool.close_all()
        db.disconnect()
        
        print("\n=== Demo completed successfully! ===")
        
    except Exception as e:
        print(f"Demo error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Clean up temporary files
        import shutil
        try:
            shutil.rmtree(temp_dir)
            print(f"Cleaned up temporary directory: {temp_dir}")
        except Exception as e:
            print(f"Warning: Could not clean up {temp_dir}: {e}")


if __name__ == '__main__':
    main()