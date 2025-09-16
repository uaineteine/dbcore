# dbcore

The `dbcore` submodule provides comprehensive database abstractions and utilities for working with DuckDB databases in this project. It offers a clean, robust interface for database management with advanced features like connection pooling, transaction management, query building, schema management, and health monitoring.

## Contents

### Core Components
- `db.py`: Defines the `DB` base class for generic database file management
- `ddb.py`: Defines the enhanced `DuckDB` class with retry logic, health monitoring, and advanced operations

### Advanced Features
- `connection_pool.py`: Connection pooling utilities for efficient database connection management
- `transactions.py`: Transaction management and batch operation utilities
- `query_builder.py`: Fluent SQL query builder with support for complex queries
- `schema.py`: Database schema management utilities for table/index creation and migration
- `monitoring.py`: Database health monitoring and performance statistics

## Quick Start

```python
from foundation_packages.dbcore import DuckDB

# Create a DuckDB database object
my_db = DuckDB(path='path/to/database.duckdb', name='database.duckdb')

# Connect and run a query
my_db.connect()
results = my_db.run_query('SELECT * FROM my_table;')
my_db.disconnect()
```

## Core Features

### Enhanced DuckDB Class

The `DuckDB` class provides robust database operations with automatic retry logic and health monitoring:

```python
# Initialize with retry configuration
db = DuckDB('data.duckdb', 'data.duckdb', retry_attempts=3, retry_delay=1.0)

# Execute queries with automatic retry
results = db.run_query("SELECT * FROM users WHERE age > ?", [18])
single_result = db.run_query_single("SELECT COUNT(*) FROM users")

# Execute non-query operations
rows_affected = db.execute_non_query("INSERT INTO users (name, age) VALUES (?, ?)", ['John', 25])

# Batch operations
db.execute_many("INSERT INTO users (name, age) VALUES (?, ?)", 
                [['Alice', 30], ['Bob', 35], ['Carol', 28]])

# Transaction support
with db.transaction():
    db.execute_non_query("INSERT INTO accounts (user_id, balance) VALUES (?, ?)", [1, 100])
    db.execute_non_query("UPDATE users SET account_created = true WHERE id = ?", [1])
```

### Connection Pooling

Efficient connection management for multiple databases:

```python
from foundation_packages.dbcore import ConnectionPool, get_global_pool

# Use global connection pool
pool = get_global_pool()
db = pool.get_connection('/path/to/database.duckdb')
results = db.run_query("SELECT * FROM my_table")
pool.return_connection('/path/to/database.duckdb')

# Create custom pool
custom_pool = ConnectionPool(max_connections=20, connection_timeout=600)
db = custom_pool.get_connection('/path/to/another_db.duckdb')
```

### Transaction Management

Advanced transaction control with automatic rollback:

```python
from foundation_packages.dbcore import TransactionManager, BatchOperationManager

# Transaction management
db = DuckDB('data.duckdb', 'data.duckdb')
tm = TransactionManager(db)

# Manual transaction control
tm.begin_transaction()
try:
    db.execute_non_query("INSERT INTO table1 VALUES (?)", [value1])
    db.execute_non_query("INSERT INTO table2 VALUES (?)", [value2])
    tm.commit()
except Exception:
    tm.rollback()

# Context manager (automatic)
with tm.transaction():
    db.execute_non_query("INSERT INTO table1 VALUES (?)", [value1])
    db.execute_non_query("INSERT INTO table2 VALUES (?)", [value2])

# Batch operations
bm = BatchOperationManager(db, batch_size=1000)
data = [['Alice', 25], ['Bob', 30], ['Carol', 28]]
rows_inserted = bm.batch_insert('users', ['name', 'age'], data)
```

### Query Builder

Fluent interface for building complex SQL queries:

```python
from foundation_packages.dbcore import QueryBuilder, JoinType, OrderDirection

qb = QueryBuilder()

# Simple query
query = (qb.reset()
         .select('name', 'age', 'email')
         .from_table('users')
         .where('age > 18')
         .order_by('name', OrderDirection.ASC)
         .limit(10)
         .build())

# Complex query with joins
query = (qb.reset()
         .select('u.name', 'p.title', 'c.name')
         .from_table('users u')
         .join('posts p', 'p.user_id = u.id', JoinType.LEFT)
         .join('categories c', 'c.id = p.category_id', JoinType.INNER)
         .where('u.active = true')
         .where_in('c.name', ['Tech', 'Science'])
         .group_by('u.id')
         .having('COUNT(p.id) > 5')
         .order_by('u.name')
         .build())

# Execute the query
results = db.run_query(query)
```

### Schema Management

Programmatic database schema creation and management:

```python
from foundation_packages.dbcore import (SchemaManager, TableDefinition, 
                                       ColumnDefinition, IndexDefinition, ColumnType)

# Initialize schema manager
db = DuckDB('data.duckdb', 'data.duckdb')
sm = SchemaManager(db)

# Define table structure
columns = [
    ColumnDefinition('id', ColumnType.INTEGER, primary_key=True),
    ColumnDefinition('name', ColumnType.VARCHAR, nullable=False),
    ColumnDefinition('email', ColumnType.VARCHAR, unique=True),
    ColumnDefinition('age', ColumnType.INTEGER, default=0),
    ColumnDefinition('created_at', ColumnType.TIMESTAMP, default='CURRENT_TIMESTAMP')
]

indexes = [
    IndexDefinition('idx_users_email', 'users', ['email'], unique=True),
    IndexDefinition('idx_users_age', 'users', ['age'])
]

table_def = TableDefinition('users', columns, indexes)

# Create table
sm.create_table(table_def)

# Schema operations
sm.add_column('users', ColumnDefinition('last_login', ColumnType.TIMESTAMP))
sm.create_index(IndexDefinition('idx_users_name', 'users', ['name']))

# Schema inspection
tables = sm.list_tables()
schema_info = sm.get_table_schema('users')
exists = sm.table_exists('users')
```

### Health Monitoring

Comprehensive database health monitoring and performance statistics:

```python
from foundation_packages.dbcore import HealthMonitor

# Get health monitor from database
db = DuckDB('data.duckdb', 'data.duckdb')
monitor = db.get_health_monitor()

# Get statistics
query_stats = monitor.get_query_statistics()
db_stats = monitor.get_database_statistics()
table_stats = monitor.get_table_statistics()

# Health check
health = monitor.health_check()
print(f"Database status: {health['overall_status']}")

# Export comprehensive stats
all_stats = monitor.export_statistics()

# Find slow queries
slow_queries = monitor.get_slow_queries(threshold_seconds=2.0)
```

## Advanced Usage Examples

### Tilemap Region Database Management

```python
from foundation_packages.dbcore import DuckDB, SchemaManager, ColumnDefinition, ColumnType

# Create region database with proper schema
region_db = DuckDB(f'regions/region_{rx}_{ry}.duckdb', f'region_{rx}_{ry}.duckdb')
schema_manager = SchemaManager(region_db)

# Define cells table schema
cells_columns = [
    ColumnDefinition('x', ColumnType.INTEGER, nullable=False),
    ColumnDefinition('y', ColumnType.INTEGER, nullable=False),
    ColumnDefinition('z', ColumnType.INTEGER, nullable=False),
    ColumnDefinition('tile', ColumnType.INTEGER, default=0),
    ColumnDefinition('properties', ColumnType.JSON)
]

# Create spatial indexes for efficient lookups
spatial_indexes = [
    IndexDefinition('idx_cells_xyz', 'cells', ['x', 'y', 'z'], unique=True),
    IndexDefinition('idx_cells_xy', 'cells', ['x', 'y']),
    IndexDefinition('idx_cells_tile', 'cells', ['tile'])
]

table_def = TableDefinition('cells', cells_columns, spatial_indexes)
schema_manager.create_table(table_def)

# Efficient batch cell operations
batch_manager = BatchOperationManager(region_db, batch_size=10000)

# Insert many cells efficiently
cell_data = [(x, y, z, tile_id) for x in range(128) for y in range(128) for z in range(128)]
batch_manager.batch_insert('cells', ['x', 'y', 'z', 'tile'], cell_data)
```

### Multi-Database Operations

```python
# Use connection pool for multiple region databases
pool = get_global_pool()

regions_to_process = [(0, 0), (0, 1), (1, 0), (1, 1)]

for rx, ry in regions_to_process:
    db_path = f'regions/region_{rx}_{ry}.duckdb'
    db = pool.get_connection(db_path)
    
    # Process each region
    with db.transaction():
        # Update tiles based on some logic
        updated_tiles = db.run_query(
            "SELECT x, y, z FROM cells WHERE tile = ? AND z > ?", 
            [old_tile_id, min_height]
        )
        
        if updated_tiles:
            db.execute_non_query(
                "UPDATE cells SET tile = ? WHERE tile = ? AND z > ?",
                [new_tile_id, old_tile_id, min_height]
            )
    
    pool.return_connection(db_path)
```

## Requirements

- [duckdb](https://pypi.org/project/duckdb/) >= 0.8.0

## Version History

- **v1.0.0** - Initial release

## License

See the main project for license information.
