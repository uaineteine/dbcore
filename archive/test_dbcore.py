"""
Tests for the enhanced dbcore package functionality.
"""
import os
import tempfile
import unittest
import logging
from unittest.mock import patch, MagicMock

# Set up basic logging
logging.basicConfig(level=logging.WARNING)

from dbcore import *

class TestDuckDBEnhanced(unittest.TestCase):
    """Test the enhanced DuckDB functionality."""
    
    def setUp(self):
        """Set up test database."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'test.duckdb')
        self.db = DuckDB(self.db_path, 'test.duckdb')
    
    def tearDown(self):
        """Clean up test database."""
        if hasattr(self.db, 'conn') and self.db.conn:
            self.db.disconnect()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.rmdir(self.temp_dir)
    
    def test_enhanced_query_methods(self):
        """Test enhanced query execution methods."""
        # Create test table
        success = self.db.execute_non_query(
            "CREATE TABLE test_table (id INTEGER, name VARCHAR, age INTEGER)"
        )
        self.assertTrue(success)
        
        # Test execute_non_query
        success = self.db.execute_non_query(
            "INSERT INTO test_table VALUES (?, ?, ?)", [1, 'Alice', 25]
        )
        self.assertTrue(success)
        
        # Test run_query_single
        result = self.db.run_query_single("SELECT COUNT(*) FROM test_table")
        self.assertEqual(result[0], 1)
        
        # Test execute_many
        data = [[2, 'Bob', 30], [3, 'Carol', 28]]
        statements_executed = self.db.execute_many(
            "INSERT INTO test_table VALUES (?, ?, ?)", data
        )
        self.assertEqual(statements_executed, 2)
        
        # Test run_query with parameters
        results = self.db.run_query("SELECT * FROM test_table WHERE age > ?", [25])
        self.assertEqual(len(results), 2)
    
    def test_database_utilities(self):
        """Test database utility methods."""
        # Create test table
        self.db.execute_non_query(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR)"
        )
        
        # Test table_exists
        self.assertTrue(self.db.table_exists('users'))
        self.assertFalse(self.db.table_exists('nonexistent'))
        
        # Test get_table_names
        tables = self.db.get_table_names()
        self.assertIn('users', tables)
        
        # Test get_row_count
        self.db.execute_non_query("INSERT INTO users VALUES (1, 'Test')")
        count = self.db.get_row_count('users')
        self.assertEqual(count, 1)
    
    def test_transaction_context_manager(self):
        """Test transaction context manager."""
        self.db.execute_non_query(
            "CREATE TABLE accounts (id INTEGER, balance INTEGER)"
        )
        
        # Test successful transaction
        with self.db.transaction():
            self.db.execute_non_query("INSERT INTO accounts VALUES (1, 100)")
            self.db.execute_non_query("INSERT INTO accounts VALUES (2, 200)")
        
        count = self.db.get_row_count('accounts')
        self.assertEqual(count, 2)
        
        # Test rollback on exception
        try:
            with self.db.transaction():
                self.db.execute_non_query("INSERT INTO accounts VALUES (3, 300)")
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        # Should still be 2 rows (rollback happened)
        count = self.db.get_row_count('accounts')
        self.assertEqual(count, 2)
    
    def test_database_info(self):
        """Test database info retrieval."""
        info = self.db.get_database_info()
        self.assertEqual(info['path'], self.db_path)
        self.assertEqual(info['name'], 'test.duckdb')
        self.assertIsInstance(info['table_count'], int)


class TestConnectionPool(unittest.TestCase):
    """Test connection pooling functionality."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.pool = ConnectionPool(max_connections=3, connection_timeout=1)
    
    def tearDown(self):
        """Clean up test environment."""
        self.pool.close_all()
        # Clean up any test database files
        for file in os.listdir(self.temp_dir):
            if file.endswith('.duckdb'):
                os.remove(os.path.join(self.temp_dir, file))
        os.rmdir(self.temp_dir)
    
    def test_connection_pooling(self):
        """Test basic connection pooling functionality."""
        db_path = os.path.join(self.temp_dir, 'pool_test.duckdb')
        
        # Get connection
        db1 = self.pool.get_connection(db_path)
        self.assertIsNotNone(db1)
        
        # Get same connection again
        db2 = self.pool.get_connection(db_path)
        self.assertEqual(db1, db2)
        
        # Return connection
        self.pool.return_connection(db_path)
        
        # Check pool stats
        stats = self.pool.get_pool_stats()
        self.assertEqual(stats['active_connections'], 1)
        self.assertEqual(stats['max_connections'], 3)
    
    def test_pool_limit(self):
        """Test pool connection limit enforcement."""
        paths = [os.path.join(self.temp_dir, f'test{i}.duckdb') for i in range(5)]
        
        # Fill the pool
        connections = []
        for i in range(3):
            db = self.pool.get_connection(paths[i])
            connections.append(db)
        
        stats = self.pool.get_pool_stats()
        self.assertEqual(stats['active_connections'], 3)
        
        # Adding one more should evict the least recently used
        db4 = self.pool.get_connection(paths[3])
        stats = self.pool.get_pool_stats()
        self.assertEqual(stats['active_connections'], 3)


class TestTransactionManager(unittest.TestCase):
    """Test transaction management functionality."""
    
    def setUp(self):
        """Set up test database."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'transaction_test.duckdb')
        self.db = DuckDB(self.db_path, 'trans_test.duckdb')
        self.tm = TransactionManager(self.db)
        
        # Create test table
        self.db.execute_non_query(
            "CREATE TABLE test_table (id INTEGER, value VARCHAR)"
        )
    
    def tearDown(self):
        """Clean up test database."""
        if hasattr(self.db, 'conn') and self.db.conn:
            self.db.disconnect()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.rmdir(self.temp_dir)
    
    def test_manual_transaction(self):
        """Test manual transaction control."""
        self.tm.begin_transaction()
        self.db.execute_non_query("INSERT INTO test_table VALUES (1, 'test')")
        self.tm.commit()
        
        count = self.db.get_row_count('test_table')
        self.assertEqual(count, 1)
    
    def test_transaction_rollback(self):
        """Test transaction rollback."""
        self.tm.begin_transaction()
        self.db.execute_non_query("INSERT INTO test_table VALUES (1, 'test')")
        self.tm.rollback()
        
        count = self.db.get_row_count('test_table')
        self.assertEqual(count, 0)
    
    def test_execute_in_transaction(self):
        """Test execute_in_transaction method."""
        queries = [
            "INSERT INTO test_table VALUES (1, 'first')",
            "INSERT INTO test_table VALUES (2, 'second')"
        ]
        
        results = self.tm.execute_in_transaction(queries)
        self.assertEqual(len(results), 2)
        
        count = self.db.get_row_count('test_table')
        self.assertEqual(count, 2)


class TestBatchOperationManager(unittest.TestCase):
    """Test batch operation functionality."""
    
    def setUp(self):
        """Set up test database."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'batch_test.duckdb')
        self.db = DuckDB(self.db_path, 'batch_test.duckdb')
        self.bm = BatchOperationManager(self.db, batch_size=2)
        
        # Create test table
        self.db.execute_non_query(
            "CREATE TABLE test_table (id INTEGER, name VARCHAR, age INTEGER)"
        )
    
    def tearDown(self):
        """Clean up test database."""
        if hasattr(self.db, 'conn') and self.db.conn:
            self.db.disconnect()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.rmdir(self.temp_dir)
    
    def test_batch_insert(self):
        """Test batch insert functionality."""
        data = [
            [1, 'Alice', 25],
            [2, 'Bob', 30],
            [3, 'Carol', 28],
            [4, 'Dave', 35]
        ]
        
        rows_inserted = self.bm.batch_insert('test_table', ['id', 'name', 'age'], data)
        self.assertEqual(rows_inserted, 4)
        
        count = self.db.get_row_count('test_table')
        self.assertEqual(count, 4)


class TestQueryBuilder(unittest.TestCase):
    """Test query builder functionality."""
    
    def test_simple_select(self):
        """Test simple SELECT query building."""
        qb = QueryBuilder()
        query = (qb.select('name', 'age')
                 .from_table('users')
                 .where('age > 18')
                 .order_by('name')
                 .limit(10)
                 .build())
        
        expected = "SELECT name, age FROM users WHERE age > 18 ORDER BY name ASC LIMIT 10"
        self.assertEqual(query, expected)
    
    def test_complex_query(self):
        """Test complex query with joins."""
        qb = QueryBuilder()
        query = (qb.select('u.name', 'p.title')
                 .from_table('users u')
                 .join('posts p', 'p.user_id = u.id', JoinType.LEFT)
                 .where('u.active = true')
                 .where_in('p.status', ['published', 'draft'])
                 .group_by('u.id')
                 .order_by('u.name', OrderDirection.DESC)
                 .build())
        
        self.assertIn('SELECT u.name, p.title', query)
        self.assertIn('LEFT JOIN posts p ON p.user_id = u.id', query)
        self.assertIn('WHERE u.active = true', query)
        self.assertIn("p.status IN ('published', 'draft')", query)
        self.assertIn('GROUP BY u.id', query)
        self.assertIn('ORDER BY u.name DESC', query)
    
    def test_insert_builder(self):
        """Test INSERT query building."""
        ib = InsertBuilder()
        query = (ib.into('users')
                 .columns('name', 'age', 'email')
                 .values('Alice', 25, 'alice@example.com')
                 .values('Bob', 30, 'bob@example.com')
                 .build())
        
        self.assertIn('INSERT INTO users', query)
        self.assertIn('(name, age, email)', query)
        self.assertIn("('Alice', 25, 'alice@example.com')", query)
        self.assertIn("('Bob', 30, 'bob@example.com')", query)
    
    def test_update_builder(self):
        """Test UPDATE query building."""
        ub = UpdateBuilder()
        query = (ub.table('users')
                 .set('name', 'Alice Smith')
                 .set('age', 26)
                 .where('id = 1')
                 .build())
        
        expected = "UPDATE users SET name = 'Alice Smith', age = 26 WHERE id = 1"
        self.assertEqual(query, expected)


class TestSchemaManager(unittest.TestCase):
    """Test schema management functionality."""
    
    def setUp(self):
        """Set up test database."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'schema_test.duckdb')
        self.db = DuckDB(self.db_path, 'schema_test.duckdb')
        self.sm = SchemaManager(self.db)
    
    def tearDown(self):
        """Clean up test database."""
        if hasattr(self.db, 'conn') and self.db.conn:
            self.db.disconnect()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.rmdir(self.temp_dir)
    
    def test_table_creation(self):
        """Test table creation from definition."""
        columns = [
            ColumnDefinition('id', ColumnType.INTEGER, primary_key=True),
            ColumnDefinition('name', ColumnType.VARCHAR, nullable=False),
            ColumnDefinition('age', ColumnType.INTEGER, default=0)
        ]
        
        table_def = TableDefinition('test_users', columns)
        success = self.sm.create_table(table_def)
        self.assertTrue(success)
        
        # Verify table exists
        self.assertTrue(self.sm.table_exists('test_users'))
        
        # Check table schema
        schema = self.sm.get_table_schema('test_users')
        self.assertIsNotNone(schema)
        self.assertEqual(len(schema['columns']), 3)
    
    def test_column_operations(self):
        """Test adding and dropping columns."""
        # Create initial table
        columns = [ColumnDefinition('id', ColumnType.INTEGER, primary_key=True)]
        table_def = TableDefinition('test_table', columns)
        self.sm.create_table(table_def)
        
        # Add column
        new_column = ColumnDefinition('email', ColumnType.VARCHAR)
        success = self.sm.add_column('test_table', new_column)
        self.assertTrue(success)
        
        # Verify column was added
        schema = self.sm.get_table_schema('test_table')
        column_names = [col['name'] for col in schema['columns']]
        self.assertIn('email', column_names)
    
    def test_list_tables(self):
        """Test listing all tables."""
        # Create some tables
        for i in range(3):
            columns = [ColumnDefinition('id', ColumnType.INTEGER)]
            table_def = TableDefinition(f'table_{i}', columns)
            self.sm.create_table(table_def)
        
        tables = self.sm.list_tables()
        self.assertEqual(len(tables), 3)
        for i in range(3):
            self.assertIn(f'table_{i}', tables)


class TestHealthMonitor(unittest.TestCase):
    """Test health monitoring functionality."""
    
    def setUp(self):
        """Set up test database."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'health_test.duckdb')
        self.db = DuckDB(self.db_path, 'health_test.duckdb')
        self.monitor = HealthMonitor(self.db, max_query_history=10)
    
    def tearDown(self):
        """Clean up test database."""
        if hasattr(self.db, 'conn') and self.db.conn:
            self.db.disconnect()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.rmdir(self.temp_dir)
    
    def test_query_recording(self):
        """Test query statistics recording."""
        # Record some queries
        self.monitor.record_query("SELECT 1", 0.1, 1)
        self.monitor.record_query("SELECT 2", 0.2, 2)
        self.monitor.record_query("SELECT 3", 1.5, 0)  # Slow query
        
        stats = self.monitor.get_query_statistics()
        self.assertEqual(stats['total_queries'], 3)
        self.assertEqual(stats['slow_queries_count'], 1)
        self.assertEqual(stats['total_rows_affected'], 3)
        self.assertAlmostEqual(stats['average_execution_time'], 0.6, places=1)
    
    def test_database_statistics(self):
        """Test database statistics collection."""
        # Create a test table
        self.db.execute_non_query("CREATE TABLE test (id INTEGER)")
        self.db.execute_non_query("INSERT INTO test VALUES (1)")
        
        stats = self.monitor.get_database_statistics()
        self.assertIsNotNone(stats)
        self.assertEqual(stats.table_count, 1)
        self.assertEqual(stats.total_rows, 1)
        self.assertGreater(stats.uptime_seconds, 0)
    
    def test_health_check(self):
        """Test comprehensive health check."""
        # Create a table for testing
        self.db.execute_non_query("CREATE TABLE test (id INTEGER)")
        
        health = self.monitor.health_check()
        self.assertIn('overall_status', health)
        self.assertIn('issues', health)
        self.assertIn('warnings', health)
        self.assertIn('timestamp', health)


class TestModuleInfo(unittest.TestCase):
    """Test module information functionality."""
    
    def test_module_info(self):
        """Test module information retrieval."""
        info = get_module_info()
        self.assertEqual(info['name'], 'dbcore')
        self.assertEqual(info['version'], '1.0.0')
        self.assertIn('description', info)


if __name__ == '__main__':
    # Create a test suite
    test_suite = unittest.TestSuite()
    
    # Add all test classes
    test_classes = [
        TestDuckDBEnhanced,
        TestConnectionPool,
        TestTransactionManager,
        TestBatchOperationManager,
        TestQueryBuilder,
        TestSchemaManager,
        TestHealthMonitor,
        TestModuleInfo
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Exit with appropriate code
    exit(0 if result.wasSuccessful() else 1)