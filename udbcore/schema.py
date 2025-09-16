"""
Database schema management utilities for creating and maintaining database schemas.
"""
import logging
from typing import Dict, List, Optional, Any, Union
from enum import Enum
from .ddb import DuckDB


class ColumnType(Enum):
    """Enumeration of DuckDB column types."""
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    SMALLINT = "SMALLINT"
    TINYINT = "TINYINT"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    BLOB = "BLOB"
    JSON = "JSON"


class ColumnDefinition:
    """
    Represents a database column definition.
    """
    
    def __init__(self, name: str, column_type: Union[ColumnType, str], 
                 nullable: bool = True, default: Any = None, 
                 primary_key: bool = False, unique: bool = False):
        """
        Initialize a column definition.
        
        :param name: Column name
        :param column_type: Column data type
        :param nullable: Whether the column can be NULL
        :param default: Default value for the column
        :param primary_key: Whether this is a primary key column
        :param unique: Whether this column has a unique constraint
        """
        self.name = name
        self.column_type = column_type if isinstance(column_type, str) else column_type.value
        self.nullable = nullable
        self.default = default
        self.primary_key = primary_key
        self.unique = unique
    
    def to_sql(self) -> str:
        """
        Convert the column definition to SQL DDL.
        
        :return: SQL column definition string
        """
        sql = f"{self.name} {self.column_type}"
        
        if self.primary_key:
            sql += " PRIMARY KEY"
        elif not self.nullable:
            sql += " NOT NULL"
        
        if self.unique and not self.primary_key:
            sql += " UNIQUE"
        
        if self.default is not None:
            if isinstance(self.default, str):
                sql += f" DEFAULT '{self.default}'"
            else:
                sql += f" DEFAULT {self.default}"
        
        return sql


class IndexDefinition:
    """
    Represents a database index definition.
    """
    
    def __init__(self, name: str, table: str, columns: List[str], unique: bool = False):
        """
        Initialize an index definition.
        
        :param name: Index name
        :param table: Table name
        :param columns: List of column names to index
        :param unique: Whether this is a unique index
        """
        self.name = name
        self.table = table
        self.columns = columns
        self.unique = unique
    
    def to_sql(self) -> str:
        """
        Convert the index definition to SQL DDL.
        
        :return: SQL CREATE INDEX statement
        """
        unique_keyword = "UNIQUE " if self.unique else ""
        columns_str = ", ".join(self.columns)
        return f"CREATE {unique_keyword}INDEX {self.name} ON {self.table} ({columns_str})"


class TableDefinition:
    """
    Represents a database table definition.
    """
    
    def __init__(self, name: str, columns: List[ColumnDefinition], indexes: List[IndexDefinition] = None):
        """
        Initialize a table definition.
        
        :param name: Table name
        :param columns: List of column definitions
        :param indexes: List of index definitions
        """
        self.name = name
        self.columns = columns
        self.indexes = indexes or []
    
    def add_column(self, column: ColumnDefinition):
        """
        Add a column to the table definition.
        
        :param column: Column definition to add
        """
        self.columns.append(column)
    
    def add_index(self, index: IndexDefinition):
        """
        Add an index to the table definition.
        
        :param index: Index definition to add
        """
        self.indexes.append(index)
    
    def to_sql(self) -> str:
        """
        Convert the table definition to SQL DDL.
        
        :return: SQL CREATE TABLE statement
        """
        columns_sql = ",\n    ".join([col.to_sql() for col in self.columns])
        return f"CREATE TABLE {self.name} (\n    {columns_sql}\n)"


class SchemaManager:
    """
    Manages database schema operations including creation, migration, and validation.
    """
    
    def __init__(self, duckdb_instance: DuckDB):
        """
        Initialize the schema manager.
        
        :param duckdb_instance: DuckDB instance to manage schema for
        """
        self.duckdb = duckdb_instance
        self.logger = logging.getLogger(__name__)
    
    def create_table(self, table_def: TableDefinition, if_not_exists: bool = True) -> bool:
        """
        Create a table from a table definition.
        
        :param table_def: Table definition
        :param if_not_exists: Whether to add IF NOT EXISTS clause
        :return: True if successful, False otherwise
        """
        try:
            conn = self.duckdb.connect()
            
            # Create the table
            create_sql = table_def.to_sql()
            if if_not_exists:
                create_sql = create_sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
            
            conn.execute(create_sql)
            self.logger.info(f"Created table {table_def.name}")
            
            # Create indexes
            for index in table_def.indexes:
                try:
                    conn.execute(index.to_sql())
                    self.logger.info(f"Created index {index.name} on table {table_def.name}")
                except Exception as e:
                    self.logger.warning(f"Failed to create index {index.name}: {e}")
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to create table {table_def.name}: {e}")
            return False
    
    def drop_table(self, table_name: str, if_exists: bool = True) -> bool:
        """
        Drop a table.
        
        :param table_name: Name of the table to drop
        :param if_exists: Whether to add IF EXISTS clause
        :return: True if successful, False otherwise
        """
        try:
            conn = self.duckdb.connect()
            drop_sql = f"DROP TABLE {'IF EXISTS ' if if_exists else ''}{table_name}"
            conn.execute(drop_sql)
            self.logger.info(f"Dropped table {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to drop table {table_name}: {e}")
            return False
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        :param table_name: Name of the table to check
        :return: True if table exists, False otherwise
        """
        try:
            conn = self.duckdb.connect()
            result = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='main' AND table_name=?",
                [table_name]
            ).fetchone()
            return result is not None
        except Exception as e:
            self.logger.error(f"Failed to check if table {table_name} exists: {e}")
            return False
    
    def get_table_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the schema information for a table.
        
        :param table_name: Name of the table
        :return: Dictionary with table schema information, or None if table doesn't exist
        """
        try:
            conn = self.duckdb.connect()
            
            # Get column information
            columns_result = conn.execute(
                "SELECT column_name, data_type, is_nullable, column_default "
                "FROM information_schema.columns "
                "WHERE table_name=? AND table_schema='main' "
                "ORDER BY ordinal_position",
                [table_name]
            ).fetchall()
            
            if not columns_result:
                return None
            
            columns = []
            for row in columns_result:
                columns.append({
                    'name': row[0],
                    'type': row[1],
                    'nullable': row[2] == 'YES',
                    'default': row[3]
                })
            
            # Get index information (this is DuckDB-specific)
            try:
                indexes_result = conn.execute(
                    "SELECT index_name, column_name, is_unique "
                    "FROM duckdb_indexes() "
                    "WHERE table_name=?",
                    [table_name]
                ).fetchall()
                
                indexes = []
                for row in indexes_result:
                    indexes.append({
                        'name': row[0],
                        'column': row[1],
                        'unique': row[2]
                    })
            except Exception:
                # Fallback if duckdb_indexes() is not available
                indexes = []
            
            return {
                'table_name': table_name,
                'columns': columns,
                'indexes': indexes
            }
        except Exception as e:
            self.logger.error(f"Failed to get schema for table {table_name}: {e}")
            return None
    
    def list_tables(self) -> List[str]:
        """
        Get a list of all tables in the database.
        
        :return: List of table names
        """
        try:
            conn = self.duckdb.connect()
            result = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='main'"
            ).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            self.logger.error(f"Failed to list tables: {e}")
            return []
    
    def add_column(self, table_name: str, column_def: ColumnDefinition) -> bool:
        """
        Add a column to an existing table.
        
        :param table_name: Name of the table
        :param column_def: Column definition to add
        :return: True if successful, False otherwise
        """
        try:
            conn = self.duckdb.connect()
            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {column_def.to_sql()}"
            conn.execute(alter_sql)
            self.logger.info(f"Added column {column_def.name} to table {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to add column {column_def.name} to table {table_name}: {e}")
            return False
    
    def drop_column(self, table_name: str, column_name: str) -> bool:
        """
        Drop a column from an existing table.
        
        :param table_name: Name of the table
        :param column_name: Name of the column to drop
        :return: True if successful, False otherwise
        """
        try:
            conn = self.duckdb.connect()
            alter_sql = f"ALTER TABLE {table_name} DROP COLUMN {column_name}"
            conn.execute(alter_sql)
            self.logger.info(f"Dropped column {column_name} from table {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to drop column {column_name} from table {table_name}: {e}")
            return False
    
    def create_index(self, index_def: IndexDefinition) -> bool:
        """
        Create an index on a table.
        
        :param index_def: Index definition
        :return: True if successful, False otherwise
        """
        try:
            conn = self.duckdb.connect()
            conn.execute(index_def.to_sql())
            self.logger.info(f"Created index {index_def.name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create index {index_def.name}: {e}")
            return False
    
    def drop_index(self, index_name: str) -> bool:
        """
        Drop an index.
        
        :param index_name: Name of the index to drop
        :return: True if successful, False otherwise
        """
        try:
            conn = self.duckdb.connect()
            conn.execute(f"DROP INDEX IF EXISTS {index_name}")
            self.logger.info(f"Dropped index {index_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to drop index {index_name}: {e}")
            return False