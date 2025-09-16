"""
Query builder utilities for constructing SQL queries programmatically.
"""
from typing import List, Dict, Any, Optional, Union
from enum import Enum


class JoinType(Enum):
    """Enumeration of SQL join types."""
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"


class OrderDirection(Enum):
    """Enumeration of SQL order directions."""
    ASC = "ASC"
    DESC = "DESC"


class QueryBuilder:
    """
    A fluent interface for building SQL queries programmatically.
    """
    
    def __init__(self):
        """Initialize a new QueryBuilder."""
        self.reset()
    
    def reset(self):
        """Reset the query builder to initial state."""
        self._select_fields = []
        self._from_table = None
        self._joins = []
        self._where_conditions = []
        self._group_by_fields = []
        self._having_conditions = []
        self._order_by_fields = []
        self._limit_value = None
        self._offset_value = None
        return self
    
    def select(self, *fields: str):
        """
        Add SELECT fields to the query.
        
        :param fields: Field names to select
        :return: Self for method chaining
        """
        self._select_fields.extend(fields)
        return self
    
    def select_distinct(self, *fields: str):
        """
        Add DISTINCT SELECT fields to the query.
        
        :param fields: Field names to select with DISTINCT
        :return: Self for method chaining
        """
        if fields:
            distinct_fields = [f"DISTINCT {fields[0]}"] + list(fields[1:])
            self._select_fields.extend(distinct_fields)
        return self
    
    def from_table(self, table_name: str):
        """
        Set the FROM table for the query.
        
        :param table_name: Name of the table
        :return: Self for method chaining
        """
        self._from_table = table_name
        return self
    
    def join(self, table: str, on_condition: str, join_type: JoinType = JoinType.INNER):
        """
        Add a JOIN clause to the query.
        
        :param table: Table to join
        :param on_condition: JOIN condition
        :param join_type: Type of join (INNER, LEFT, etc.)
        :return: Self for method chaining
        """
        self._joins.append(f"{join_type.value} {table} ON {on_condition}")
        return self
    
    def where(self, condition: str):
        """
        Add a WHERE condition to the query.
        
        :param condition: WHERE condition
        :return: Self for method chaining
        """
        self._where_conditions.append(condition)
        return self
    
    def where_in(self, field: str, values: List[Any]):
        """
        Add a WHERE field IN (values) condition.
        
        :param field: Field name
        :param values: List of values
        :return: Self for method chaining
        """
        if values:
            value_list = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in values])
            self._where_conditions.append(f"{field} IN ({value_list})")
        return self
    
    def where_between(self, field: str, start_value: Any, end_value: Any):
        """
        Add a WHERE field BETWEEN start AND end condition.
        
        :param field: Field name
        :param start_value: Start value
        :param end_value: End value
        :return: Self for method chaining
        """
        start_str = f"'{start_value}'" if isinstance(start_value, str) else str(start_value)
        end_str = f"'{end_value}'" if isinstance(end_value, str) else str(end_value)
        self._where_conditions.append(f"{field} BETWEEN {start_str} AND {end_str}")
        return self
    
    def group_by(self, *fields: str):
        """
        Add GROUP BY fields to the query.
        
        :param fields: Field names to group by
        :return: Self for method chaining
        """
        self._group_by_fields.extend(fields)
        return self
    
    def having(self, condition: str):
        """
        Add a HAVING condition to the query.
        
        :param condition: HAVING condition
        :return: Self for method chaining
        """
        self._having_conditions.append(condition)
        return self
    
    def order_by(self, field: str, direction: OrderDirection = OrderDirection.ASC):
        """
        Add an ORDER BY field to the query.
        
        :param field: Field name to order by
        :param direction: Order direction (ASC or DESC)
        :return: Self for method chaining
        """
        self._order_by_fields.append(f"{field} {direction.value}")
        return self
    
    def limit(self, count: int):
        """
        Add a LIMIT clause to the query.
        
        :param count: Number of rows to limit
        :return: Self for method chaining
        """
        self._limit_value = count
        return self
    
    def offset(self, count: int):
        """
        Add an OFFSET clause to the query.
        
        :param count: Number of rows to offset
        :return: Self for method chaining
        """
        self._offset_value = count
        return self
    
    def build(self) -> str:
        """
        Build and return the final SQL query string.
        
        :return: Complete SQL query string
        """
        if not self._from_table:
            raise ValueError("FROM table must be specified")
        
        # SELECT clause
        if not self._select_fields:
            select_clause = "SELECT *"
        else:
            select_clause = f"SELECT {', '.join(self._select_fields)}"
        
        # FROM clause
        from_clause = f"FROM {self._from_table}"
        
        # JOIN clauses
        join_clause = ' '.join(self._joins) if self._joins else ""
        
        # WHERE clause
        where_clause = f"WHERE {' AND '.join(self._where_conditions)}" if self._where_conditions else ""
        
        # GROUP BY clause
        group_by_clause = f"GROUP BY {', '.join(self._group_by_fields)}" if self._group_by_fields else ""
        
        # HAVING clause
        having_clause = f"HAVING {' AND '.join(self._having_conditions)}" if self._having_conditions else ""
        
        # ORDER BY clause
        order_by_clause = f"ORDER BY {', '.join(self._order_by_fields)}" if self._order_by_fields else ""
        
        # LIMIT clause
        limit_clause = f"LIMIT {self._limit_value}" if self._limit_value is not None else ""
        
        # OFFSET clause
        offset_clause = f"OFFSET {self._offset_value}" if self._offset_value is not None else ""
        
        # Combine all clauses
        clauses = [select_clause, from_clause, join_clause, where_clause, 
                  group_by_clause, having_clause, order_by_clause, limit_clause, offset_clause]
        
        # Filter out empty clauses and join with spaces
        query = ' '.join(clause for clause in clauses if clause.strip())
        
        return query


class InsertBuilder:
    """
    Builder for INSERT statements.
    """
    
    def __init__(self):
        """Initialize a new InsertBuilder."""
        self.reset()
    
    def reset(self):
        """Reset the insert builder to initial state."""
        self._table = None
        self._columns = []
        self._values = []
        self._on_conflict = None
        return self
    
    def into(self, table_name: str):
        """
        Set the table to insert into.
        
        :param table_name: Name of the table
        :return: Self for method chaining
        """
        self._table = table_name
        return self
    
    def columns(self, *column_names: str):
        """
        Set the columns to insert into.
        
        :param column_names: Names of the columns
        :return: Self for method chaining
        """
        self._columns = list(column_names)
        return self
    
    def values(self, *values: Any):
        """
        Add a row of values to insert.
        
        :param values: Values for the row
        :return: Self for method chaining
        """
        if len(values) != len(self._columns):
            raise ValueError(f"Number of values ({len(values)}) must match number of columns ({len(self._columns)})")
        self._values.append(values)
        return self
    
    def on_conflict_ignore(self):
        """
        Add ON CONFLICT DO NOTHING clause.
        
        :return: Self for method chaining
        """
        self._on_conflict = "ON CONFLICT DO NOTHING"
        return self
    
    def build(self) -> str:
        """
        Build and return the final INSERT statement.
        
        :return: Complete INSERT SQL statement
        """
        if not self._table:
            raise ValueError("Table name must be specified")
        if not self._columns:
            raise ValueError("Columns must be specified")
        if not self._values:
            raise ValueError("At least one row of values must be specified")
        
        columns_clause = f"({', '.join(self._columns)})"
        
        # Format values
        value_rows = []
        for row in self._values:
            formatted_values = []
            for value in row:
                if value is None:
                    formatted_values.append("NULL")
                elif isinstance(value, str):
                    # Escape single quotes by doubling them
                    escaped_value = value.replace("'", "''")
                    formatted_values.append(f"'{escaped_value}'")
                else:
                    formatted_values.append(str(value))
            value_rows.append(f"({', '.join(formatted_values)})")
        
        values_clause = f"VALUES {', '.join(value_rows)}"
        
        # Build the complete statement
        statement = f"INSERT INTO {self._table} {columns_clause} {values_clause}"
        
        if self._on_conflict:
            statement += f" {self._on_conflict}"
        
        return statement


class UpdateBuilder:
    """
    Builder for UPDATE statements.
    """
    
    def __init__(self):
        """Initialize a new UpdateBuilder."""
        self.reset()
    
    def reset(self):
        """Reset the update builder to initial state."""
        self._table = None
        self._set_clauses = []
        self._where_conditions = []
        return self
    
    def table(self, table_name: str):
        """
        Set the table to update.
        
        :param table_name: Name of the table
        :return: Self for method chaining
        """
        self._table = table_name
        return self
    
    def set(self, column: str, value: Any):
        """
        Add a SET clause for a column.
        
        :param column: Column name
        :param value: Value to set
        :return: Self for method chaining
        """
        if value is None:
            formatted_value = "NULL"
        elif isinstance(value, str):
            # Escape single quotes by doubling them
            escaped_value = value.replace("'", "''")
            formatted_value = f"'{escaped_value}'"
        else:
            formatted_value = str(value)
        
        self._set_clauses.append(f"{column} = {formatted_value}")
        return self
    
    def where(self, condition: str):
        """
        Add a WHERE condition.
        
        :param condition: WHERE condition
        :return: Self for method chaining
        """
        self._where_conditions.append(condition)
        return self
    
    def build(self) -> str:
        """
        Build and return the final UPDATE statement.
        
        :return: Complete UPDATE SQL statement
        """
        if not self._table:
            raise ValueError("Table name must be specified")
        if not self._set_clauses:
            raise ValueError("At least one SET clause must be specified")
        
        set_clause = f"SET {', '.join(self._set_clauses)}"
        where_clause = f"WHERE {' AND '.join(self._where_conditions)}" if self._where_conditions else ""
        
        statement = f"UPDATE {self._table} {set_clause}"
        if where_clause:
            statement += f" {where_clause}"
        
        return statement