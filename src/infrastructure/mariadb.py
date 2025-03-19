"""MariaDB database operations implementation."""
import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Any, Optional, Iterator
import logging
from datetime import datetime, date

from src.core.exceptions import DatabaseError
from src.core.logging import get_logger
from ..domain.interfaces import DatabaseInterface
from ..domain.models import TableMetadata, ColumnMetadata, DatabaseConfig

logger = get_logger(__name__)

class MariaDB(DatabaseInterface):
    """Implementation of MariaDB database operations."""
    
    def __init__(self, config: DatabaseConfig):
        """Initialize the MariaDB connection."""
        self.config = {
            'host': config.host,
            'port': config.port,
            'user': config.user,
            'password': config.password,
            'database': config.database,
            'use_pure': getattr(config, 'use_pure', True)
        }
        self._connection = None
        self._cursor = None
    
    def connect(self) -> None:
        """Connect to the MariaDB database."""
        try:
            if not self._connection or not self._connection.is_connected():
                self._connection = mysql.connector.connect(**self.config)
                self._cursor = self._connection.cursor(dictionary=True)
                logger.info("Connected to MariaDB database")
        except Error as e:
            raise DatabaseError(f"Failed to connect to database: {str(e)}")
    
    def disconnect(self) -> None:
        """Disconnect from the MariaDB database."""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
            logger.info("Disconnected from MariaDB database")
    
    def _ensure_connected(self) -> None:
        """Ensure database connection is active."""
        if not self._connection or not self._connection.is_connected():
            self.connect()
    
    def get_table_names(self) -> List[str]:
        """Get list of all table names."""
        try:
            self._ensure_connected()
            self._cursor.execute("SHOW TABLES")
            return [row[list(row.keys())[0]] for row in self._cursor.fetchall()]
        except Error as e:
            raise DatabaseError(f"Failed to get table names: {str(e)}")
    
    def get_table_metadata(self, table_name: str) -> TableMetadata:
        """Get metadata for a specific table."""
        try:
            self._ensure_connected()
            
            # Get column metadata
            columns = self.get_column_metadata(table_name)
            column_names = [col.name for col in columns]
            
            # Get primary key
            self._cursor.execute(f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s
                AND TABLE_NAME = %s
                AND CONSTRAINT_NAME = 'PRIMARY'
            """, (self.config['database'], table_name))
            primary_key = [row['COLUMN_NAME'] for row in self._cursor.fetchall()]
            
            # Get foreign keys
            self._cursor.execute(f"""
                SELECT
                    COLUMN_NAME,
                    REFERENCED_TABLE_NAME,
                    REFERENCED_COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s
                AND TABLE_NAME = %s
                AND REFERENCED_TABLE_NAME IS NOT NULL
            """, (self.config['database'], table_name))
            foreign_keys = [
                {
                    'column': row['COLUMN_NAME'],
                    'ref_table': row['REFERENCED_TABLE_NAME'],
                    'ref_column': row['REFERENCED_COLUMN_NAME']
                }
                for row in self._cursor.fetchall()
            ]
            
            # Get indexes
            self._cursor.execute(f"""
                SELECT
                    INDEX_NAME,
                    COLUMN_NAME,
                    SEQ_IN_INDEX
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = %s
                AND TABLE_NAME = %s
                AND INDEX_NAME != 'PRIMARY'
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
            """, (self.config['database'], table_name))
            indexes = []
            current_index = None
            for row in self._cursor.fetchall():
                if current_index is None or current_index['name'] != row['INDEX_NAME']:
                    if current_index is not None:
                        indexes.append(current_index)
                    current_index = {
                        'name': row['INDEX_NAME'],
                        'columns': [row['COLUMN_NAME']]
                    }
                else:
                    current_index['columns'].append(row['COLUMN_NAME'])
            if current_index is not None:
                indexes.append(current_index)
            
            # Get constraints
            self._cursor.execute(f"""
                SELECT
                    tc.CONSTRAINT_NAME,
                    tc.CONSTRAINT_TYPE,
                    kcu.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                    AND tc.TABLE_NAME = kcu.TABLE_NAME
                WHERE tc.TABLE_SCHEMA = %s
                AND tc.TABLE_NAME = %s
                AND tc.CONSTRAINT_TYPE != 'PRIMARY KEY'
                AND tc.CONSTRAINT_TYPE != 'FOREIGN KEY'
            """, (self.config['database'], table_name))
            constraints = []
            current_constraint = None
            for row in self._cursor.fetchall():
                if current_constraint is None or current_constraint['name'] != row['CONSTRAINT_NAME']:
                    if current_constraint is not None:
                        constraints.append(current_constraint)
                    current_constraint = {
                        'name': row['CONSTRAINT_NAME'],
                        'type': row['CONSTRAINT_TYPE'],
                        'columns': [row['COLUMN_NAME']]
                    }
                else:
                    current_constraint['columns'].append(row['COLUMN_NAME'])
            if current_constraint is not None:
                constraints.append(current_constraint)
            
            # Get CREATE TABLE statement
            self._cursor.execute(f"SHOW CREATE TABLE {table_name}")
            schema = self._cursor.fetchone()['Create Table']
            
            return TableMetadata(
                name=table_name,
                columns=column_names,
                primary_key=primary_key,
                foreign_keys=foreign_keys,
                indexes=indexes,
                constraints=constraints,
                schema=schema
            )
            
        except Error as e:
            raise DatabaseError(f"Failed to get metadata for table {table_name}: {str(e)}")
    
    def get_column_metadata(self, table_name: str) -> List[ColumnMetadata]:
        """Get metadata for all columns in a table."""
        try:
            self._ensure_connected()
            self._cursor.execute(f"""
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    EXTRA,
                    CHARACTER_SET_NAME,
                    COLLATION_NAME,
                    COLUMN_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s
                AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (self.config['database'], table_name))
            
            return [
                ColumnMetadata(
                    name=row['COLUMN_NAME'],
                    data_type=row['DATA_TYPE'],
                    is_nullable=row['IS_NULLABLE'] == 'YES',
                    default=row['COLUMN_DEFAULT'],
                    extra=row['EXTRA'],
                    character_set=row['CHARACTER_SET_NAME'],
                    collation=row['COLLATION_NAME'],
                    column_type=row['COLUMN_TYPE']
                )
                for row in self._cursor.fetchall()
            ]
            
        except Error as e:
            raise DatabaseError(f"Failed to get column metadata for table {table_name}: {str(e)}")
    
    def get_table_data(
        self,
        table_name: str,
        batch_size: int = 1000,
        where_clause: Optional[str] = None
    ) -> Iterator[List[Dict[str, Any]]]:
        """Get data from a table in batches."""
        try:
            self._ensure_connected()
            
            # Build query
            query = f"SELECT * FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            
            # Get total count
            count_query = f"SELECT COUNT(*) as count FROM {table_name}"
            if where_clause:
                count_query += f" WHERE {where_clause}"
            self._cursor.execute(count_query)
            total_rows = self._cursor.fetchone()['count']
            
            # Fetch data in batches
            offset = 0
            while offset < total_rows:
                batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
                self._cursor.execute(batch_query)
                batch = self._cursor.fetchall()
                if not batch:
                    break
                yield batch
                offset += batch_size
            
        except Error as e:
            raise DatabaseError(f"Failed to get data from table {table_name}: {str(e)}")
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results."""
        try:
            self._ensure_connected()
            self._cursor.execute(query)
            return self._cursor.fetchall()
        except Error as e:
            raise DatabaseError(f"Failed to execute query: {str(e)}")
    
    def execute_batch(self, statements: List[str]) -> None:
        """Execute a batch of SQL statements."""
        try:
            self._ensure_connected()
            for statement in statements:
                self._cursor.execute(statement)
            self._connection.commit()
        except Error as e:
            self._connection.rollback()
            raise DatabaseError(f"Failed to execute batch: {str(e)}")
    
    def _format_value(self, value: Any) -> str:
        """Format a value for SQL insertion."""
        if value is None:
            return 'NULL'
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, (datetime, date)):
            return f"'{value.isoformat()}'"
        elif isinstance(value, bool):
            return '1' if value else '0'
        else:
            # Escape single quotes and backslashes
            escaped = str(value).replace("'", "''").replace("\\", "\\\\")
            return f"'{escaped}'"
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if the table exists, False otherwise
        """
        try:
            self._ensure_connected()
            self._cursor.execute("""
                SELECT COUNT(*) as count
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name = %s
            """, (self.config['database'], table_name))
            return self._cursor.fetchone()['count'] > 0
        except Error as e:
            raise DatabaseError(f"Failed to check if table {table_name} exists: {str(e)}")

    def drop_table(self, table_name: str) -> None:
        """Drop a table from the database.
        
        Args:
            table_name: Name of the table to drop
        """
        try:
            self._ensure_connected()
            self._cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            self._connection.commit()
        except Error as e:
            raise DatabaseError(f"Failed to drop table {table_name}: {str(e)}")

    def create_table(self, schema: str) -> None:
        """Create a table using the provided schema.
        
        Args:
            schema: CREATE TABLE statement
        """
        try:
            self._ensure_connected()
            self._cursor.execute(schema)
            self._connection.commit()
        except Error as e:
            raise DatabaseError(f"Failed to create table: {str(e)}")

    def import_table_data(self, table_name: str, data_file: str, batch_size: int = 1000) -> int:
        """Import data into a table from a SQL file.
        
        Args:
            table_name: Name of the table to import into
            data_file: Path to the SQL file containing INSERT statements
            batch_size: Number of rows to import in each batch
            
        Returns:
            Number of rows imported
        """
        try:
            self._ensure_connected()
            total_rows = 0
            
            with open(data_file, 'r') as f:
                batch = []
                for line in f:
                    line = line.strip()
                    if line and line.startswith('INSERT INTO'):
                        batch.append(line)
                        if len(batch) >= batch_size:
                            self.execute_batch(batch)
                            total_rows += len(batch)
                            batch = []
                
                if batch:
                    self.execute_batch(batch)
                    total_rows += len(batch)
            
            return total_rows
            
        except Error as e:
            raise DatabaseError(f"Failed to import data into table {table_name}: {str(e)}")
        except IOError as e:
            raise DatabaseError(f"Failed to read data file {data_file}: {str(e)}") 