"""Domain models for the MariaDB export tool."""
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Dict, Any, Optional

@dataclass
class DatabaseConfig:
    """Database configuration."""
    # Implementation: Connection parameters for MariaDB/MySQL
    host: str
    port: int
    user: str
    password: str
    database: str
    use_pure: bool = True  # Use pure Python implementation of the MySQL protocol
    auth_plugin: Optional[str] = None  # Authentication plugin (mysql_native_password, etc.)
    ssl: bool = False  # Whether to use SSL for connection
    ssl_ca: Optional[str] = None  # Path to CA certificate
    ssl_cert: Optional[str] = None  # Path to client certificate
    ssl_key: Optional[str] = None  # Path to client key
    ssl_verify_cert: bool = False  # Verify server certificate
    ssl_verify_identity: bool = False  # Verify server identity

class ImportMode(Enum):
    """Import mode for handling existing tables."""
    # Implementation: Different strategies for handling existing tables during import
    SKIP = "skip"  # Skip if table exists
    OVERWRITE = "overwrite"  # Drop and recreate if exists
    MERGE = "merge"  # Keep structure but append data
    CANCEL = "cancel"  # Stop if any table exists

@dataclass
class TableMetadata:
    """Table metadata."""
    # Implementation: Complete table structure information
    name: str  # Table name
    columns: List[str]  # Column names
    primary_key: Optional[List[str]]  # Primary key columns
    foreign_keys: List[Dict[str, str]]  # Foreign key relationships
    indexes: List[Dict[str, Any]]  # Index definitions
    constraints: List[Dict[str, Any]]  # Constraint definitions
    schema: Optional[str] = None  # CREATE TABLE statement
    definition: Optional[str] = None  # Full table definition including constraints

@dataclass
class ColumnMetadata:
    """Column metadata."""
    # Implementation: Detailed column information
    name: str  # Column name
    data_type: str  # SQL data type
    is_nullable: bool  # Whether NULL values are allowed
    default: Optional[str]  # Default value
    extra: Optional[str]  # Additional information (auto_increment, etc.)
    character_set: Optional[str]  # Character set
    collation: Optional[str]  # Collation
    column_type: str  # Full type definition including length/precision

@dataclass
class ExportOptions:
    """Options for exporting data."""
    # Implementation: Configuration parameters for export operation
    batch_size: int = 1000  # Number of rows per batch
    where_clause: Optional[str] = None  # Optional filtering clause
    include_schema: bool = True  # Whether to export table schema
    include_indexes: bool = True  # Whether to export indexes
    include_constraints: bool = True  # Whether to export constraints
    compression: bool = False  # Whether to compress output files
    parallel_workers: int = 4  # Number of parallel worker threads

@dataclass
class ImportOptions:
    """Options for importing data."""
    # Implementation: Configuration parameters for import operation
    mode: ImportMode = ImportMode.SKIP  # How to handle existing tables
    batch_size: int = 1000  # Number of rows per batch
    compression: bool = False  # Whether input files are compressed
    include_schema: bool = True  # Whether to import table schema
    include_indexes: bool = True  # Whether to import indexes
    include_constraints: bool = True  # Whether to import constraints
    disable_foreign_keys: bool = True  # Whether to disable foreign key checks during import
    continue_on_error: bool = False  # Whether to continue on error
    parallel_workers: int = 4  # Number of parallel worker threads

@dataclass
class ExportResult:
    """Result of an export operation."""
    # Implementation: Export operation result tracking
    table_name: str  # Name of exported table
    total_rows: int  # Number of rows exported
    schema_file: Optional[Path] = None  # Path to schema file
    data_file: Optional[Path] = None  # Path to data file

@dataclass
class ImportResult:
    """Result of an import operation."""
    # Implementation: Import operation result tracking
    table_name: str  # Name of imported table
    status: str  # Status of import (success, error, skipped)
    total_rows: int = 0  # Number of rows imported
    error_message: Optional[str] = None  # Error message if import failed

@dataclass
class TableInfo:
    """Table information for tracking export/import operations."""
    # Implementation: Basic table information for tracking operations
    name: str  # Table name
    row_count: Optional[int] = None  # Number of rows in the table
    size_bytes: Optional[int] = None  # Size of table data in bytes
    has_schema: bool = True  # Whether the table has schema information
    has_data: bool = True  # Whether the table has data 