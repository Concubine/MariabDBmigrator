"""Domain models for the MariaDB export tool."""
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Dict, Any, Optional

@dataclass
class DatabaseConfig:
    """Database configuration."""
    host: str
    port: int
    user: str
    password: str
    database: str
    use_pure: bool = True
    auth_plugin: Optional[str] = None
    ssl: bool = False
    ssl_ca: Optional[str] = None
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    ssl_verify_cert: bool = False
    ssl_verify_identity: bool = False

class ImportMode(Enum):
    """Import mode for handling existing tables."""
    SKIP = "skip"  # Skip if table exists
    OVERWRITE = "overwrite"  # Drop and recreate if exists
    MERGE = "merge"  # Keep structure but append data
    CANCEL = "cancel"  # Stop if any table exists

@dataclass
class TableMetadata:
    """Table metadata."""
    name: str
    columns: List[str]
    primary_key: Optional[List[str]]
    foreign_keys: List[Dict[str, str]]
    indexes: List[Dict[str, Any]]
    constraints: List[Dict[str, Any]]
    schema: Optional[str] = None
    definition: Optional[str] = None

@dataclass
class ColumnMetadata:
    """Column metadata."""
    name: str
    data_type: str
    is_nullable: bool
    default: Optional[str]
    extra: Optional[str]
    character_set: Optional[str]
    collation: Optional[str]
    column_type: str

@dataclass
class ExportOptions:
    """Options for exporting data."""
    batch_size: int = 1000
    where_clause: Optional[str] = None
    include_schema: bool = True
    include_indexes: bool = True
    include_constraints: bool = True
    compression: bool = False
    parallel_workers: int = 4

@dataclass
class ImportOptions:
    """Options for importing data."""
    mode: ImportMode = ImportMode.SKIP
    batch_size: int = 1000
    compression: bool = False
    include_schema: bool = True
    include_indexes: bool = True
    include_constraints: bool = True
    disable_foreign_keys: bool = True
    continue_on_error: bool = False
    parallel_workers: int = 4

@dataclass
class ExportResult:
    """Export result for a table."""
    
    def __init__(
        self,
        table_name: str,
        success: bool = False,
        rows_exported: int = 0,
        schema_exported: bool = False,
        data_exported: bool = False,
        file_path: Optional[str] = None,
        file_size: int = 0,
        duration: float = 0.0,
        error_message: Optional[str] = None,
        checksum: Optional[str] = None
    ):
        """Initialize export result."""
        self.table_name = table_name
        self.success = success
        self.rows_exported = rows_exported
        self.schema_exported = schema_exported
        self.data_exported = data_exported
        self.file_path = file_path
        self.file_size = file_size
        self.duration = duration
        self.error_message = error_message
        self.checksum = checksum

@dataclass
class ImportResult:
    """Import result for a file."""
    
    def __init__(
        self,
        table_name: str,
        status: str = "pending",
        rows_imported: int = 0,
        duration: float = 0.0,
        error_message: Optional[str] = None,
        database: Optional[str] = None,
        checksum_verified: bool = False,
        checksum_status: Optional[str] = None
    ):
        """Initialize import result."""
        self.table_name = table_name
        self.status = status
        self.rows_imported = rows_imported
        self.duration = duration
        self.error_message = error_message
        self.database = database
        self.checksum_verified = checksum_verified
        self.checksum_status = checksum_status

class ChecksumData:
    """Checksum data for data integrity verification."""
    
    def __init__(
        self,
        database: str,
        table: str,
        checksum: str,
        version: str = "1.0"
    ):
        """Initialize checksum data.
        
        Args:
            database: Database name
            table: Table name
            checksum: Checksum value
            version: Checksum format version
        """
        self.database = database
        self.table = table
        self.checksum = checksum
        self.version = version 