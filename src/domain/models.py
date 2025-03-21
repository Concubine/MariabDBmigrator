"""Domain models for the MariaDB export tool."""
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Set

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
    """Result of an export operation."""
    table_name: str
    success: bool = True
    rows_exported: int = 0
    file_path: Optional[str] = None
    file_size: int = 0
    duration: float = 0.0
    error_message: Optional[str] = None
    schema_file: Optional[Path] = None
    data_file: Optional[Path] = None
    total_rows: int = 0  # For backward compatibility

@dataclass
class ImportResult:
    """Result of an import operation."""
    table_name: str
    status: str
    file_name: str = ""
    source_path: str = ""
    error_message: str = ""
    rows_imported: int = 0
    expected_rows: int = 0
    duration: float = 0.0
    warnings: List[str] = field(default_factory=list)
    success: bool = True  # Overall success flag
    total_rows: int = 0  # Total rows processed (for backward compatibility)
    validation_details: Optional[Dict[str, Any]] = None  # Additional validation details
    
    def __post_init__(self):
        """Set success flag based on status if not explicitly set."""
        if self.status == "error":
            self.success = False 