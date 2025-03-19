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
    total_rows: int
    schema_file: Optional[Path] = None
    data_file: Optional[Path] = None

@dataclass
class ImportResult:
    """Result of an import operation."""
    table_name: str
    status: str
    total_rows: int = 0
    error_message: Optional[str] = None 