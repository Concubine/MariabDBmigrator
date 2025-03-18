"""Domain models."""
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
    """Export options."""
    batch_size: int = 1000
    where_clause: Optional[str] = None
    include_schema: bool = True
    include_indexes: bool = True
    include_constraints: bool = True
    compression: bool = False

@dataclass
class ImportOptions:
    """Import options."""
    mode: ImportMode = ImportMode.SKIP  # How to handle existing tables
    batch_size: int = 1000
    compression: bool = False
    include_schema: bool = True  # Whether to import schema files
    include_indexes: bool = True  # Whether to import indexes
    include_constraints: bool = True  # Whether to import constraints
    disable_foreign_keys: bool = True  # Whether to disable foreign keys during import
    continue_on_error: bool = False  # Whether to continue if an error occurs

@dataclass
class ExportResult:
    """Export result."""
    table_name: str
    total_rows: int
    data_file: Path
    schema_file: Optional[Path] = None

@dataclass
class ImportResult:
    """Import result."""
    table_name: str
    total_rows: int
    status: str  # 'imported', 'skipped', 'error'
    error_message: Optional[str] = None 