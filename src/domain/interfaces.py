"""Abstract interfaces for the MariaDB export tool."""
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Iterator
from pathlib import Path

from .models import (
    TableMetadata,
    ColumnMetadata,
    ExportOptions,
    ImportOptions,
    ExportResult,
    ImportResult
)

class DatabaseInterface(ABC):
    """Interface for database operations."""
    
    @abstractmethod
    def connect(self) -> None:
        """Connect to the database."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Disconnect from the database."""
        pass
    
    @abstractmethod
    def get_table_names(self) -> List[str]:
        """Get list of all table names."""
        pass
    
    @abstractmethod
    def get_table_metadata(self, table_name: str) -> TableMetadata:
        """Get metadata for a specific table."""
        pass
    
    @abstractmethod
    def get_column_metadata(self, table_name: str) -> List[ColumnMetadata]:
        """Get metadata for all columns in a table."""
        pass
    
    @abstractmethod
    def get_table_data(
        self,
        table_name: str,
        batch_size: int = 1000,
        where_clause: Optional[str] = None
    ) -> Iterator[List[Dict[str, Any]]]:
        """Get data from a table in batches."""
        pass
    
    @abstractmethod
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results."""
        pass
    
    @abstractmethod
    def execute_batch(self, statements: List[str]) -> None:
        """Execute a batch of SQL statements."""
        pass

class ExportInterface(ABC):
    """Abstract interface for export operations."""
    
    @abstractmethod
    def export_table(
        self,
        table_name: str,
        options: ExportOptions,
        output_path: Path
    ) -> ExportResult:
        """Export a table to a file."""
        pass
    
    @abstractmethod
    def export_query(
        self,
        query: str,
        options: ExportOptions,
        output_path: Path
    ) -> ExportResult:
        """Export results of a custom query to a file."""
        pass

class StorageInterface(ABC):
    """Interface for storage operations."""
    
    @abstractmethod
    def save_data(
        self,
        data: List[Dict[str, Any]],
        file_path: Path,
        compression: bool = False
    ) -> None:
        """Save data to a SQL file."""
        pass
    
    @abstractmethod
    def load_data(
        self,
        file_path: Path,
        compression: bool = False
    ) -> List[Dict[str, Any]]:
        """Load data from a SQL file."""
        pass
    
    @abstractmethod
    def save_schema(
        self,
        schema: str,
        file_path: Path,
        compression: bool = False
    ) -> None:
        """Save table schema to a SQL file."""
        pass
    
    @abstractmethod
    def load_schema(
        self,
        file_path: Path,
        compression: bool = False
    ) -> str:
        """Load table schema from a SQL file."""
        pass

class ProgressInterface(ABC):
    """Interface for progress tracking."""
    
    @abstractmethod
    def initialize(self, total: int) -> None:
        """Initialize progress tracking."""
        pass
    
    @abstractmethod
    def update(self, current: int) -> None:
        """Update progress."""
        pass
    
    @abstractmethod
    def finish(self) -> None:
        """Finish progress tracking."""
        pass 