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
        # Implementation contract: Establish connection to database with configured parameters
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Disconnect from the database."""
        # Implementation contract: Close connection and release resources
        pass
    
    @abstractmethod
    def get_table_names(self) -> List[str]:
        """Get list of all table names."""
        # Implementation contract: Return list of all tables in current database
        pass
    
    @abstractmethod
    def get_table_metadata(self, table_name: str) -> TableMetadata:
        """Get metadata for a specific table."""
        # Implementation contract: Retrieve comprehensive metadata for specified table
        pass
    
    @abstractmethod
    def get_column_metadata(self, table_name: str) -> List[ColumnMetadata]:
        """Get metadata for all columns in a table."""
        # Implementation contract: Return detailed metadata for all columns in specified table
        pass
    
    @abstractmethod
    def get_table_data(
        self,
        table_name: str,
        batch_size: int = 1000,
        where_clause: Optional[str] = None
    ) -> Iterator[List[Dict[str, Any]]]:
        """Get data from a table in batches."""
        # Implementation contract: Retrieve data in memory-efficient batches with filtering
        pass
    
    @abstractmethod
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results."""
        # Implementation contract: Execute arbitrary SQL and return results as dictionaries
        pass
    
    @abstractmethod
    def execute_batch(self, statements: List[str]) -> None:
        """Execute a batch of SQL statements."""
        # Implementation contract: Execute multiple SQL statements efficiently
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
        # Implementation contract: Export table schema and/or data to file with configured options
        pass
    
    @abstractmethod
    def export_query(
        self,
        query: str,
        options: ExportOptions,
        output_path: Path
    ) -> ExportResult:
        """Export results of a custom query to a file."""
        # Implementation contract: Execute query and export results to file
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
        # Implementation contract: Convert data to SQL and write to file
        pass
    
    @abstractmethod
    def load_data(
        self,
        file_path: Path,
        compression: bool = False
    ) -> List[Dict[str, Any]]:
        """Load data from a SQL file."""
        # Implementation contract: Parse SQL file and return data as dictionaries
        pass
    
    @abstractmethod
    def save_schema(
        self,
        schema: str,
        file_path: Path,
        compression: bool = False
    ) -> None:
        """Save table schema to a SQL file."""
        # Implementation contract: Write schema SQL to file with optional compression
        pass
    
    @abstractmethod
    def load_schema(
        self,
        file_path: Path,
        compression: bool = False
    ) -> str:
        """Load table schema from a SQL file."""
        # Implementation contract: Read schema SQL from file with optional decompression
        pass

class ProgressInterface(ABC):
    """Interface for progress tracking."""
    
    @abstractmethod
    def initialize(self, total: int) -> None:
        """Initialize progress tracking."""
        # Implementation contract: Setup progress tracking with specified total
        pass
    
    @abstractmethod
    def update(self, current: int) -> None:
        """Update progress."""
        # Implementation contract: Update progress display with current value
        pass
    
    @abstractmethod
    def finish(self) -> None:
        """Finish progress tracking."""
        # Implementation contract: Complete progress tracking and cleanup resources
        pass 