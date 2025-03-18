"""Export service implementation."""
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging

from ..core.exceptions import ExportError
from ..domain.interfaces import DatabaseInterface, StorageInterface
from ..domain.models import ExportOptions, ExportResult, TableMetadata

logger = logging.getLogger(__name__)

class ExportService:
    """Service for exporting database data."""
    
    def __init__(
        self,
        database: DatabaseInterface,
        storage: StorageInterface
    ):
        """Initialize the export service."""
        self.database = database
        self.storage = storage
    
    def export_table(
        self,
        table_name: str,
        output_dir: Path,
        options: ExportOptions
    ) -> ExportResult:
        """Export a single table to SQL files."""
        try:
            # Get table metadata
            table_metadata = self.database.get_table_metadata(table_name)
            
            # Create output directory
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Export schema if requested
            schema_file = None
            if options.include_schema and table_metadata.schema:
                schema_file = output_dir / f"{table_name}.schema.sql"
                self.storage.save_schema(
                    table_metadata.schema,
                    schema_file,
                    options.compression
                )
            
            # Export data
            data_file = output_dir / f"{table_name}.sql"
            total_rows = 0
            
            # Get data in batches
            for batch in self.database.get_table_data(
                table_name,
                options.batch_size,
                options.where_clause
            ):
                self.storage.save_data(
                    batch,
                    data_file,
                    options.compression
                )
                total_rows += len(batch)
            
            return ExportResult(
                table_name=table_name,
                total_rows=total_rows,
                data_file=data_file,
                schema_file=schema_file
            )
            
        except Exception as e:
            raise ExportError(f"Failed to export table {table_name}: {str(e)}")
    
    def export_tables(
        self,
        table_names: List[str],
        output_dir: Path,
        options: ExportOptions
    ) -> List[ExportResult]:
        """Export multiple tables to SQL files."""
        results = []
        
        for table_name in table_names:
            try:
                result = self.export_table(
                    table_name,
                    output_dir,
                    options
                )
                results.append(result)
                logger.info(
                    f"Exported table {table_name}: {result.total_rows} rows"
                )
            except ExportError as e:
                logger.error(f"Failed to export table {table_name}: {str(e)}")
                raise
        
        return results 