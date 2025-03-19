"""Import service for MariaDB data."""
import logging
from pathlib import Path
from typing import List, Optional

from src.core.exceptions import ImportError
from src.domain.models import ImportOptions, ImportResult, ImportMode
from src.infrastructure.mariadb import MariaDB
from src.infrastructure.storage import SQLStorage
from src.infrastructure.parallel import ParallelWorker, WorkerConfig

logger = logging.getLogger(__name__)

class ImportService:
    """Service for importing data into MariaDB."""
    
    def __init__(self, database: MariaDB, storage: SQLStorage):
        """Initialize the import service.
        
        Args:
            database: Database connection
            storage: Storage handler
        """
        self.database = database
        self.storage = storage
    
    def import_tables(
        self,
        tables: List[str],
        input_dir: Path,
        options: ImportOptions
    ) -> List[ImportResult]:
        """Import multiple tables in parallel.
        
        Args:
            tables: List of table names to import
            input_dir: Input directory containing SQL files
            options: Import options
            
        Returns:
            List of import results
            
        Raises:
            ImportError: If import fails
        """
        if not input_dir.exists():
            raise ImportError(f"Input directory not found: {input_dir}")
        
        # Create worker config
        worker_config = WorkerConfig(
            max_workers=options.parallel_workers,
            use_processes=False,  # Use threads for I/O-bound operations
            chunk_size=1  # Process one table at a time
        )
        
        # Import tables in parallel
        with ParallelWorker(worker_config) as worker:
            results = worker.map(
                lambda table: self._import_table(table, input_dir, options),
                tables
            )
        
        return results
    
    def _import_table(
        self,
        table: str,
        input_dir: Path,
        options: ImportOptions
    ) -> ImportResult:
        """Import a single table.
        
        Args:
            table: Table name to import
            input_dir: Input directory
            options: Import options
            
        Returns:
            Import result
            
        Raises:
            ImportError: If import fails
        """
        try:
            logger.info(f"Importing table: {table}")
            
            # Check if table exists
            if self.database.table_exists(table):
                if options.mode == ImportMode.CANCEL:
                    return ImportResult(
                        table_name=table,
                        status="skipped",
                        error_message="Table already exists"
                    )
                elif options.mode == ImportMode.OVERWRITE:
                    self.database.drop_table(table)
                elif options.mode == ImportMode.SKIP:
                    return ImportResult(
                        table_name=table,
                        status="skipped",
                        error_message="Table already exists"
                    )
            
            # Import schema if requested
            schema_file = input_dir / f"{table}.schema.sql"
            if options.include_schema and schema_file.exists():
                schema = self.storage.load_schema(schema_file)
                self.database.create_table(schema)
            
            # Import data
            data_file = input_dir / f"{table}.sql"
            if not data_file.exists():
                raise ImportError(f"Data file not found: {data_file}")
            
            total_rows = self.database.import_table_data(
                table,
                data_file,
                options.batch_size
            )
            
            return ImportResult(
                table_name=table,
                status="success",
                total_rows=total_rows
            )
            
        except Exception as e:
            if options.continue_on_error:
                return ImportResult(
                    table_name=table,
                    status="error",
                    error_message=str(e)
                )
            raise ImportError(f"Failed to import table {table}: {e}") 