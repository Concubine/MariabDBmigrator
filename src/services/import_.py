"""Import service for MariaDB data."""
import logging
from pathlib import Path
from typing import List, Optional

from src.core.logging import get_logger
from src.core.exceptions import ImportError
from src.core.config import ImportConfig, DatabaseConfig
from src.domain.models import ImportResult, ImportMode
from src.infrastructure.mariadb import MariaDB
from src.infrastructure.storage import SQLStorage
from src.infrastructure.parallel import ParallelWorker, WorkerConfig

logger = get_logger(__name__)

class ImportService:
    """Service for importing database tables."""
    
    def __init__(self, database: DatabaseConfig, files: List[str], config: ImportConfig):
        """Initialize the import service.
        
        Args:
            database: Database configuration
            files: List of SQL files to import
            config: Import configuration
        """
        self.database = database
        self.files = files
        self.config = config
        self.db = MariaDB(database)
        self.storage = SQLStorage()
        
    def import_data(self) -> List[ImportResult]:
        """Import tables into the database.
        
        Returns:
            List of import results
            
        Raises:
            ImportError: If import fails
        """
        try:
            # Connect to database
            self.db.connect()
            
            # Import each file
            results = []
            for file_path in self.files:
                result = self._import_file(Path(file_path))
                results.append(result)
                
            return results
            
        except Exception as e:
            raise ImportError(f"Import failed: {str(e)}")
            
        finally:
            self.db.disconnect()
            
    def _import_file(self, file_path: Path) -> ImportResult:
        """Import a single SQL file.
        
        Args:
            file_path: Path to SQL file
            
        Returns:
            Import result
        """
        if not file_path.exists():
            return ImportResult(
                table_name=file_path.stem,
                status="error",
                error_message=f"File not found: {file_path}"
            )
            
        try:
            # Get table name from file name
            table_name = file_path.stem
            
            # Check if table exists
            if self.db.table_exists(table_name):
                if self.config.mode == ImportMode.SKIP.value:
                    logger.info(f"Skipping table {table_name} - already exists")
                    return ImportResult(table_name=table_name, status="skipped")
                    
                elif self.config.mode == ImportMode.OVERWRITE.value:
                    logger.info(f"Dropping existing table {table_name}")
                    self.db.drop_table(table_name)
                    
                elif self.config.mode == ImportMode.CANCEL.value:
                    logger.info(f"Skipping table {table_name} - already exists")
                    return ImportResult(
                        table_name=table_name,
                        status="error",
                        error_message="Table already exists"
                    )
                    
            # Create table if schema import is enabled
            if self.config.import_schema:
                logger.info(f"Creating table {table_name}")
                schema = self.storage.read_schema(file_path)
                self.db.create_table(table_name, schema)
                
            # Import data if enabled
            total_rows = 0
            if self.config.import_data:
                # Configure parallel worker
                worker_config = WorkerConfig(
                    max_workers=self.config.parallel_workers,
                    batch_size=self.config.batch_size
                )
                
                # Import data in parallel
                logger.info(f"Importing data into table {table_name}")
                worker = ParallelWorker(worker_config)
                total_rows = worker.process_table(
                    table=table_name,
                    database=self.database,
                    input_file=file_path,
                    disable_foreign_keys=self.config.disable_foreign_keys
                )
                
                logger.info(f"Successfully imported {total_rows} rows into table {table_name}")
                
            return ImportResult(
                table_name=table_name,
                status="success",
                total_rows=total_rows
            )
            
        except Exception as e:
            error_msg = str(e)
            if self.config.continue_on_error:
                logger.warning(f"Error importing {file_path.name}: {error_msg}")
                return ImportResult(
                    table_name=file_path.stem,
                    status="error",
                    error_message=error_msg
                )
            else:
                raise ImportError(f"Error importing {file_path.name}: {error_msg}")
                
    def _import_table(self, table: str) -> None:
        """Import a single table.
        
        Args:
            table: Name of table to import
        """
        logger.info(f"Importing table: {table}")
        
        # Get table metadata
        metadata = self.db.get_table_metadata(table)
        
        # Create table if it doesn't exist
        if not self.db.table_exists(table):
            self.db.create_table(table, metadata.schema)
            
        # Import data
        if self.config.disable_foreign_keys:
            self.db.execute("SET FOREIGN_KEY_CHECKS=0")
            
        try:
            # Configure parallel worker
            worker_config = WorkerConfig(
                max_workers=self.config.parallel_workers,
                batch_size=self.config.batch_size
            )
            
            # Import data in parallel
            worker = ParallelWorker(worker_config)
            worker.process_table(
                table=table,
                database=self.database,
                input_file=Path(f"{table}.sql"),
                disable_foreign_keys=self.config.disable_foreign_keys
            )
            
        finally:
            if self.config.disable_foreign_keys:
                self.db.execute("SET FOREIGN_KEY_CHECKS=1") 