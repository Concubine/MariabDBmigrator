"""Import service implementation."""
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging

from ..core.exceptions import ImportError
from ..domain.interfaces import DatabaseInterface, StorageInterface
from ..domain.models import ImportOptions, ImportResult, ImportMode

logger = logging.getLogger(__name__)

class ImportService:
    """Service for importing database data."""
    
    def __init__(
        self,
        database: DatabaseInterface,
        storage: StorageInterface
    ):
        """Initialize the import service."""
        self.database = database
        self.storage = storage
    
    def _check_table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        try:
            self.database.get_table_metadata(table_name)
            return True
        except:
            return False
    
    def _handle_existing_table(
        self,
        table_name: str,
        options: ImportOptions
    ) -> bool:
        """Handle existing table based on import mode.
        
        Returns:
            bool: True if import should proceed, False if it should be skipped.
        """
        exists = self._check_table_exists(table_name)
        if not exists:
            return True
            
        if options.mode == ImportMode.CANCEL:
            raise ImportError(
                f"Table {table_name} already exists and mode is CANCEL"
            )
        elif options.mode == ImportMode.SKIP:
            logger.warning(f"Skipping existing table: {table_name}")
            return False
        elif options.mode == ImportMode.OVERWRITE:
            logger.info(f"Dropping existing table: {table_name}")
            self.database.execute_query(f"DROP TABLE {table_name}")
            return True
        elif options.mode == ImportMode.MERGE:
            logger.info(f"Merging data into existing table: {table_name}")
            return True
        
        return True
    
    def import_table(
        self,
        table_name: str,
        input_dir: Path,
        options: ImportOptions
    ) -> ImportResult:
        """Import a single table from SQL files."""
        try:
            # Check if we should proceed with import
            should_import = self._handle_existing_table(table_name, options)
            if not should_import:
                return ImportResult(
                    table_name=table_name,
                    total_rows=0,
                    status="skipped"
                )
            
            # Disable foreign keys if requested
            if options.disable_foreign_keys:
                self.database.execute_query("SET FOREIGN_KEY_CHECKS = 0")
            
            try:
                # Import schema if requested
                if options.include_schema:
                    schema_file = input_dir / f"{table_name}.schema.sql"
                    if schema_file.exists():
                        schema = self.storage.load_schema(
                            schema_file,
                            options.compression
                        )
                        self.database.execute_query(schema)
                
                # Import data
                data_file = input_dir / f"{table_name}.sql"
                if not data_file.exists():
                    raise ImportError(f"Data file not found: {data_file}")
                
                total_rows = 0
                batch = []
                
                # Load and execute data in batches
                for row in self.storage.load_data(
                    data_file,
                    options.compression
                ):
                    batch.append(row)
                    if len(batch) >= options.batch_size:
                        self.database.execute_batch(batch)
                        total_rows += len(batch)
                        batch = []
                
                # Execute remaining rows
                if batch:
                    self.database.execute_batch(batch)
                    total_rows += len(batch)
                
                return ImportResult(
                    table_name=table_name,
                    total_rows=total_rows,
                    status="imported"
                )
                
            finally:
                # Re-enable foreign keys if they were disabled
                if options.disable_foreign_keys:
                    self.database.execute_query("SET FOREIGN_KEY_CHECKS = 1")
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to import table {table_name}: {error_msg}")
            
            if not options.continue_on_error:
                raise ImportError(f"Failed to import table {table_name}: {error_msg}")
            
            return ImportResult(
                table_name=table_name,
                total_rows=0,
                status="error",
                error_message=error_msg
            )
    
    def import_tables(
        self,
        table_names: List[str],
        input_dir: Path,
        options: ImportOptions
    ) -> List[ImportResult]:
        """Import multiple tables from SQL files."""
        results = []
        
        # Check for CANCEL mode first
        if options.mode == ImportMode.CANCEL:
            for table_name in table_names:
                if self._check_table_exists(table_name):
                    raise ImportError(
                        f"Table {table_name} exists and mode is CANCEL"
                    )
        
        # Import tables
        for table_name in table_names:
            try:
                result = self.import_table(
                    table_name,
                    input_dir,
                    options
                )
                results.append(result)
                
                if result.status == "imported":
                    logger.info(
                        f"Imported table {table_name}: {result.total_rows} rows"
                    )
                elif result.status == "skipped":
                    logger.warning(f"Skipped table {table_name}")
                else:
                    logger.error(
                        f"Failed to import table {table_name}: {result.error_message}"
                    )
                    
            except ImportError as e:
                if not options.continue_on_error:
                    raise
                logger.error(str(e))
        
        return results 