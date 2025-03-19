"""Import service for MariaDB data."""
import logging
from pathlib import Path
from typing import List, Optional
import re
import sqlparse

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
            
            # Check if database is specified in configuration
            if not self.database.database:
                logger.info("No target database specified in configuration. Attempting to extract from SQL files...")
                
                # Try to extract database name from SQL files
                target_database = None
                for file_path in self.files:
                    try:
                        # Read the first few lines of the SQL file to look for database name
                        with open(Path(file_path), 'r', encoding='utf-8') as f:
                            content = ''.join(f.readline() for _ in range(20))  # Read first 20 lines
                            
                            # Look for CREATE DATABASE or USE statements
                            import re
                            db_match = re.search(r'CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`"]?([^`"\s]+)[`"]?', content, re.IGNORECASE)
                            if not db_match:
                                db_match = re.search(r'USE\s+[`"]?([^`"\s]+)[`"]?', content, re.IGNORECASE)
                            
                            # Look for table creation with database specified
                            if not db_match:
                                db_match = re.search(r'CREATE\s+TABLE\s+[`"]?([^`"\s\.]+)[`"]?\.[`"]?([^`"\s]+)[`"]?', content, re.IGNORECASE)
                                
                            if db_match:
                                target_database = db_match.group(1)
                                logger.info(f"Found database name in SQL file: {target_database}")
                                break
                    except Exception as e:
                        logger.warning(f"Error reading SQL file {file_path}: {str(e)}")
                        continue
                
                # If still no database found, use a default name
                if not target_database:
                    target_database = "imported_db"
                    logger.info(f"No database found in SQL files. Using default name: {target_database}")
                
                # Update the database config
                self.database.database = target_database
            
            # Check if database exists and create it if not
            available_dbs = self.db.get_available_databases()
            if self.database.database not in available_dbs:
                logger.info(f"Database '{self.database.database}' does not exist. Creating it.")
                self.db.execute(f"CREATE DATABASE IF NOT EXISTS `{self.database.database}`")
            
            # Select the database
            self.db.select_database(self.database.database)
            logger.info(f"Using database '{self.database.database}' for import")
            
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
            # Read the SQL file content
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Extract table name from CREATE TABLE statement if present
            create_table_match = re.search(r'CREATE\s+TABLE\s+(?:`?([^`\s\.]+)`?\.)?`?([^`\s]+)`?', sql_content, re.IGNORECASE)
            
            if create_table_match:
                # Group 1 is the database name (if present), group 2 is the table name
                db_name = create_table_match.group(1)
                table_name = create_table_match.group(2)
                
                if db_name and db_name != self.database.database:
                    logger.warning(f"SQL file contains a different database name ({db_name}) than the configured one ({self.database.database})")
                    # Replace database name in SQL content if needed
                    sql_content = re.sub(
                        r'(`?' + re.escape(db_name) + r'`?\.)',
                        f'`{self.database.database}`.',
                        sql_content
                    )
            else:
                # If no CREATE TABLE statement found, use the filename as table name
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
                    
            # Split SQL file into statements
            statements = sqlparse.split(sql_content)
            
            # Execute each statement
            data_statements = []
            for stmt in statements:
                stmt = stmt.strip()
                if not stmt:
                    continue
                    
                # Execute schema statements directly
                if stmt.upper().startswith(('CREATE', 'ALTER', 'DROP')) and self.config.import_schema:
                    logger.info(f"Executing schema statement for {table_name}")
                    self.db.execute(stmt)
                # Collect data statements for batch execution
                elif stmt.upper().startswith('INSERT') and self.config.import_data:
                    data_statements.append(stmt)
            
            # Import data if enabled
            total_rows = 0
            if data_statements and self.config.import_data:
                # Execute in batches
                batch_size = self.config.batch_size
                for i in range(0, len(data_statements), batch_size):
                    batch = data_statements[i:i+batch_size]
                    self.db.execute_batch(batch)
                    total_rows += len(batch)
                
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