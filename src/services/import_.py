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
        # Implementation: Initialize components needed for database import
        self.database = database
        self.files = files
        self.config = config
        self.db = MariaDB(database)  # Database access layer
        self.storage = SQLStorage()  # File storage layer
        self.live_view_adapter = None
        
    def import_data(self) -> List[ImportResult]:
        """Import tables into the database.
        
        Returns:
            List of import results
            
        Raises:
            ImportError: If import fails
        """
        try:
            # Implementation: Connect to database and determine target database
            self.db.connect()
            
            # Track tables that have been dropped during import
            # This helps ensure we create tables before inserting data
            self.dropped_tables = set()
            
            # Track failed constraints to retry later
            self.failed_constraints = []
            
            # First check if a database name was specified in the import config
            # Implementation: Database target resolution logic with multiple fallbacks
            if self.config.database:
                logger.info(f"Using database name from import configuration: {self.config.database}")
                self.database.database = self.config.database
            # Then check if database is specified in database configuration
            elif not self.database.database:
                logger.info("No target database specified in configuration. Attempting to extract from SQL files...")
                
                # Initialize target database variable
                target_database = None
                
                # First check if there are any database info files
                # Implementation: Try to extract database name from info files first
                info_files = [f for f in self.files if f.endswith('_info.sql')]
                if info_files:
                    logger.info(f"Found database info files: {', '.join(info_files)}")
                    for info_file in info_files:
                        try:
                            with open(Path(info_file), 'r', encoding='utf-8') as f:
                                content = f.read()
                                # Extract database name from info file
                                db_match = re.search(r'--\s*Database:\s*([^\s\r\n]+)', content, re.IGNORECASE)
                                if db_match:
                                    target_database = db_match.group(1)
                                    logger.info(f"Found database name in info file: {target_database}")
                                    break
                        except Exception as e:
                            logger.warning(f"Error reading info file {info_file}: {str(e)}")
                            continue
                
                # If no info file found, try to extract from regular SQL files
                # Implementation: Fall back to examining SQL files for database references
                if not target_database:
                    for file_path in self.files:
                        try:
                            # Read the first few lines of the SQL file to look for database name
                            with open(Path(file_path), 'r', encoding='utf-8') as f:
                                content = ''.join(f.readline() for _ in range(20))  # Read first 20 lines
                                
                                # Look for CREATE DATABASE or USE statements
                                db_match = re.search(r'CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`"]?([^`"\s;]+)[`"]?', content, re.IGNORECASE)
                                if not db_match:
                                    # Updated regex to properly handle quoted database names and avoid including trailing punctuation
                                    db_match = re.search(r'USE\s+`([^`]+)`|USE\s+"([^"]+)"|USE\s+([^\s;,\'"`]+)', content, re.IGNORECASE)
                                    if db_match:
                                        # Get the first non-None capture group
                                        target_database = next(group for group in db_match.groups() if group is not None)
                                
                                # Look for special comment with database name (added by our export process)
                                if not db_match:
                                    db_match = re.search(r'--\s*Database:\s*([^\s\r\n]+)', content, re.IGNORECASE)
                                    if db_match:
                                        target_database = db_match.group(1)
                                        logger.info(f"Found database name in comment: {target_database}")
                                
                                # Look for table creation with database specified
                                if not db_match:
                                    db_match = re.search(r'CREATE\s+TABLE\s+(?:`([^`]+)`|"([^"]+)"|([^\s.`"]+))\.', content, re.IGNORECASE)
                                    if db_match:
                                        # Get the first non-None capture group
                                        target_database = next(group for group in db_match.groups() if group is not None)
                                        
                                # Check for comment that might include database name
                                if not db_match:
                                    db_match = re.search(r'--\s*(?:Database|DB):\s*[`"]?([^`"\r\n]+)[`"]?', content, re.IGNORECASE)
                                
                                if db_match and not target_database:
                                    target_database = db_match.group(1)
                                    logger.info(f"Found database name in SQL file: {target_database}")
                                    break
                        except Exception as e:
                            logger.warning(f"Error reading SQL file {file_path}: {str(e)}")
                            continue
                
                # If still no database found, use a default name
                # Implementation: Last resort - use default database name
                if not target_database:
                    # Instead of using a hardcoded default name, check for database name in configuration
                    if self.database.database:
                        target_database = self.database.database
                        logger.info(f"Using database name from configuration: {target_database}")
                    else:
                        # Use the original name if specified in file, otherwise fall back to default
                        target_database = "imported_db" 
                        logger.info(f"No database found in SQL files or configuration. Using default name: {target_database}")
                    
                # Update the database config
                self.database.database = target_database
            
            # Check if database exists and create it if not
            # Implementation: Create database if it doesn't exist
            available_dbs = self.db.get_available_databases()
            if self.database.database not in available_dbs:
                logger.info(f"Database '{self.database.database}' does not exist. Creating it.")
                self.db.execute(f"CREATE DATABASE IF NOT EXISTS `{self.database.database}`")
            
            # Select the database
            self.db.select_database(self.database.database)
            logger.info(f"Using database '{self.database.database}' for import")
            
            # Disable foreign key checks globally if configured
            # Implementation: Handle disabling foreign keys for import if requested
            if self.config.disable_foreign_keys:
                logger.info("Disabling foreign key checks for import")
                self.db.execute("SET FOREIGN_KEY_CHECKS=0")
                
            # Process files in a specific order
            # Implementation: Sort files to ensure schema is imported before data
            sorted_files = sorted(self.files, key=self.file_sort_key)
            
            # Import each file
            results = []
            for file_path in sorted_files:
                try:
                    result = self._import_file(Path(file_path))
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error importing file {file_path}: {str(e)}")
                    if not self.config.continue_on_error:
                        raise ImportError(f"Import failed: {str(e)}")
                        
            # Re-enable foreign key checks if they were disabled
            if self.config.disable_foreign_keys:
                logger.info("Re-enabling foreign key checks")
                self.db.execute("SET FOREIGN_KEY_CHECKS=1")
                
            # Apply any failed constraints that were saved for retry
            # Implementation: Apply deferred constraints after data import
            if self.failed_constraints:
                logger.info(f"Applying {len(self.failed_constraints)} deferred constraints")
                self._apply_deferred_constraints()
                
            return results
                
        except Exception as e:
            raise ImportError(f"Import failed: {str(e)}")
            
        finally:
            # Implementation: Ensure connection cleanup regardless of outcome
            # Re-enable foreign key checks if they were disabled
            if hasattr(self, 'config') and self.config.disable_foreign_keys:
                try:
                    self.db.execute("SET FOREIGN_KEY_CHECKS=1")
                except Exception as e:
                    logger.warning(f"Failed to re-enable foreign key checks: {str(e)}")
                    
            self.db.disconnect()

    def file_sort_key(self, file_path):
        """Generate a sort key for SQL files to ensure proper import order.
        
        Args:
            file_path: Path to the SQL file
            
        Returns:
            Sort key tuple
        """
        # Implementation: Custom sorting logic to ensure proper file import order:
        # 1. Database info files first
        # 2. Schema files before data files
        # 3. Alphabetical order within each category
        
        # ... existing code ...

    def _import_file(self, file_path: Path) -> ImportResult:
        """Import a single SQL file.
        
        Args:
            file_path: Path to the SQL file
            
        Returns:
            Import result
        """
        # Implementation: Multi-stage SQL file import process:
        # 1. Detect file type (schema or data)
        # 2. Parse and process statements according to type
        # 3. Track import results and handle errors
        
        # ... existing code ...
        
    def set_live_view_adapter(self, adapter) -> None:
        """Set the live view adapter for real-time progress display.
        
        Args:
            adapter: Live view adapter instance
        """
        # Implementation: Configure real-time progress monitoring
        self.live_view_adapter = adapter

    def _import_table(self, table: str) -> None:
        """Import a single table.
        
        Args:
            table: Table name
        """
        # Implementation: Core table import logic
        # ... existing code ...
        
    def _extract_create_table_statement(self, table_name: str) -> Optional[str]:
        """Extract the CREATE TABLE statement for a specific table from schema files.
        
        Args:
            table_name: Table name
            
        Returns:
            CREATE TABLE statement if found, None otherwise
        """
        # Implementation: Parse schema files to find specific table definition
        # ... existing code ...
        
    def _apply_deferred_constraints(self) -> None:
        """Apply constraints that were deferred during import due to dependencies."""
        # Implementation: Apply constraints after data import to avoid dependency issues
        # ... existing code ...
        
    def _process_sql_statements(self, content: str) -> List[str]:
        """Process and split SQL content into individual executable statements.
        
        Args:
            content: SQL content
            
        Returns:
            List of executable SQL statements
        """
        # Implementation: SQL parser to handle complex statement parsing
        # ... existing code ... 