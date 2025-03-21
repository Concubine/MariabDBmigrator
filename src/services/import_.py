"""Import service for importing tables from SQL files."""
import logging
import time
import os
import hashlib
import json
from pathlib import Path
from typing import List, Optional, Any, Dict, Tuple
import re
import sqlparse

from src.core.logging import get_logger
from src.core.exceptions import ImportError
from src.core.config import ImportConfig, DatabaseConfig
from src.domain.models import ImportResult, ImportMode
from src.infrastructure.mariadb import MariaDB
from src.infrastructure.storage import SQLStorage
from src.infrastructure.parallel import ParallelWorker, WorkerConfig
from src.ui.progress import ProgressTracker, ProgressStats

logger = get_logger(__name__)

class ImportResult:
    """Result of an import operation."""
    
    def __init__(self, file_name=None, file_path=None, status="pending", 
                 error=None, warning=None, statements_total=0, statements_executed=0,
                 expected_rows=0, actual_rows=0, start_time=None, end_time=None,
                 duration=0, checksum=None, file_type=None):
        """Initialize import result.
        
        Args:
            file_name: Name of the imported file
            file_path: Path to the imported file
            status: Status of the import (pending, success, warning, error)
            error: Error message if import failed
            warning: Warning message if import had issues
            statements_total: Total number of SQL statements in the file
            statements_executed: Number of successfully executed statements
            expected_rows: Expected number of rows (for data imports)
            actual_rows: Actual number of rows imported
            start_time: Import start time
            end_time: Import end time
            duration: Import duration in seconds
            checksum: File checksum for verification
            file_type: Type of file (schema, data, triggers, etc.)
        """
        self.file_name = file_name
        self.file_path = file_path
        self.status = status
        self.error = error
        self.warning = warning
        self.statements_total = statements_total
        self.statements_executed = statements_executed
        self.expected_rows = expected_rows
        self.actual_rows = actual_rows
        self.start_time = start_time
        self.end_time = end_time
        self.duration = duration
        self.checksum = checksum
        self.file_type = file_type
        self.errors = []  # List to store multiple errors
        
    def __str__(self):
        """Return string representation of import result."""
        status_str = f"{self.status.upper()}"
        if self.error:
            status_str += f": {self.error}"
        elif self.warning:
            status_str += f": {self.warning}"
            
        if self.file_type and self.file_type != "unknown":
            file_info = f"{self.file_name} ({self.file_type})"
        else:
            file_info = self.file_name
            
        if self.duration:
            duration_str = f" in {self.duration:.2f}s"
        else:
            duration_str = ""
            
        statement_info = ""
        if self.statements_total > 0:
            statement_info = f", {self.statements_executed}/{self.statements_total} statements"
            
        row_info = ""
        if self.expected_rows > 0:
            row_info = f", {self.actual_rows}/{self.expected_rows} rows"
            
        return f"{file_info}: {status_str}{duration_str}{statement_info}{row_info}"

class ImportService:
    """Service for importing data from SQL files."""

    def __init__(self, mariadb, storage_service, files: List[str] = None, config: ImportConfig = None):
        """Initialize the ImportService.
        
        Args:
            mariadb: MariaDB instance
            storage_service: Storage service
            files: List of files to import
            config: Import configuration
        """
        if not config:
            config = ImportConfig()
        
        self.files = files or []
        self.config = config
        self.db = mariadb
        self.storage = storage_service
        
        # Metadata files are processed separately
        self.metadata_files = [f for f in self.files if os.path.basename(f) == 'metadata.json']
        # Other files are used for actual import
        self.import_files = [f for f in self.files if os.path.basename(f) != 'metadata.json']
        
        # Log metadata files found
        if self.metadata_files:
            logger.info(f"Found {len(self.metadata_files)} metadata files: {', '.join(self.metadata_files)}")
        else:
            logger.warning("No metadata.json files found in the import list")
            # Try to find metadata.json in subdirectories
            for file_path in self.files:
                # Get the containing directory
                dir_path = os.path.dirname(file_path)
                potential_metadata_path = os.path.join(dir_path, 'metadata.json')
                if os.path.exists(potential_metadata_path) and os.path.isfile(potential_metadata_path):
                    logger.info(f"Found metadata file in import directory: {potential_metadata_path}")
                    self.metadata_files.append(potential_metadata_path)
                    break
        
        # Fix: Get database from config instead of directly accessing the attribute
        self.database = mariadb.config.get('database', '')
        logger.info(f"Database from config: '{self.database}'")
        
        # If database is not specified in config, try to get it from metadata
        if not self.database and self.metadata_files:
            try:
                metadata_file = self.metadata_files[0]
                logger.info(f"Trying to read database from metadata file: {metadata_file}")
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                    if 'database' in metadata:
                        self.database = metadata['database']
                        logger.info(f"Using database from metadata: {self.database}")
                    else:
                        logger.warning(f"Metadata file does not contain 'database' field: {metadata}")
            except Exception as e:
                logger.warning(f"Could not read metadata file: {str(e)}")
                # Try to check if file exists
                if not os.path.exists(metadata_file):
                    logger.error(f"Metadata file not found: {metadata_file}")
                elif not os.path.isfile(metadata_file):
                    logger.error(f"Metadata file is not a file: {metadata_file}")
                elif os.path.getsize(metadata_file) == 0:
                    logger.error(f"Metadata file is empty: {metadata_file}")
        elif not self.database:
            logger.warning("No database specified in config and no metadata files found")
        
        self.logger = logging.getLogger('import')
        
        # Track checksums for imported files
        self.checksums = {}
        
        # Configure logging
        if hasattr(config, 'log_level') and config.log_level:
            self.logger.setLevel(config.log_level)
        
        if hasattr(config, 'log_file') and config.log_file:
            file_handler = logging.FileHandler(config.log_file)
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(file_handler)
        
        # Initialize table name mapping if provided
        self.table_mapping = {}
        if hasattr(self.config, 'table_mapping') and isinstance(self.config.table_mapping, dict):
            self.table_mapping = self.config.table_mapping
            logger.info(f"Using table name mapping for {len(self.table_mapping)} tables")
        
    def import_data(self) -> List[ImportResult]:
        """Import data from the specified files."""
        self.logger.info(f"Importing {len(self.import_files)} files")
        
        # Ensure database is selected
        if not self.database:
            self.logger.error("No database specified for import")
            raise ValueError("No database specified for import. Please specify a database in the configuration.")
            
        # Check if database exists and create it if needed
        try:
            # First check if database exists
            existing_databases = self.db.get_available_databases()
            if self.database not in existing_databases:
                self.logger.info(f"Database '{self.database}' does not exist, creating it")
                self.db.execute(f"CREATE DATABASE `{self.database}`")
                self.logger.info(f"Created database: {self.database}")
            
            # Then select the database
            self.db.execute(f"USE `{self.database}`")
            self.logger.info(f"Selected database: {self.database}")
        except Exception as e:
            self.logger.error(f"Failed to select database {self.database}: {str(e)}")
            raise ValueError(f"Failed to select database {self.database}: {str(e)}")
            
        # Organize files by type for proper import order
        table_schema_files = []
        table_data_files = []
        triggers_files = []
        procedures_files = []
        views_files = []  
        events_files = []
        functions_files = []
        user_types_files = []
        
        # Sort files into their respective categories
        for file in self.import_files:
            filename = os.path.basename(file).lower()
            if filename.endswith('_schema.sql'):
                table_schema_files.append(file)
            elif filename.endswith('_data.sql'):
                table_data_files.append(file)
            elif filename == 'triggers.sql':
                triggers_files.append(file)
            elif filename == 'procedures.sql':
                procedures_files.append(file)
            elif filename == 'views.sql':
                views_files.append(file)
            elif filename == 'events.sql':
                events_files.append(file)
            elif filename == 'functions.sql':
                functions_files.append(file)
            elif filename == 'user_types.sql':
                user_types_files.append(file)
                
        # Check if we have each type of file
        has_tables = len(table_schema_files) > 0
        has_triggers = len(triggers_files) > 0
        has_procedures = len(procedures_files) > 0
        has_views = len(views_files) > 0
        has_events = len(events_files) > 0
        has_functions = len(functions_files) > 0
        has_user_types = len(user_types_files) > 0
        
        # Skip, Merge, Overwrite, Force Drop, or Cancel
        if self.config.mode == ImportMode.SKIP:
            self.logger.info("Import mode: SKIP - Skipping existing objects")
        elif self.config.mode == ImportMode.MERGE:  
            self.logger.info("Import mode: MERGE - Merging with existing objects")
        elif self.config.mode == ImportMode.OVERWRITE:
            self.logger.info("Import mode: OVERWRITE - Overwriting existing objects")
            self._handle_overwrite_mode(table_schema_files)
        elif self.config.mode == ImportMode.CANCEL:
            self.logger.info("Import mode: CANCEL - Will cancel on collision")
            
        # Handle force drop if configured
        if self.config.force_drop:
            self.logger.info("Force drop enabled - will drop existing objects")
            self._force_drop_database_objects(has_tables, has_triggers, has_procedures, has_views, has_events, has_functions, has_user_types)
        
        # Statistics
        stats = {
            'start_time': time.time(),
            'files_processed': 0,
            'successful': 0,
            'warnings': 0,
            'errors': 0,
            'tables_imported': 0,
            'triggers_imported': 0,
            'procedures_imported': 0,
            'views_imported': 0,
            'events_imported': 0,
            'functions_imported': 0,
            'user_types_imported': 0,
            'data_rows_imported': 0
        }
        
        # Disable foreign key checks if configured
        if self.config.disable_foreign_keys:
            try:
                self.logger.info("Disabling foreign key checks")
                self.db.execute("SET FOREIGN_KEY_CHECKS = 0")
            except Exception as e:
                self.logger.warning(f"Could not disable foreign key checks: {str(e)}")
        
        # Import in specific order: user types, tables, functions, procedures, views, triggers, events
        try:
            # 1. User-defined types first (if any)
            if has_user_types and not self.config.exclude_user_types:
                self.logger.info("Importing user-defined types")
                for file in user_types_files:
                    result = self._import_file(file)
                    stats['files_processed'] += 1
                    if result['success']:
                        stats['successful'] += 1
                        stats['user_types_imported'] += 1
                    elif result['warning']:
                        stats['warnings'] += 1
                    else:
                        stats['errors'] += 1
            
            # 2. Table schemas
            if has_tables and not self.config.exclude_tables:
                self.logger.info("Importing table schemas")
                for file in table_schema_files:
                    result = self._import_file(file)
                    stats['files_processed'] += 1
                    if result['success']:
                        stats['successful'] += 1
                        stats['tables_imported'] += 1
                    elif result['warning']:
                        stats['warnings'] += 1
                    else:
                        stats['errors'] += 1
            
            # 3. Table data
            if has_tables and not self.config.exclude_data:
                self.logger.info("Importing table data")
                for file in table_data_files:
                    result = self._import_file(file)
                    stats['files_processed'] += 1
                    if result['success']:
                        stats['successful'] += 1
                        stats['data_rows_imported'] += result.get('rows_affected', 0)
                    elif result['warning']:
                        stats['warnings'] += 1
                    else:
                        stats['errors'] += 1
            
            # 4. Functions
            if has_functions and not self.config.exclude_functions:
                self.logger.info("Importing functions")
                for file in functions_files:
                    result = self._import_file(file)
                    stats['files_processed'] += 1
                    if result['success']:
                        stats['successful'] += 1
                        stats['functions_imported'] += 1
                    elif result['warning']:
                        stats['warnings'] += 1
                    else:
                        stats['errors'] += 1
            
            # 5. Procedures
            if has_procedures and not self.config.exclude_procedures:
                self.logger.info("Importing procedures")
                for file in procedures_files:
                    result = self._import_file(file)
                    stats['files_processed'] += 1
                    if result['success']:
                        stats['successful'] += 1
                        stats['procedures_imported'] += 1
                    elif result['warning']:
                        stats['warnings'] += 1
                    else:
                        stats['errors'] += 1
            
            # 6. Views
            if has_views and not self.config.exclude_views:
                self.logger.info("Importing views")
                for file in views_files:
                    result = self._import_file(file)
                    stats['files_processed'] += 1
                    if result['success']:
                        stats['successful'] += 1
                        stats['views_imported'] += 1
                    elif result['warning']:
                        stats['warnings'] += 1
                    else:
                        stats['errors'] += 1
            
            # 7. Triggers
            if has_triggers and not self.config.exclude_triggers:
                self.logger.info("Importing triggers")
                for file in triggers_files:
                    result = self._import_file(file)
                    stats['files_processed'] += 1
                    if result['success']:
                        stats['successful'] += 1
                        stats['triggers_imported'] += 1
                    elif result['warning']:
                        stats['warnings'] += 1
                    else:
                        stats['errors'] += 1
            
            # 8. Events
            if has_events and not self.config.exclude_events:
                self.logger.info("Importing events")
                for file in events_files:
                    result = self._import_file(file)
                    stats['files_processed'] += 1
                    if result['success']:
                        stats['successful'] += 1
                        stats['events_imported'] += 1
                    elif result['warning']:
                        stats['warnings'] += 1
                    else:
                        stats['errors'] += 1
        
        finally:
            # Re-enable foreign key checks if they were disabled
            if self.config.disable_foreign_keys:
                try:
                    self.logger.info("Re-enabling foreign key checks")
                    self.db.execute("SET FOREIGN_KEY_CHECKS = 1")
                except Exception as e:
                    self.logger.warning(f"Could not re-enable foreign key checks: {str(e)}")
        
        # Calculate execution time
        stats['end_time'] = time.time()
        stats['execution_time'] = stats['end_time'] - stats['start_time']
        
        # Log import summary
        self.logger.info(f"Import completed in {stats['execution_time']:.2f} seconds. "
                         f"Success: {stats['successful']}, Warnings: {stats['warnings']}, Errors: {stats['errors']}")
        
        # Print detailed summary
        print("\nImport Summary:")
        print(f"Total time: {stats['execution_time']:.2f} seconds")
        print(f"Files processed: {stats['files_processed']}")
        print(f"Successful: {stats['successful']}")
        print(f"Warnings: {stats['warnings']}")
        print(f"Errors: {stats['errors']}")
        print("\nImported Objects:")
        objects_imported = []
        if stats['tables_imported'] > 0:
            objects_imported.append(f"  - Tables: {stats['tables_imported']}")
        if stats['triggers_imported'] > 0:
            objects_imported.append(f"  - Triggers: {stats['triggers_imported']}")
        if stats['procedures_imported'] > 0:
            objects_imported.append(f"  - Procedures: {stats['procedures_imported']}")
        if stats['views_imported'] > 0:
            objects_imported.append(f"  - Views: {stats['views_imported']}")
        if stats['events_imported'] > 0:
            objects_imported.append(f"  - Events: {stats['events_imported']}")
        if stats['functions_imported'] > 0:
            objects_imported.append(f"  - Functions: {stats['functions_imported']}")
        if stats['user_types_imported'] > 0:
            objects_imported.append(f"  - User Types: {stats['user_types_imported']}")
        if stats['data_rows_imported'] > 0:
            objects_imported.append(f"  - Data Rows: {stats['data_rows_imported']}")
            
        if not objects_imported:
            objects_imported.append("  - Other: 0")
            
        print("\n".join(objects_imported))
        
        # Return results
        results = []
        for file_path in self.import_files:
            # Create an ImportResult object for each file
            file_name = os.path.basename(file_path)
            table_name = file_name.replace('_schema.sql', '').replace('_data.sql', '')
            
            # Use local ImportResult class rather than the imported one
            result = ImportResult(
                file_name=file_name,
                file_path=file_path,
                status="success" if stats['errors'] == 0 else "error" if not self.config.continue_on_error else "warning"
            )
            results.append(result)
            
        return results

    def _calculate_file_checksum(self, file_path: str) -> Optional[str]:
        """Calculate MD5 checksum for a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            MD5 checksum as string, or None if file could not be read
        """
        try:
            md5_hash = hashlib.md5()
            with open(file_path, "rb") as f:
                # Read and update hash in chunks to handle large files
                for byte_block in iter(lambda: f.read(4096), b""):
                    md5_hash.update(byte_block)
            return md5_hash.hexdigest()
        except Exception as e:
            logger.warning(f"Failed to calculate checksum for {file_path}: {str(e)}")
            return None
            
    def _format_time(self, seconds: float) -> str:
        """Format time in seconds to a human-readable string."""
        if seconds < 60:
            return f"{seconds:.1f} seconds"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f} minutes"
        else:
            hours = seconds / 3600
            return f"{hours:.1f} hours"
    
    def _get_mapped_table_name(self, table_name: str) -> str:
        """Get mapped table name if a mapping exists.
        
        Args:
            table_name: Original table name
            
        Returns:
            Mapped table name or original name if no mapping exists
        """
        if not hasattr(self, 'table_mapping') or not self.table_mapping:
            return table_name
            
        return self.table_mapping.get(table_name, table_name)
    
    def _count_statements_in_files(self, files: List[str]) -> int:
        """Count the total number of SQL statements in all files.
        
        Args:
            files: List of SQL files
            
        Returns:
            Total number of SQL statements
        """
        total = 0
        for file_path in files:
            try:
                with open(Path(file_path), 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                    statements = sqlparse.split(sql_content)
                    total += len([s for s in statements if s.strip()])
            except Exception as e:
                logger.warning(f"Error counting statements in {file_path}: {str(e)}")
        return total
            
    def _read_file_with_multiple_encodings(self, file_path, max_size=None):
        """Read file content trying multiple encodings.
        
        Args:
            file_path: Path to the file
            max_size: Maximum size to read (in KB)
            
        Returns:
            File content as a string
        """
        # Encodings to try in order
        encodings = ['utf-8', 'utf-16', 'latin-1', 'iso-8859-1', 'cp1252']
        content = None
        
        for encoding in encodings:
            try:
                if max_size:
                    # Read only the first max_size KB
                    with open(file_path, 'r', encoding=encoding) as f:
                        content = f.read(max_size * 1024)
                else:
                    # Read the entire file
                    with open(file_path, 'r', encoding=encoding) as f:
                        content = f.read()
                
                # If we got here, the encoding worked
                self.logger.debug(f"Successfully read file {file_path} with encoding {encoding}")
                break
            except UnicodeDecodeError:
                # Try the next encoding
                self.logger.debug(f"Failed to read file {file_path} with encoding {encoding}")
                continue
            except Exception as e:
                # Other error occurred
                self.logger.error(f"Error reading file {file_path}: {str(e)}")
                return None
        
        if content is None:
            self.logger.warning(f"Could not read file {file_path} with any encoding")
            
        return content

    def _extract_database_name_from_file(self, file_path: Path) -> Optional[str]:
        """Extract database name from a SQL file.
        
        Attempts to determine the database name using several methods:
        1. From the filename (table_schema.sql or table_data.sql)
        2. From comments in the file header (-- Database: dbname)
        3. From CREATE TABLE or INSERT INTO statements
        
        Args:
            file_path: Path to SQL file
            
        Returns:
            Database name or None if not found
        """
        # First try from filename
        file_name = file_path.stem
        if file_name.endswith('_schema') or file_name.endswith('_data'):
            database_name = file_name.replace('_schema', '').replace('_data', '')
            # Check if this seems to be a table name rather than a database name
            # For example 'customers_schema.sql' should extract 'customers' as table name,
            # not as database name
            if '_' not in file_path.parent.name and file_path.parent.name not in ['', '.', 'exports']:
                return file_path.parent.name
            # Check if file is in a directory with the database name
            if file_path.parent.name and file_path.parent.name not in ['', '.', 'exports']:
                return file_path.parent.name
        
        # Read the file to check for database name in content
        try:
            # Use the multiple encodings reader method
            content = self._read_file_with_multiple_encodings(str(file_path))
            
            if not content:
                logger.warning(f"Could not read file {file_path} with any encoding")
                return None
            
            # Extract the first 20 lines for header examination
            header_lines = content.split('\n')[:20]
            header = '\n'.join(header_lines)
            
            # Look for database name in comment
            db_comment_match = re.search(r'--\s*Database:\s*(\w+)', header, re.IGNORECASE)
            if db_comment_match:
                return db_comment_match.group(1)
            
            # Look for database name in CREATE TABLE statement
            create_match = re.search(r'CREATE\s+TABLE\s+(?:`([^`\s\.]+)`?\.)?', content, re.IGNORECASE)
            if create_match and create_match.group(1):
                return create_match.group(1)
            
            # Look for database name in INSERT INTO statement
            insert_match = re.search(r'INSERT\s+INTO\s+(?:`?([^`\s\.]+)`?\.)?', content, re.IGNORECASE)
            if insert_match and insert_match.group(1):
                return insert_match.group(1)
            
            # If file is in a subfolder that looks like a database name, use that
            if file_path.parent.name and file_path.parent.name not in ['', '.', 'exports']:
                return file_path.parent.name
            
            # Check if there's a CREATE DATABASE statement
            create_db_match = re.search(r'CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?([^`\s]+)`?', 
                                      content, re.IGNORECASE)
            if create_db_match:
                return create_db_match.group(1)
        
        except Exception as e:
            logger.warning(f"Error reading file {file_path} to extract database name: {str(e)}")
        
        return None

    def _validate_imported_table(self, table_name: str, expected_rows: int, imported_rows: int = 0) -> dict:
        """Validate imported table data.
        
        Args:
            table_name: Name of the table
            expected_rows: Expected number of rows
            imported_rows: Number of rows imported according to the operation
            
        Returns:
            Dictionary with validation status and message
        """
        result = {
            "status": "success",
            "message": ""
        }
        
        try:
            # Check if table exists
            if not self.db.table_exists(table_name):
                result["status"] = "warning"
                result["message"] = f"Table {table_name} does not exist for validation"
                return result
                
            # Count rows in the table
            count_query = f"SELECT COUNT(*) FROM `{table_name}`"
            count_result = self.db.execute_query(count_query)
            actual_rows = count_result[0][0] if count_result else 0
            
            # Validate row count
            if expected_rows > 0 and expected_rows != actual_rows:
                result["status"] = "warning"
                result["message"] = f"Row count mismatch: expected {expected_rows}, got {actual_rows}"
                self.logger.warning(f"Validation warning for {table_name}: {result['message']}")
            elif imported_rows > 0 and imported_rows != actual_rows:
                result["status"] = "warning"
                result["message"] = f"Row count mismatch: imported {imported_rows}, actual count {actual_rows}"
                self.logger.warning(f"Validation warning for {table_name}: {result['message']}")
            else:
                if actual_rows == 0:
                    result["status"] = "warning"
                    result["message"] = f"Table {table_name} has zero rows after import"
                    self.logger.warning(f"Validation warning for {table_name}: {result['message']}")
                else:
                    result["message"] = f"Table {table_name} validated: {actual_rows} rows"
                    self.logger.info(result["message"])
                
        except Exception as e:
            result["status"] = "warning"
            result["message"] = f"Error validating table {table_name}: {str(e)}"
            self.logger.error(result["message"])
        
        return result

    def _extract_expected_row_count(self, file_path: str) -> int:
        """Extract expected row count from file content or filename.
        
        Args:
            file_path: Path to the SQL file
            
        Returns:
            Expected row count or 0 if not found
        """
        try:
            # First try to get from filename, e.g. tablename_1000_data.sql
            file_name = os.path.basename(file_path)
            file_parts = os.path.splitext(file_name)[0].split('_')
            
            # Look for numeric part in filename
            for part in file_parts:
                if part.isdigit():
                    expected_rows = int(part)
                    self.logger.debug(f"Found expected row count from filename: {expected_rows}")
                    return expected_rows
            
            # If not found in filename, try to extract from file content
            content = self._read_file_with_multiple_encodings(file_path, max_size=10)  # Read just the beginning
            if not content:
                return 0
                
            # Look for a comment with row count info
            # e.g. -- Rows: 1000
            row_match = re.search(r'--\s*Rows:\s*(\d+)', content, re.IGNORECASE)
            if row_match:
                expected_rows = int(row_match.group(1))
                self.logger.debug(f"Found expected row count from file comment: {expected_rows}")
                return expected_rows
                
            # Try to count INSERT statements
            if content.upper().count('INSERT INTO') > 0:
                # Read full file for accurate count
                full_content = self._read_file_with_multiple_encodings(file_path)
                if full_content:
                    # Count VALUES clauses
                    values_count = full_content.upper().count('VALUES')
                    if values_count > 0:
                        self.logger.debug(f"Estimated {values_count} rows from INSERT statements")
                        return values_count
                        
            return 0
        except Exception as e:
            self.logger.warning(f"Error extracting expected row count: {str(e)}")
            return 0

    def _process_sql_file(self, file_path: str) -> tuple:
        """Process a SQL file by executing each statement.
        
        Args:
            file_path: Path to the SQL file
            
        Returns:
            Tuple of (statements_count, executed_count, errors)
        """
        self.logger.debug(f"Processing SQL file: {file_path}")
        
        statements_count = 0
        executed_count = 0
        errors = []
        
        try:
            # Read file content
            content = self._read_file_with_multiple_encodings(file_path)
            if not content:
                self.logger.warning(f"Could not read file or file is empty: {file_path}")
                return 0, 0, ["File is empty or could not be read"]
            
            # Split content into statements
            statements = sqlparse.split(content)
            statements = [stmt.strip() for stmt in statements if stmt.strip()]
            statements_count = len(statements)
            
            if statements_count == 0:
                self.logger.warning(f"No SQL statements found in file: {file_path}")
                return 0, 0, []
            
            # Execute each statement
            for stmt in statements:
                try:
                    if 'DELIMITER' in stmt.upper():
                        # Skip DELIMITER statements, they are handled in _process_delimiter_sql_file
                        continue
                        
                    # Execute the statement
                    self.db.execute(stmt)
                    executed_count += 1
                except Exception as e:
                    error_msg = f"Error executing statement: {str(e)}\nStatement: {stmt[:200]}..."
                    self.logger.error(error_msg)
                    errors.append(error_msg)
                    
                    # Stop on error unless configured to continue
                    if not hasattr(self.config, 'continue_on_error') or not self.config.continue_on_error:
                        return statements_count, executed_count, errors
            
            return statements_count, executed_count, errors
            
        except Exception as e:
            error_msg = f"Error processing file {file_path}: {str(e)}"
            self.logger.error(error_msg)
            return statements_count, executed_count, [error_msg]

    def _handle_import_mode(self, table_name: str, import_mode: str) -> None:
        """Handle import mode for existing tables.
        
        Args:
            table_name: Name of the table
            import_mode: Import mode (SKIP, OVERWRITE, MERGE, CANCEL)
        """
        import_mode = import_mode.upper()
        
        # Check if table exists
        if not self.db.table_exists(table_name):
            return
        
        self.logger.info(f"Table {table_name} already exists. Import mode: {import_mode}")
        
        if import_mode == "CANCEL":
            raise Exception(f"Table {table_name} already exists and import mode is CANCEL")
        elif import_mode == "SKIP":
            self.logger.info(f"Skipping data import for table {table_name} (mode: SKIP)")
            raise Exception(f"Table {table_name} already exists and import mode is SKIP")
        elif import_mode == "OVERWRITE":
            self.logger.info(f"Truncating table {table_name} before import (mode: OVERWRITE)")
            self.db.execute(f"TRUNCATE TABLE `{table_name}`")
        # MERGE mode doesn't need any special handling - it will just add data

    def _import_direct_execute(self, file_content, file_path):
        """Directly execute a SQL file with special objects using the raw cursor
        
        This method bypasses normal statement splitting for special object files
        like stored procedures, functions, triggers, and views
        
        Args:
            file_content: Content of the SQL file
            file_path: Path to the file being imported
            
        Returns:
            Dictionary with import results
        """
        result = {
            'success': True,
            'statements_total': 1,
            'statements_executed': 0,
            'errors': [],
            'warnings': []
        }
        
        try:
            # First, extract and execute DROP statements separately
            drop_pattern = r"DROP\s+(PROCEDURE|FUNCTION|TRIGGER|EVENT|VIEW)\s+IF\s+EXISTS\s+`?(\w+)`?[;\s]*"
            for match in re.finditer(drop_pattern, file_content, re.IGNORECASE):
                object_type = match.group(1)
                object_name = match.group(2)
                
                drop_stmt = f"DROP {object_type} IF EXISTS `{object_name}`"
                try:
                    self.db.execute(drop_stmt)
                    self.logger.debug(f"Dropped existing {object_type.lower()}: {object_name}")
                except Exception as e:
                    error_msg = str(e)
                    self.logger.warning(f"Error dropping {object_type.lower()} {object_name}: {error_msg}")
                    result['warnings'].append(f"Error dropping {object_type}: {error_msg}")
            
            # Extract CREATE statements from file
            # Remove comments and empty lines to make parsing easier
            content_lines = []
            for line in file_content.splitlines():
                if line.strip() and not line.strip().startswith('--'):
                    content_lines.append(line)
            
            # Rejoin without comment lines
            processed_content = '\n'.join(content_lines)
            
            # Now extract CREATE statements using regex pattern matching
            create_patterns = {
                'PROCEDURE': r'CREATE\s+(?:DEFINER\s*=\s*[^\s]+\s+)?PROCEDURE\s+`?([^`\s(]+)`?(?:\s*\([^)]*\))?\s*(?:COMMENT\s+\'[^\']*\')?\s*(?:LANGUAGE\s+SQL\s+|READS\s+SQL\s+DATA\s+|MODIFIES\s+SQL\s+DATA\s+|CONTAINS\s+SQL\s+|NOT\s+DETERMINISTIC\s+|DETERMINISTIC\s+)*(?:BEGIN|RETURN|CALL|SET|IF|WHILE|LOOP|SELECT|DECLARE|INSERT|UPDATE|DELETE).*?END',
                'FUNCTION': r'CREATE\s+(?:DEFINER\s*=\s*[^\s]+\s+)?FUNCTION\s+`?([^`\s(]+)`?(?:\s*\([^)]*\))?\s*RETURNS\s+(?:.*?)(?:BEGIN|RETURN|CALL|SET|IF|WHILE|LOOP|DECLARE).*?END',
                'TRIGGER': r'CREATE\s+(?:DEFINER\s*=\s*[^\s]+\s+)?TRIGGER\s+`?([^`\s]+)`?[^;]*?(?:BEGIN|FOR EACH ROW).*?END',
                'EVENT': r'CREATE\s+(?:DEFINER\s*=\s*[^\s]+\s+)?EVENT\s+`?([^`\s]+)`?[^;]*?(?:BEGIN|DO|CALL).*?END',
                'VIEW': r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:DEFINER\s*=\s*[^\s]+\s+)?(?:SQL\s+SECURITY\s+[^\s]+\s+)?VIEW\s+`?([^`\s]+)`?[^;]*;'
            }
            
            filename = os.path.basename(file_path).lower()
            object_type = None
            
            if 'procedures.sql' in filename:
                object_type = 'PROCEDURE'
            elif 'functions.sql' in filename:
                object_type = 'FUNCTION'
            elif 'triggers.sql' in filename:
                object_type = 'TRIGGER'
            elif 'events.sql' in filename:
                object_type = 'EVENT'
            elif 'views.sql' in filename:
                object_type = 'VIEW'
            
            if object_type:
                # Use the dedicated procedure executor for better handling
                if object_type == 'PROCEDURE':
                    return self._execute_create_procedure(file_content, result)
                
                pattern = create_patterns[object_type]
                
                # Try to find all matching objects
                for match in re.finditer(pattern, processed_content, re.IGNORECASE | re.DOTALL):
                    object_name = match.group(1)
                    self.logger.info(f"Creating {object_type.lower()}: {object_name}")
                    
                    # Extract the full SQL statement
                    full_stmt = match.group(0)
                    
                    # Clean up the statement
                    full_stmt = full_stmt.strip()
                    if not full_stmt.endswith(';'):
                        full_stmt += ';'
                    
                    self.logger.debug(f"Full statement to execute:\n{full_stmt}")
                    
                    try:
                        # Use a raw cursor for direct execution
                        with self.db.connection.cursor() as cursor:
                            self.logger.debug("About to execute procedure statement")
                            # Try to extract the procedure name from the query if it's not already known
                            if not object_name:
                                name_match = re.search(r'CREATE\s+(?:DEFINER\s*=\s*[^\s]+\s+)?(?:PROCEDURE|FUNCTION|TRIGGER|EVENT|VIEW)\s+`?([^`\s(]+)', full_stmt, re.IGNORECASE)
                                if name_match:
                                    object_name = name_match.group(1)
                                else:
                                    object_name = "unknown"
                            cursor.execute(full_stmt)
                            self.logger.debug("Procedure statement executed successfully")
                        
                        self.logger.info(f"Successfully created {object_type.lower()} {object_name}")
                        result['statements_executed'] += 1
                    except Exception as e:
                        error_msg = str(e)
                        self.logger.error(f"Error creating {object_type.lower()} {object_name}: {error_msg}")
                        
                        # Check if this is an undeclared variable error, which is common in procedures
                        if "undeclared variable" in error_msg.lower():
                            try:
                                # Try to extract the variable name
                                var_match = re.search(r"Undeclared variable: (\w+)", error_msg, re.IGNORECASE)
                                if var_match:
                                    var_name = var_match.group(1)
                                    self.logger.info(f"Attempting to fix undeclared variable: {var_name}")
                                    
                                    # Add a DECLARE statement for this variable after BEGIN
                                    modified_stmt = re.sub(
                                        r'(BEGIN\s+)',
                                        f'\\1DECLARE {var_name} INT DEFAULT 0;\n',
                                        full_stmt,
                                        flags=re.IGNORECASE
                                    )
                                    
                                    self.logger.debug(f"Retrying with modified statement adding variable declaration")
                                    with self.db.connection.cursor() as cursor:
                                        cursor.execute(modified_stmt)
                                    
                                    self.logger.info(f"Successfully created {object_type.lower()} {object_name} after fixing variable")
                                    result['statements_executed'] += 1
                                    continue
                            except Exception as var_e:
                                self.logger.error(f"Failed to fix undeclared variable: {str(var_e)}")
                        
                        # If it's a multi-statement procedure, try a different approach with DELIMITER
                        if "You have an error in your SQL syntax" in error_msg or "unexpected end of statement" in error_msg.lower():
                            try:
                                self.logger.info(f"Attempting to execute with DELIMITER syntax")
                                with self.db.connection.cursor() as cursor:
                                    # Set delimiter
                                    cursor.execute("DELIMITER //")
                                    
                                    # Add END delimiter if missing
                                    if not re.search(r'END\s*;?\s*$', full_stmt, re.IGNORECASE):
                                        full_stmt = full_stmt.rstrip(';') + "\nEND //"
                                    else:
                                        full_stmt = full_stmt.rstrip(';') + " //"
                                    
                                    # Execute with delimiter
                                    cursor.execute(full_stmt)
                                    
                                    # Reset delimiter
                                    cursor.execute("DELIMITER ;")
                                
                                self.logger.info(f"Successfully created {object_type.lower()} {object_name} using DELIMITER syntax")
                                result['statements_executed'] += 1
                                continue
                            except Exception as delim_e:
                                self.logger.error(f"Failed DELIMITER approach: {str(delim_e)}")
                        
                        # If all else fails, record the error
                        result['errors'].append(f"Error creating {object_type} {object_name}: {error_msg}")
                
                if result['statements_executed'] == 0:
                    self.logger.warning(f"No {object_type.lower()}s were created from file {os.path.basename(file_path)}")
                    result['warnings'].append(f"No {object_type.lower()}s were created")
            else:
                self.logger.warning(f"File type not recognized for special object handling: {os.path.basename(file_path)}")
                result['warnings'].append("File type not recognized for special object handling")
            
            # Set success flag based on errors and executed statements
            if result['errors']:
                # If there are errors but we still created some objects
                if result['statements_executed'] > 0 and self.config.continue_on_error:
                    result['success'] = True
                    # Convert errors to warnings instead since we want to continue
                    result['warnings'].extend(result['errors'])
                    result['errors'] = []
                else:
                    result['success'] = False
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in direct execute: {str(e)}")
            self.logger.error(f"Error importing file {file_path}: {str(e)}")
            result['success'] = False
            result['errors'].append(f"Error importing file: {str(e)}")
            return result

    def _process_delimiter_sql_file(self, file_path: str) -> tuple:
        """Process SQL file with DELIMITER statements for triggers, procedures, etc.
        
        Args:
            file_path: Path to the SQL file
            
        Returns:
            Tuple of (statements_count, executed_count, errors)
        """
        self.logger.debug(f"Processing delimiter-based SQL file: {file_path}")
        
        statements_count = 0
        executed_count = 0
        errors = []
        
        try:
            # Read file content
            content = self._read_file_with_multiple_encodings(file_path)
            if not content:
                self.logger.warning(f"Could not read file or file is empty: {file_path}")
                return 0, 0, ["File is empty or could not be read"]
            
            # Split content by DELIMITER statements
            delimiter_blocks = re.split(r'(?i)DELIMITER\s+([^\n;]+)', content)
            
            current_delimiter = ';'
            current_block = ''
            
            # Process each block
            for i, block in enumerate(delimiter_blocks):
                if i == 0:
                    # First block (before any DELIMITER statement)
                    current_block = block
                elif i % 2 == 1:
                    # This is a new delimiter definition
                    current_delimiter = block.strip()
                else:
                    # This is a block with the previously defined delimiter
                    current_block = block
                
                # Skip empty blocks
                if not current_block.strip():
                    continue
                
                # Split the block by the current delimiter
                if current_delimiter != ';':
                    # For custom delimiters, we need to be careful about how we split
                    raw_statements = current_block.split(current_delimiter)
                else:
                    # For standard delimiter, use sqlparse to handle nested semicolons better
                    raw_statements = sqlparse.split(current_block)
                
                # Process each statement in the block
                for stmt in raw_statements:
                    stmt = stmt.strip()
                    if not stmt:
                        continue
                    
                    # Replace any remaining custom delimiters with semicolons for execution
                    if current_delimiter != ';':
                        stmt = stmt.replace(current_delimiter, ';')
                    
                    statements_count += 1
                    
                    try:
                        # Execute the statement
                        self.db.execute(stmt)
                        executed_count += 1
                    except Exception as e:
                        error_msg = f"Error executing statement: {str(e)}\nStatement: {stmt[:200]}..."
                        self.logger.error(error_msg)
                        errors.append(error_msg)
                        
                        # Stop on error unless configured to continue
                        if not hasattr(self.config, 'continue_on_error') or not self.config.continue_on_error:
                            return statements_count, executed_count, errors
            
            return statements_count, executed_count, errors
            
        except Exception as e:
            error_msg = f"Error processing file {file_path}: {str(e)}"
            self.logger.error(error_msg)
            return statements_count, executed_count, [error_msg]

    def _import_file(self, file_path: str) -> dict:
        """Import a file containing SQL statements.
        
        Args:
            file_path: Path to the file to import
            
        Returns:
            Dictionary with import result information
        """
        start_time = time.time()
        result = {
            'success': False,
            'warning': False,
            'error': None,
            'file': os.path.basename(file_path),
            'rows_affected': 0
        }
        
        file_content = self._read_file_with_multiple_encodings(file_path)
        if not file_content:
            self.logger.error(f"Could not read file {file_path}")
            result['error'] = f"Could not read file {file_path}"
            return result
        
        # Get file info
        filename = os.path.basename(file_path).lower()
        
        # Special handling for stored procedures, functions, triggers, and events
        special_files = ['procedures.sql', 'functions.sql', 'triggers.sql', 'events.sql', 'views.sql']
        if filename in special_files:
            self.logger.info(f"Using direct execution for special file: {filename}")
            
            # Try direct execution first
            direct_result = self._import_direct_execute(file_content, file_path)
            
            if direct_result['success']:
                result['success'] = True
                execution_time = time.time() - start_time
                result['execution_time'] = execution_time
                self.logger.info(f"Successfully imported {filename} using direct execution in {execution_time:.2f}s")
                return result
            else:
                self.logger.warning(f"Direct execution failed for {filename}, falling back to regular import")
                # Fall through to regular import method
        
        # For regular files (not special objects), use the standard processing
        if not file_content.strip():
            self.logger.warning(f"File {file_path} is empty or contains only whitespace")
            result['warning'] = True
            result['success'] = True
            return result
            
        # Process SQL statements from the file
        try:
            # Apply delimiter handling logic
            sql_statements = self._split_sql_statements(file_content)
            if not sql_statements:
                self.logger.warning(f"No SQL statements found in file {file_path}")
                result['warning'] = True
                result['success'] = True
                return result
                
            # Use the enhanced SQL statement processor
            context = {
                'file_path': file_path,
                'file_name': filename
            }
            
            import_result = self._import_sql_statements(sql_statements, file_path, context)
            
            # Update our result based on the import result
            if not import_result['success']:
                if import_result['warnings'] and self.config.continue_on_error:
                    result['warning'] = True
                    result['success'] = True
                    self.logger.warning(f"Warnings executing SQL in {filename}: {', '.join(import_result['warnings'])}")
                else:
                    result['error'] = "; ".join(import_result['errors'])
                    self.logger.error(f"Errors executing SQL in {filename}: {result['error']}")
                    return result
            else:
                result['success'] = True
                if import_result['warnings']:
                    result['warning'] = True
                
            # Calculate execution time
            execution_time = time.time() - start_time
            result['execution_time'] = execution_time
                
            self.logger.info(f"Successfully imported {filename} ({import_result['statements_executed']} statements) in {execution_time:.2f}s")
            return result
            
        except Exception as e:
            self.logger.error(f"Error importing file {file_path}: {str(e)}")
            result['error'] = f"Error importing file: {str(e)}"
            return result

    def _split_sql_statements(self, content: str) -> List[str]:
        """Split SQL content into individual statements, handling DELIMITER directives.
        
        Args:
            content: SQL content to split
            
        Returns:
            List of SQL statements
        """
        statements = []
        current_statement = []
        delimiter = ";"
        in_string = False
        string_char = None
        in_comment = False
        escaped = False
        in_delimiter_directive = False
        
        # First check if there are DELIMITER directives in the content
        has_delimiter_directives = re.search(r'^\s*DELIMITER\s+', content, re.IGNORECASE | re.MULTILINE) is not None
        
        # If this is likely a stored procedure, function, trigger or event but has no DELIMITER directives,
        # we'll handle them differently
        is_special_object = (
            re.search(r'CREATE\s+(?:DEFINER\s*=\s*[^\s]+\s+)?PROCEDURE', content, re.IGNORECASE) or
            re.search(r'CREATE\s+(?:DEFINER\s*=\s*[^\s]+\s+)?FUNCTION', content, re.IGNORECASE) or
            re.search(r'CREATE\s+(?:DEFINER\s*=\s*[^\s]+\s+)?TRIGGER', content, re.IGNORECASE) or
            re.search(r'CREATE\s+(?:DEFINER\s*=\s*[^\s]+\s+)?EVENT', content, re.IGNORECASE)
        ) is not None
        
        if is_special_object and not has_delimiter_directives:
            self.logger.info("Found special object without DELIMITER directives, will add them automatically")
            # Just return the content as a single statement and let _import_sql_statements add delimiters
            return [content]
        
        lines = content.split('\n')
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            i += 1
            
            # Check for DELIMITER directive
            if line.upper().startswith('DELIMITER '):
                in_delimiter_directive = True
                # Complete current statement if any
                if current_statement:
                    stmt = '\n'.join(current_statement).strip()
                    if stmt:
                        statements.append(stmt)
                    current_statement = []
                
                # Set new delimiter and add the DELIMITER directive as its own statement
                delimiter = line[10:].strip()
                statements.append(line)
                self.logger.debug(f"Found DELIMITER directive, new delimiter: '{delimiter}'")
                continue
            
            # Special case: handle a line that is just the delimiter (common in exports)
            if line == delimiter and delimiter != ';':
                # This often means the end of a procedure/function/trigger/event
                # Add the line to the current statement
                current_statement.append(line)
                
                # Complete the statement
                stmt = '\n'.join(current_statement).strip()
                if stmt:
                    statements.append(stmt)
                
                # Start a new statement
                current_statement = []
                continue
            
            # Skip empty lines and comments when not in a statement
            if not current_statement and (not line or line.startswith('--') or line.startswith('#')):
                continue
            
            # Add comments to current statement
            if not current_statement and (line.startswith('--') or line.startswith('#')):
                current_statement.append(line)
                continue
            
            # Process line character by character
            j = 0
            while j < len(line):
                char = line[j]
                next_char = line[j+1] if j+1 < len(line) else None
                
                # Handle escape character
                if char == '\\' and not escaped:
                    escaped = True
                    j += 1
                    continue
                
                # Handle string literals
                if not in_comment and (char == "'" or char == '"') and not escaped:
                    if not in_string:
                        in_string = True
                        string_char = char
                    elif string_char == char:
                        in_string = False
                
                # Handle comments
                if not in_string and not in_comment and char == '-' and next_char == '-':
                    in_comment = True
                    j += 2
                    continue
                elif not in_string and not in_comment and char == '#':
                    in_comment = True
                    j += 1
                    continue
                elif not in_string and not in_comment and char == '/' and next_char == '*':
                    in_comment = True
                    j += 2
                    continue
                elif in_comment and char == '*' and next_char == '/':
                    in_comment = False
                    j += 2
                    continue
                
                # Handle end of line (reset line comment)
                if j == len(line) - 1:
                    if char != '\\':  # Not a continued line
                        in_comment = False if in_comment and not in_comment == 'block' else in_comment
                
                # Check for delimiter
                if not in_string and not in_comment and not escaped:
                    # If we found the delimiter
                    if j + len(delimiter) <= len(line) and line[j:j+len(delimiter)] == delimiter:
                        # Add current line up to delimiter
                        current_statement.append(line[:j+len(delimiter)])
                        
                        # Complete the statement
                        stmt = '\n'.join(current_statement).strip()
                        if stmt:
                            # Remove the delimiter from the end if it's not a special directive
                            if stmt.endswith(delimiter) and not stmt.upper().startswith('DELIMITER '):
                                stmt = stmt[:-len(delimiter)]
                            statements.append(stmt)
                        
                        # Start a new statement with the rest of the line
                        current_statement = [line[j+len(delimiter):]]
                        break
                
                # Reset escape flag
                escaped = False
                j += 1
            
            # If we didn't find a delimiter, add the whole line
            if j >= len(line):
                current_statement.append(line)
        
        # Add the last statement if any
        if current_statement:
            stmt = '\n'.join(current_statement).strip()
            if stmt and not stmt.upper().startswith('DELIMITER '):
                statements.append(stmt)
                
        # Special processing for DELIMITER statements to ensure they're properly paired
        processed_statements = []
        change_delimiter = None
        
        for stmt in statements:
            if stmt.upper().startswith('DELIMITER '):
                # This is a delimiter change directive
                if change_delimiter is None:
                    # Start a new delimiter change
                    change_delimiter = stmt
                    processed_statements.append(stmt)
                else:
                    # End a delimiter change by returning to standard delimiter
                    if stmt.strip().upper() == 'DELIMITER ;':
                        change_delimiter = None
                        processed_statements.append(stmt)
                    else:
                        # Unexpected delimiter change - handle it anyway
                        self.logger.warning(f"Unexpected DELIMITER directive: {stmt}")
                        processed_statements.append(stmt)
                        change_delimiter = stmt
            else:
                processed_statements.append(stmt)
        
        # If we have an unclosed delimiter change, add a closing directive
        if change_delimiter is not None:
            self.logger.warning("Found unclosed DELIMITER directive, adding closing directive")
            processed_statements.append("DELIMITER ;")
            
        self.logger.debug(f"Split SQL content into {len(processed_statements)} statements")
        return processed_statements

    def _import_sql_statements(self, statements: List[str], file_path: str = '', context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute multiple SQL statements.
        
        Args:
            statements: List of SQL statements to execute
            file_path: Source file path
            context: Additional context information
            
        Returns:
            Dict with execution results
        """
        context = context or {}
        results = {
            'statements_total': len(statements),
            'statements_executed': 0,
            'errors': [],
            'warnings': [],
            'success': True
        }
        
        current_delimiter = ";"
        in_delimiter_block = False
        
        filename = os.path.basename(file_path).lower() if file_path else ''
        
        # Check if this is a special object file
        is_procedure_file = "procedures.sql" in filename or any("CREATE PROCEDURE" in stmt.upper() for stmt in statements)
        is_function_file = "functions.sql" in filename or any("CREATE FUNCTION" in stmt.upper() for stmt in statements)
        is_trigger_file = "triggers.sql" in filename or any("CREATE TRIGGER" in stmt.upper() for stmt in statements)
        is_event_file = "events.sql" in filename or any("CREATE EVENT" in stmt.upper() for stmt in statements)
        is_view_file = "views.sql" in filename or any("CREATE VIEW" in stmt.upper() for stmt in statements)
        
        # Log what we're importing for better debugging
        self.logger.info(f"Importing from file: {filename} - Contains: " + 
                         (", ".join(filter(None, [
                             "procedures" if is_procedure_file else None,
                             "functions" if is_function_file else None,
                             "triggers" if is_trigger_file else None,
                             "events" if is_event_file else None,
                             "views" if is_view_file else None
                         ])) or "regular SQL"))
        
        # Pre-process statements with special delimiter handling for procedure-like objects
        is_special_file = is_procedure_file or is_function_file or is_trigger_file or is_event_file
        
        # Find and pre-process special objects that need delimiter handling
        if is_special_file:
            # First remove any existing DELIMITER statements - we'll handle them manually
            filtered_statements = []
            preprocess_needed = False
            
            for i, stmt in enumerate(statements):
                if stmt.upper().startswith("DELIMITER "):
                    # Skip delimiter statements - we'll add our own
                    preprocess_needed = True
                    in_delimiter_block = True
                    continue
                elif in_delimiter_block and (i+1 < len(statements) and statements[i+1].upper().startswith("DELIMITER ;")):
                    # This statement is the last one before a DELIMITER reset
                    # It should be combined with previous statements
                    in_delimiter_block = False
                    continue
                else:
                    filtered_statements.append(stmt)
            
            # Process special object definitions
            processed_statements = []
            for stmt in filtered_statements:
                stmt = stmt.strip()
                if stmt and ("CREATE PROCEDURE" in stmt.upper() or 
                           "CREATE FUNCTION" in stmt.upper() or
                           "CREATE TRIGGER" in stmt.upper() or
                           "CREATE EVENT" in stmt.upper()):
                    
                    # This is a special statement that needs DELIMITER handling
                    # Add the drop statement if it's not already there
                    object_match = re.search(r'CREATE\s+(?:DEFINER\s*=\s*[^\s]+\s+)?(?:PROCEDURE|FUNCTION|TRIGGER|EVENT)\s+`?([^`\s(]+)', stmt, re.IGNORECASE)
                    if object_match:
                        object_name = object_match.group(1)
                        object_type = re.search(r'CREATE\s+(?:DEFINER\s*=\s*[^\s]+\s+)?(PROCEDURE|FUNCTION|TRIGGER|EVENT)', stmt, re.IGNORECASE).group(1)
                        
                        # First add a drop statement
                        drop_stmt = f"DROP {object_type} IF EXISTS `{object_name}`"
                        processed_statements.append(drop_stmt)
                        
                        # For MariaDB, we need to add special delimiter handling and execute separately
                        self.logger.info(f"Adding delimiter handling for {object_type} {object_name}")
                        
                        # Execute the drop statement first
                        try:
                            self.db.execute(drop_stmt)
                            results['statements_executed'] += 1
                            self.logger.debug(f"Executed drop statement: {drop_stmt}")
                        except Exception as e:
                            self.logger.warning(f"Error dropping {object_type.lower()} {object_name}: {e}")
                        
                        # Then execute the create statement with a custom delimiter
                        try:
                            # We need to use the direct connection to execute with a custom delimiter
                            self.logger.debug(f"Executing {object_type} creation with custom delimiter")
                            
                            # Special handling for MariaDB - use the raw cursor directly
                            cursor = self.db.connection.cursor()
                            
                            # Set the delimiter
                            cursor.execute("DELIMITER //")
                            
                            # Execute the create statement
                            cursor.execute(stmt + " //")
                            
                            # Reset the delimiter
                            cursor.execute("DELIMITER ;")
                            
                            cursor.close()
                            
                            results['statements_executed'] += 1
                            self.logger.info(f"Successfully created {object_type.lower()} {object_name}")
                        except Exception as e:
                            error_msg = str(e)
                            self.logger.error(f"Error creating {object_type.lower()} {object_name}: {error_msg}")
                            results['warnings'].append(f"Error creating {object_type.lower()} {object_name}: {error_msg}")
                            
                            # Special handling for undeclared variables
                            if "undeclared variable" in error_msg.lower():
                                var_match = re.search(r'undeclared variable: ([a-zA-Z0-9_]+)', error_msg, re.IGNORECASE)
                                if var_match:
                                    var_name = var_match.group(1)
                                    self.logger.warning(f"Attempting to fix undeclared variable: {var_name}")
                                    
                                    # Try adding a declaration for the variable and re-execute
                                    begin_match = re.search(r'\bBEGIN\b', stmt, re.IGNORECASE)
                                    if begin_match:
                                        begin_pos = begin_match.start()
                                        declaration = f"\nDECLARE {var_name} INT DEFAULT 0;\n"
                                        fixed_stmt = stmt[:begin_pos] + declaration + stmt[begin_pos:]
                                        
                                        try:
                                            # Try again with the fixed statement
                                            cursor = self.db.connection.cursor()
                                            cursor.execute("DELIMITER //")
                                            cursor.execute(fixed_stmt + " //")
                                            cursor.execute("DELIMITER ;")
                                            cursor.close()
                                            
                                            results['statements_executed'] += 1
                                            self.logger.info(f"Fixed and created {object_type.lower()} {object_name}")
                                        except Exception as e2:
                                            error_msg = str(e2)
                                            self.logger.error(f"Error fixing {object_type.lower()} {object_name}: {error_msg}")
                                            results['warnings'].append(f"Error fixing {object_type.lower()} {object_name}: {error_msg}")
                    else:
                        # Add the statement as-is if we can't parse it
                        processed_statements.append(stmt)
                else:
                    # Add regular statements as-is
                    processed_statements.append(stmt)
            
            if processed_statements:
                # Update statements to the processed ones
                statements = processed_statements
                self.logger.debug(f"Preprocessed statements for special objects, now have {len(statements)} statements")
            
            # Skip the rest of normal execution since we've already executed special objects
            return results
        
        # Process each statement for regular SQL (non-special objects)
        i = 0
        while i < len(statements):
            stmt = statements[i].strip()
            i += 1
            
            # Skip empty statements
            if not stmt:
                continue
                
            try:
                # Log the statement being executed (first 100 chars to avoid huge logs)
                self.logger.debug(f"Executing statement: {stmt[:100]}..." if len(stmt) > 100 else f"Executing statement: {stmt}")
                
                # Execute the SQL statement
                self.db.execute(stmt)
                results['statements_executed'] += 1
                self.logger.debug(f"Executed statement {results['statements_executed']}/{results['statements_total']}")
                
            except Exception as e:
                error_msg = str(e)
                
                # Check if it's a duplicate object error and mode is SKIP
                if self.config.mode == ImportMode.SKIP and (
                    "already exists" in error_msg.lower() or
                    "duplicate" in error_msg.lower()
                ):
                    if self.config.continue_on_error:
                        results['warnings'].append(f"Skipped: {error_msg}")
                        self.logger.warning(f"Skipped existing object: {error_msg}")
                        continue
                    else:
                        results['errors'].append(f"Object already exists: {error_msg}")
                        results['success'] = False
                        break
                
                # If in CONTINUE_ON_ERROR mode, log the error but don't stop
                if self.config.continue_on_error:
                    results['warnings'].append(error_msg)
                    self.logger.warning(f"Error executing SQL: {error_msg}")
                else:
                    results['errors'].append(error_msg)
                    results['success'] = False
                    self.logger.error(f"Error executing SQL: {error_msg}")
                    break
            
        return results

    def _force_drop_database_objects(self, has_tables: bool, has_triggers: bool, has_procedures: bool, has_views: bool, has_events: bool, has_functions: bool, has_user_types: bool) -> None:
        """Force drop specific database objects being imported.
        
        Only drops objects that are being imported, not all objects in the database.
        This protects objects in the target environment that aren't part of the import.
        """
        self.logger.info("Force dropping database objects that match import files")
        
        # Extract object names from the import files
        tables_to_drop = []
        triggers_to_drop = []
        procedures_to_drop = []
        views_to_drop = []
        events_to_drop = []
        functions_to_drop = []
        user_types_to_drop = []
        
        # First pass: identify objects to be imported
        for file in self.import_files:
            filename = os.path.basename(file).lower()
            
            # Extract table names from schema files
            if filename.endswith('_schema.sql'):
                table_name = filename.replace('_schema.sql', '')
                tables_to_drop.append(table_name)
                
            # For other object types, we need to extract names from file content
            file_content = self._read_file_with_multiple_encodings(file)
            if not file_content:
                continue
                
            # Check for each object type
            if filename == 'triggers.sql':
                # Extract trigger names
                trigger_matches = re.findall(r'CREATE\s+(?:DEFINER\s*=\s*[^\s]+\s+)?TRIGGER\s+[`"]?([^\s`"]+)[`"]?', 
                                              file_content, re.IGNORECASE)
                triggers_to_drop.extend(trigger_matches)
                
            elif filename == 'procedures.sql':
                # Extract procedure names
                procedure_matches = re.findall(r'CREATE\s+(?:DEFINER\s*=\s*[^\s]+\s+)?PROCEDURE\s+[`"]?([^\s`"(]+)[`"]?', 
                                                file_content, re.IGNORECASE)
                procedures_to_drop.extend(procedure_matches)
                
            elif filename == 'views.sql':
                # Extract view names
                view_matches = re.findall(r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:DEFINER\s*=\s*[^\s]+\s+)?(?:SQL\s+SECURITY\s+[^\s]+\s+)?VIEW\s+[`"]?([^\s`"]+)[`"]?', 
                                           file_content, re.IGNORECASE)
                views_to_drop.extend(view_matches)
                
            elif filename == 'events.sql':
                # Extract event names
                event_matches = re.findall(r'CREATE\s+(?:DEFINER\s*=\s*[^\s]+\s+)?EVENT\s+[`"]?([^\s`"]+)[`"]?', 
                                            file_content, re.IGNORECASE)
                events_to_drop.extend(event_matches)
                
            elif filename == 'functions.sql':
                # Extract function names
                function_matches = re.findall(r'CREATE\s+(?:DEFINER\s*=\s*[^\s]+\s+)?FUNCTION\s+[`"]?([^\s`"(]+)[`"]?', 
                                               file_content, re.IGNORECASE)
                functions_to_drop.extend(function_matches)
                
            elif filename == 'user_types.sql':
                # Extract user-defined type names
                type_matches = re.findall(r'CREATE\s+TYPE\s+[`"]?([^\s`"]+)[`"]?', 
                                           file_content, re.IGNORECASE)
                user_types_to_drop.extend(type_matches)
        
        # Remove duplicates
        tables_to_drop = list(set(tables_to_drop))
        triggers_to_drop = list(set(triggers_to_drop))
        procedures_to_drop = list(set(procedures_to_drop))
        views_to_drop = list(set(views_to_drop))
        events_to_drop = list(set(events_to_drop))
        functions_to_drop = list(set(functions_to_drop))
        user_types_to_drop = list(set(user_types_to_drop))
        
        # Log what will be dropped
        self.logger.info(f"Objects to drop: {len(tables_to_drop)} tables, {len(triggers_to_drop)} triggers, "
                         f"{len(procedures_to_drop)} procedures, {len(views_to_drop)} views, "
                         f"{len(events_to_drop)} events, {len(functions_to_drop)} functions, "
                         f"{len(user_types_to_drop)} user types")
        
        # Drop objects in reverse dependency order
        try:
            # Disable foreign key checks temporarily
            self.db.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            # Drop triggers
            if has_triggers and triggers_to_drop:
                self.logger.info(f"Dropping {len(triggers_to_drop)} triggers: {', '.join(triggers_to_drop)}")
                for trigger in triggers_to_drop:
                    try:
                        self.db.execute(f"DROP TRIGGER IF EXISTS `{trigger}`")
                    except Exception as e:
                        self.logger.warning(f"Error dropping trigger {trigger}: {str(e)}")
            
            # Drop views
            if has_views and views_to_drop:
                self.logger.info(f"Dropping {len(views_to_drop)} views: {', '.join(views_to_drop)}")
                for view in views_to_drop:
                    try:
                        self.db.execute(f"DROP VIEW IF EXISTS `{view}`")
                    except Exception as e:
                        self.logger.warning(f"Error dropping view {view}: {str(e)}")
            
            # Drop events
            if has_events and events_to_drop:
                self.logger.info(f"Dropping {len(events_to_drop)} events: {', '.join(events_to_drop)}")
                for event in events_to_drop:
                    try:
                        self.db.execute(f"DROP EVENT IF EXISTS `{event}`")
                    except Exception as e:
                        self.logger.warning(f"Error dropping event {event}: {str(e)}")
            
            # Drop functions
            if has_functions and functions_to_drop:
                self.logger.info(f"Dropping {len(functions_to_drop)} functions: {', '.join(functions_to_drop)}")
                for function in functions_to_drop:
                    try:
                        self.db.execute(f"DROP FUNCTION IF EXISTS `{function}`")
                    except Exception as e:
                        self.logger.warning(f"Error dropping function {function}: {str(e)}")
            
            # Drop procedures
            if has_procedures and procedures_to_drop:
                self.logger.info(f"Dropping {len(procedures_to_drop)} procedures: {', '.join(procedures_to_drop)}")
                for procedure in procedures_to_drop:
                    try:
                        self.db.execute(f"DROP PROCEDURE IF EXISTS `{procedure}`")
                    except Exception as e:
                        self.logger.warning(f"Error dropping procedure {procedure}: {str(e)}")
            
            # Drop tables
            if has_tables and tables_to_drop:
                self.logger.info(f"Dropping {len(tables_to_drop)} tables: {', '.join(tables_to_drop)}")
                for table in tables_to_drop:
                    try:
                        self.db.execute(f"DROP TABLE IF EXISTS `{table}`")
                    except Exception as e:
                        self.logger.warning(f"Error dropping table {table}: {str(e)}")
            
            # Drop user types (last, as they might be used by other objects)
            if has_user_types and user_types_to_drop:
                self.logger.info(f"Dropping {len(user_types_to_drop)} user types: {', '.join(user_types_to_drop)}")
                for user_type in user_types_to_drop:
                    try:
                        self.db.execute(f"DROP TYPE IF EXISTS `{user_type}`")
                    except Exception as e:
                        self.logger.warning(f"Error dropping user type {user_type}: {str(e)}")
                        
            # Re-enable foreign key checks
            self.db.execute("SET FOREIGN_KEY_CHECKS = 1")
            
            self.logger.info("Successfully dropped specified database objects")
            
        except Exception as e:
            self.logger.error(f"Error during force drop operation: {str(e)}")
            # Re-enable foreign key checks in case of error
            try:
                self.db.execute("SET FOREIGN_KEY_CHECKS = 1")
            except:
                pass

    def _handle_overwrite_mode(self, table_schema_files: List[str]) -> None:
        """Handle OVERWRITE mode by dropping tables properly."""
        self.logger.info("Handling OVERWRITE mode - dropping objects before recreating them")
        
        try:
            # Get current tables in the database
            current_tables = self.db.get_table_names()
            if not current_tables:
                self.logger.info("No existing tables to drop")
                return
            
            # Extract table names from schema files
            table_names = []
            for file_path in table_schema_files:
                filename = os.path.basename(file_path)
                table_name = filename.replace('_schema.sql', '')
                table_names.append(table_name)
            
            # Find tables that exist both in the database and in our import list
            tables_to_drop = [table for table in current_tables if table in table_names]
            
            if not tables_to_drop:
                self.logger.info("No matching tables to drop")
                return
            
            self.logger.info(f"Will drop {len(tables_to_drop)} tables and their associated constraints")
            
            # Disable foreign key checks to avoid constraint errors
            self.db.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            # First drop all foreign key constraints from ALL tables in the database
            # This is necessary because constraints might reference tables we're trying to drop
            try:
                self.logger.info("Dropping all foreign key constraints that reference tables being imported")
                query = """
                    SELECT DISTINCT
                        tc.TABLE_NAME, 
                        tc.CONSTRAINT_NAME
                    FROM 
                        INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    JOIN 
                        INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                    ON 
                        tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME 
                        AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                    WHERE 
                        tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
                        AND tc.TABLE_SCHEMA = DATABASE()
                """
                
                all_constraints = self.db.execute_query(query)
                if all_constraints:
                    for constraint in all_constraints:
                        table_name = constraint['TABLE_NAME']
                        constraint_name = constraint['CONSTRAINT_NAME']
                        try:
                            self.db.execute(f"ALTER TABLE `{table_name}` DROP FOREIGN KEY `{constraint_name}`")
                            self.logger.debug(f"Dropped foreign key constraint {constraint_name} from table {table_name}")
                        except Exception as e:
                            self.logger.warning(f"Error dropping constraint {constraint_name}: {str(e)}")
            except Exception as e:
                self.logger.warning(f"Error dropping foreign key constraints: {str(e)}")
            
            # Now drop the tables
            for table in tables_to_drop:
                try:
                    # Drop all indexes except PRIMARY on the table
                    try:
                        query = f"""
                            SELECT INDEX_NAME 
                            FROM INFORMATION_SCHEMA.STATISTICS 
                            WHERE TABLE_SCHEMA = DATABASE() 
                            AND TABLE_NAME = '{table}'
                            AND INDEX_NAME != 'PRIMARY'
                        """
                        indexes = self.db.execute_query(query)
                        for index in indexes:
                            index_name = index['INDEX_NAME']
                            try:
                                self.db.execute(f"ALTER TABLE `{table}` DROP INDEX `{index_name}`")
                                self.logger.debug(f"Dropped index {index_name} from table {table}")
                            except Exception as e:
                                self.logger.warning(f"Error dropping index {index_name}: {str(e)}")
                    except Exception as e:
                        self.logger.warning(f"Error getting or dropping indexes for table {table}: {str(e)}")
                    
                    # Now drop the table
                    self.logger.info(f"Dropping table {table}")
                    self.db.execute(f"DROP TABLE IF EXISTS `{table}`")
                except Exception as e:
                    self.logger.warning(f"Error dropping table {table}: {str(e)}")
                
            # Re-enable foreign key checks
            self.db.execute("SET FOREIGN_KEY_CHECKS = 1")
            
            self.logger.info(f"Successfully prepared database for importing {len(tables_to_drop)} tables")
            
        except Exception as e:
            self.logger.error(f"Error handling OVERWRITE mode: {str(e)}")
            # Continue with import even if dropping fails, but warn the user
            self.logger.warning("Continuing with import despite dropping errors - you may see duplicate key errors")
            # Re-enable foreign key checks in case of an error
            try:
                self.db.execute("SET FOREIGN_KEY_CHECKS = 1")
            except:
                pass

    def _execute_create_procedure(self, content, result):
        """Execute CREATE PROCEDURE statements in a more robust way
        
        This method specifically handles procedure creation by:
        1. Extracting procedure definitions
        2. Adding DROP statements
        3. Handling multi-statement procedures with DELIMITER
        4. Fixing common errors
        
        Args:
            content: SQL content containing procedure definitions
            result: Result dictionary to update
            
        Returns:
            Updated result dictionary
        """
        self.logger.info("Using specialized procedure executor")
        
        # Remove comments and empty lines
        content_lines = []
        for line in content.splitlines():
            if line.strip() and not line.strip().startswith('--'):
                content_lines.append(line)
        
        processed_content = '\n'.join(content_lines)
        
        # Extract all procedure definitions
        # This pattern is more permissive to capture many variations
        proc_pattern = r'CREATE\s+(?:DEFINER\s*=\s*[^\s]+\s+)?PROCEDURE\s+`?([^`\s(]+)`?(?:\s*\([^)]*\))?(?:[\s\S]*?END|[\s\S]*?[^;]*;)'
        procedures_found = 0
        
        for match in re.finditer(proc_pattern, processed_content, re.IGNORECASE | re.DOTALL):
            try:
                proc_name = match.group(1)
                proc_def = match.group(0).strip()
                self.logger.info(f"Found procedure: {proc_name}")
                
                # First drop the procedure if it exists
                try:
                    self.db.execute(f"DROP PROCEDURE IF EXISTS `{proc_name}`")
                    self.logger.debug(f"Dropped existing procedure: {proc_name}")
                except Exception as drop_e:
                    self.logger.warning(f"Could not drop procedure {proc_name}: {str(drop_e)}")
                
                # Add delimiter markers for multi-statement procedures
                has_multiple_statements = "BEGIN" in proc_def.upper() and "END" in proc_def.upper()
                
                if has_multiple_statements:
                    # Make sure the procedure ends with END
                    if not re.search(r'END\s*;?\s*$', proc_def):
                        proc_def = proc_def.rstrip(';') + "\nEND;"
                    
                    try:
                        # Try with DELIMITER approach
                        with self.db.connection.cursor() as cursor:
                            # Set delimiter
                            cursor.execute("DELIMITER //")
                            
                            # Replace existing delimiter if present
                            proc_to_execute = proc_def.rstrip(';') + " //"
                            
                            # Execute
                            self.logger.debug(f"Executing procedure with DELIMITER: {proc_name}")
                            cursor.execute(proc_to_execute)
                            
                            # Reset delimiter
                            cursor.execute("DELIMITER ;")
                        
                        self.logger.info(f"Successfully created procedure {proc_name}")
                        procedures_found += 1
                        continue
                    except Exception as delimiter_e:
                        self.logger.warning(f"DELIMITER approach failed: {str(delimiter_e)}")
                
                # If we get here, try direct execution
                try:
                    self.logger.debug(f"Trying direct execution for procedure {proc_name}")
                    self.db.execute(proc_def)
                    self.logger.info(f"Successfully created procedure {proc_name}")
                    procedures_found += 1
                except Exception as direct_e:
                    error_msg = str(direct_e)
                    self.logger.error(f"Error creating procedure {proc_name}: {error_msg}")
                    
                    # Try to fix common errors
                    if "undeclared variable" in error_msg.lower():
                        try:
                            # Try to extract the variable name
                            var_match = re.search(r"Undeclared variable: (\w+)", error_msg, re.IGNORECASE)
                            if var_match:
                                var_name = var_match.group(1)
                                self.logger.info(f"Attempting to fix undeclared variable: {var_name}")
                                
                                # Add a DECLARE statement for this variable after BEGIN
                                modified_def = re.sub(
                                    r'(BEGIN\s+)',
                                    f'\\1DECLARE {var_name} INT DEFAULT 0;\n',
                                    proc_def,
                                    flags=re.IGNORECASE
                                )
                                
                                with self.db.connection.cursor() as cursor:
                                    # Set delimiter
                                    cursor.execute("DELIMITER //")
                                    
                                    # Execute with added variable declaration
                                    proc_to_execute = modified_def.rstrip(';') + " //"
                                    cursor.execute(proc_to_execute)
                                    
                                    # Reset delimiter
                                    cursor.execute("DELIMITER ;")
                                
                                self.logger.info(f"Successfully created procedure {proc_name} after fixing variable")
                                procedures_found += 1
                                continue
                        except Exception as var_e:
                            self.logger.error(f"Failed to fix undeclared variable: {str(var_e)}")
                    
                    # If all fixes failed, add to errors
                    result['errors'].append(f"Error creating procedure {proc_name}: {error_msg}")
            except Exception as e:
                self.logger.error(f"Error processing procedure: {str(e)}")
        
        # Update the result
        result['statements_executed'] += procedures_found
        result['statements_total'] = max(result['statements_total'], procedures_found)
        
        if procedures_found == 0:
            self.logger.warning("No procedures were created")
            result['warnings'].append("No procedures were created")
        else:
            self.logger.info(f"Successfully created {procedures_found} procedures")
        
        return result