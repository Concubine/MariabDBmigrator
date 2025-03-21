"""Import service for importing tables from SQL files."""
import logging
import time
import os
import hashlib
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
        
        # Fix: Get database from config instead of directly accessing the attribute
        self.database = mariadb.config.get('database', '')
        
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
        
    def import_data(self) -> dict:
        """Import data from the specified files."""
        self.logger.info(f"Importing {len(self.files)} files")
        
        # Ensure database is selected
        if not self.database:
            self.logger.error("No database specified for import")
            raise ValueError("No database specified for import. Please specify a database in the configuration.")
            
        # Ensure database exists and is selected
        try:
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
        for file in self.files:
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
            if not self.config.force_drop:
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
        return {
            'success': stats['errors'] == 0,
            'stats': stats
        }

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

    def _import_file(self, file_path: str) -> dict:
        """Import a file containing SQL statements.
        
        Args:
            file_path: Path to the file to import
            
        Returns:
            Dictionary with import result information
        """
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
        
        # If this is a schema file, extract table name
        filename = os.path.basename(file_path)
        if filename.endswith('_schema.sql'):
            table_name = filename.replace('_schema.sql', '')
            self.logger.info(f"Importing schema for table {table_name}")
        
        # Check if the file is valid SQL
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
            
            total_affected = 0
            for sql in sql_statements:
                if not sql.strip():
                    continue
                
                try:
                    # Execute the SQL statement
                    affected = self.db.execute(sql)
                    total_affected += affected if affected is not None else 0
                    
                except Exception as e:
                    error_message = str(e)
                    
                    # Check if it's a duplicate object error and mode is SKIP
                    if self.config.mode == ImportMode.SKIP and (
                        "already exists" in error_message or 
                        "Duplicate" in error_message
                    ):
                        self.logger.info(f"Skipping existing object: {error_message}")
                        result['warning'] = True
                        continue
                    
                    # If in CONTINUE_ON_ERROR mode, log the error but don't stop
                    if self.config.continue_on_error:
                        self.logger.warning(f"Error executing statement: {error_message}")
                        result['warning'] = True
                        continue
                    
                    # Otherwise, fail the import
                    self.logger.error(f"Error executing statement: {error_message}")
                    result['error'] = f"Failed to execute SQL: {error_message}"
                    return result
            
            # Success!
            result['success'] = True
            result['rows_affected'] = total_affected
            return result
            
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {str(e)}")
            result['error'] = f"Error processing file: {str(e)}"
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

    def _import_table(self, table: str) -> None:
        """Import a single table."""
        try:
            logger.info(f"Importing table: {table}")
            
            # Apply table mapping if exists
            mapped_table_name = self._get_mapped_table_name(table)
            if mapped_table_name != table:
                logger.info(f"Mapping table {table} to {mapped_table_name}")
                table = mapped_table_name
            
            # Extract CREATE TABLE statement from database exports
            create_table = self._extract_create_table_statement(table)
            if create_table:
                logger.info(f"Creating table {table}")
                self.db.execute(create_table)
            
            # Import data
            data_file = os.path.join(self.config.input_dir, f"{table}_data.sql")
            if Path(data_file).exists():
                logger.info(f"Importing data for table {table}")
                rows = self.db.import_table_data(table, data_file, self.config.batch_size)
                logger.info(f"Inserted {rows} rows into table {table}")
                
        except Exception as e:
            raise ImportError(f"Failed to import table {table}: {str(e)}")
            
    def _extract_create_table_statement(self, table_name: str) -> Optional[str]:
        """Extract CREATE TABLE statement from SQL file."""
        # Read the schema file
        schema_file = os.path.join(self.config.input_dir, f"{table_name}.sql")
        if not Path(schema_file).exists():
            logger.warning(f"Schema file not found for table {table_name}")
            return None
            
        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Extract CREATE TABLE statement
            match = re.search(r'CREATE\s+TABLE.*?;', content, re.IGNORECASE | re.DOTALL)
            if match:
                return match.group(0)
                
            logger.warning(f"CREATE TABLE statement not found in {schema_file}")
            return None
            
        except Exception as e:
            logger.error(f"Error reading schema file {schema_file}: {str(e)}")
            return None

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
        for file in self.files:
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
            raise

    def _handle_overwrite_mode(self, table_schema_files: List[str]) -> None:
        """Handle OVERWRITE mode by truncating tables instead of dropping them."""
        self.logger.info("Handling OVERWRITE mode by truncating tables")
        
        # Get the current tables in the database
        try:
            current_tables = self.db.get_table_names()
            if not current_tables:
                self.logger.info("No existing tables to truncate")
                return
                
            # Extract table names from schema files
            table_names = []
            for file_path in table_schema_files:
                filename = os.path.basename(file_path)
                table_name = filename.replace('_schema.sql', '')
                table_names.append(table_name)
                
            # Find tables that exist both in the database and in our import list
            tables_to_truncate = [table for table in current_tables if table in table_names]
            
            if not tables_to_truncate:
                self.logger.info("No matching tables to truncate")
                return
                
            self.logger.info(f"Truncating {len(tables_to_truncate)} tables")
            
            # Disable foreign key checks to avoid constraint errors
            self.db.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            # Truncate each table
            for table in tables_to_truncate:
                self.logger.info(f"Truncating table {table}")
                self.db.execute(f"TRUNCATE TABLE `{table}`")
                
            # Re-enable foreign key checks
            self.db.execute("SET FOREIGN_KEY_CHECKS = 1")
            
            self.logger.info(f"Successfully truncated {len(tables_to_truncate)} tables")
            
        except Exception as e:
            self.logger.error(f"Error handling OVERWRITE mode: {str(e)}")
            # Continue with import even if truncation fails 

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
        
        lines = content.split('\n')
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            i += 1
            
            # Check for DELIMITER directive
            if line.upper().startswith('DELIMITER '):
                # Complete current statement if any
                if current_statement:
                    stmt = '\n'.join(current_statement).strip()
                    if stmt:
                        statements.append(stmt)
                    current_statement = []
                
                # Set new delimiter
                delimiter = line[10:].strip()
                continue
            
            # Skip empty lines and comments
            if not line or line.startswith('--') or line.startswith('#'):
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
                    in_comment = False
                
                # Check for delimiter
                if not in_string and not in_comment and not escaped:
                    # If we found the delimiter
                    if line[j:j+len(delimiter)] == delimiter:
                        # Add current line up to delimiter
                        current_statement.append(line[:j+len(delimiter)])
                        
                        # Complete the statement
                        stmt = '\n'.join(current_statement).strip()
                        if stmt:
                            # Remove the delimiter from the end
                            if stmt.endswith(delimiter):
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
        
        return statements 