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

class ImportService:
    """Service for importing data from SQL files."""

    def __init__(self, mariadb, ui=None, logger=None):
        """Initialize the import service.
        
        Args:
            mariadb: MariaDB instance for database operations
            ui: UI instance for displaying progress and results
            logger: Logger instance
        """
        self.mariadb = mariadb
        self.ui = ui
        self.logger = logger or get_logger()
        self.database = mariadb.database
        self.files = []
        self.config = ImportConfig()
        self.db = mariadb
        self.storage = SQLStorage()
        self.start_time = 0
        self.files_processed = 0
        self.total_statements = 0
        self.statements_processed = 0
        self.checksums = {}  # Store file checksums for validation
        
        # Initialize table name mapping if provided
        self.table_mapping = {}
        if hasattr(self.config, 'table_mapping') and isinstance(self.config.table_mapping, dict):
            self.table_mapping = self.config.table_mapping
            logger.info(f"Using table name mapping for {len(self.table_mapping)} tables")
        
    def import_data(self, file_paths, import_mode: str) -> None:
        """Import data from a list of files.

        Args:
            file_paths: A list of file paths.
            import_mode: Mode to use when importing data.
        """
        import_results = []
        successful_imports = 0
        warnings = 0
        errors = 0
        
        for idx, file_path in enumerate(file_paths):
            self.logger.debug(f"Importing data from file {file_path} {idx+1}/{len(file_paths)}")
            import_result = self._import_file(file_path, import_mode)
            import_results.append(import_result)
            
            # Display individual result using ASCII UI
            self.ui.display_import_result(import_result)
            
            # Count statistics
            if import_result.status == "success":
                successful_imports += 1
            elif import_result.status == "warning":
                warnings += 1
            elif import_result.status == "error":
                errors += 1
        
        # Display import summary
        self.ui.display_import_summary(import_results)
        
        # Log overall stats
        total_files = len(file_paths)
        self.logger.info(f"Import completed: {successful_imports}/{total_files} files imported successfully, {warnings} with warnings, {errors} with errors")

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
    
    def _get_mapped_table_name(self, original_table: str) -> str:
        """Get mapped table name if mapping exists.
        
        Args:
            original_table: Original table name
            
        Returns:
            Mapped table name or original if no mapping exists
        """
        return self.table_mapping.get(original_table, original_table)
    
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
            
    def _read_file_with_multiple_encodings(self, file_path: str, max_size: int = 10240) -> str:
        """Read file content using multiple encodings.
        
        Args:
            file_path: Path to the file
            max_size: Maximum size to read (in KB)
            
        Returns:
            File content as string or empty string if reading fails
        """
        # Common encodings to try
        encodings = [
            'utf-8', 'latin1', 'cp1252', 
            'ascii', 'utf-16', 'utf-32',
            'iso-8859-1', 'iso-8859-2', 'iso-8859-15',
            'windows-1250', 'windows-1251', 'windows-1252',
            'windows-1253', 'windows-1254', 'windows-1255',
            'windows-1256', 'windows-1257', 'windows-1258',
            'mac-roman', 'mac-cyrillic', 'mac-greek',
            'cp437', 'cp850', 'cp866', 'cp932', 'cp949', 'cp950',
            'koi8-r', 'koi8-u', 'gb2312', 'gbk', 'big5', 
            'euc-jp', 'euc-kr', 'shift-jis', 'hz'
        ]
        
        # Check if file exists
        if not os.path.isfile(file_path):
            self.logger.error(f"File not found: {file_path}")
            return ""
        
        # Calculate max bytes to read
        max_bytes = max_size * 1024
        
        # Try each encoding
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    content = f.read(max_bytes)
                    self.logger.debug(f"Successfully read file using encoding: {encoding}")
                    return content
            except UnicodeDecodeError:
                continue
        
        # If all encodings fail, try with replacement
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read(max_bytes)
                self.logger.warning(f"Reading file with 'replace' error handler - some characters may be replaced: {file_path}")
                return content
        except Exception as e:
            self.logger.error(f"Error reading file with all encodings: {str(e)}")
            return ""

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
        """Extract expected row count from SQL file.

        Args:
            file_path: Path to SQL file.

        Returns:
            Expected row count or 0 if not found.
        """
        expected_rows = 0
        try:
            content = self._read_file_with_multiple_encodings(file_path)
            if not content:
                return 0
            
            # Look for comments indicating expected row count
            row_count_patterns = [
                r'--\s*Expected\s+rows:\s*(\d+)',
                r'#\s*Expected\s+rows:\s*(\d+)',
                r'/\*\s*Expected\s+rows:\s*(\d+)\s*\*/',
                r'--\s*Rows:\s*(\d+)',
                r'#\s*Rows:\s*(\d+)',
                r'/\*\s*Rows:\s*(\d+)\s*\*/'
            ]
            
            for pattern in row_count_patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    expected_rows = int(match.group(1))
                    self.logger.debug(f"Found expected row count: {expected_rows}")
                    break
                
            # If no explicit count found, try to count INSERT values
            if expected_rows == 0:
                # For multi-row insert statements like INSERT INTO x VALUES (...), (...), (...)
                insert_matches = re.finditer(r'INSERT\s+(?:IGNORE\s+)?INTO\s+[^(]+\s+VALUES\s*\(', content, re.IGNORECASE)
                for match in insert_matches:
                    # Find the insert statement start
                    pos = match.end()
                    # Count the number of value groups (each group starts with a parenthesis)
                    paren_count = 1  # Start with the opening parenthesis we already found
                    value_groups = 1  # Start with the first group
                    in_string = False
                    string_char = None
                    escaped = False
                    
                    for i in range(pos, len(content)):
                        char = content[i]
                        
                        # Handle string literals
                        if char in ['"', "'"] and not escaped:
                            if not in_string:
                                in_string = True
                                string_char = char
                            elif char == string_char:
                                in_string = False
                        
                        # Skip characters in string literals
                        if in_string:
                            escaped = char == '\\'
                            continue
                        
                        # Count parentheses
                        if char == '(':
                            paren_count += 1
                        elif char == ')':
                            paren_count -= 1
                            if paren_count == 0:
                                # End of values, break out
                                break
                        
                        # Count value groups (pattern: ),(
                        if char == ',' and content[i+1:i+2] == '(':
                            value_groups += 1
                    
                    expected_rows += value_groups
                
        except Exception as e:
            self.logger.warning(f"Error extracting expected row count: {str(e)}")
        
        return expected_rows

    def _process_sql_file(self, file_path: str) -> tuple:
        """Process an SQL file to extract database name, table name, and SQL statements.
        
        Args:
            file_path: Path to the SQL file
            
        Returns:
            Tuple containing (database_name, create_statements, data_statement, table_name, expected_rows)
        """
        self.logger.debug(f"Processing SQL file: {file_path}")
        
        # Read the file content with multiple encoding support
        content = self._read_file_with_multiple_encodings(file_path)
        if not content:
            raise ValueError(f"Could not read file content with any supported encoding: {file_path}")
        
        # Extract expected row count
        expected_rows = self._extract_expected_row_count(file_path)
        
        # Split content into statements
        statements = []
        current_statement = []
        in_string = False
        string_char = None
        in_comment = False
        comment_type = None
        
        for line in content.split('\n'):
            line = line.strip()
            if not line:
                continue
            
            # Skip comment lines
            if line.startswith('--') or line.startswith('#'):
                continue
            
            current_statement.append(line)
            
            if line.endswith(';'):
                statements.append(' '.join(current_statement))
                current_statement = []
        
        # Add any remaining statement
        if current_statement:
            statements.append(' '.join(current_statement))
        
        # Identify database name
        db_name = None
        
        # First try to extract from USE statement
        for stmt in statements:
            if stmt.upper().startswith('USE '):
                match = re.search(r'USE\s+[`"]?([a-zA-Z0-9_]+)[`"]?', stmt, re.IGNORECASE)
                if match:
                    db_name = match.group(1)
                    self.logger.info(f"Found database name from USE statement: {db_name}")
                    break
        
        # If not found, try to extract from file path
        if not db_name:
            # Try to extract from directory structure
            path_parts = os.path.abspath(file_path).split(os.sep)
            for part in reversed(path_parts[:-1]):  # Skip the filename itself
                if part and not part.startswith('.') and part not in ['sql', 'scripts', 'database', 'dump']:
                    db_name = part
                    self.logger.info(f"Using database name from path: {db_name}")
                    break
        
        # If still not found, try to extract from CREATE/INSERT statements
        if not db_name:
            for stmt in statements:
                # Check for explicit database references in statements
                db_match = re.search(r'(?:CREATE\s+TABLE|INSERT\s+INTO)\s+[`"]?([a-zA-Z0-9_]+)[`"]?\.[`"]?([a-zA-Z0-9_]+)[`"]?', 
                                   stmt, re.IGNORECASE)
                if db_match:
                    db_name = db_match.group(1)
                    self.logger.info(f"Found database name from SQL statement: {db_name}")
                    break
        
        # If still not found, use the file name as a fallback
        if not db_name:
            base_name = os.path.basename(file_path)
            if '_' in base_name:
                # Try to use part of the filename before underscore
                db_name = base_name.split('_')[0]
                self.logger.info(f"Using database name from filename: {db_name}")
            else:
                # Remove extension
                db_name = os.path.splitext(base_name)[0]
                self.logger.info(f"Using database name from filename without extension: {db_name}")
        
        # Identify table name
        table_name = None
        
        # Try to extract table name from CREATE TABLE statement
        for stmt in statements:
            if 'CREATE TABLE' in stmt.upper():
                create_match = re.search(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?([^`\s\.]+)`?\.)?`?([^`\s(]+)`?', 
                                        stmt, re.IGNORECASE)
                if create_match:
                    if create_match.group(1):  # Database name present
                        if not db_name:
                            db_name = create_match.group(1)
                        table_name = create_match.group(2)
                    else:
                        table_name = create_match.group(2)
                    
                    self.logger.info(f"Found table name from CREATE TABLE: {table_name}")
                    break
        
        # If not found, try to extract from INSERT INTO statement
        if not table_name:
            for stmt in statements:
                if stmt.upper().startswith('INSERT'):
                    insert_match = re.search(r'INSERT\s+(?:IGNORE\s+)?INTO\s+(?:`?([^`\s\.]+)`?\.)?`?([^`\s(]+)`?', 
                                           stmt, re.IGNORECASE)
                    if insert_match:
                        if insert_match.group(1):  # Database name present
                            if not db_name:
                                db_name = insert_match.group(1)
                            table_name = insert_match.group(2)
                        else:
                            table_name = insert_match.group(2)
                        
                        self.logger.info(f"Found table name from INSERT statement: {table_name}")
                        break
        
        # If still not found, use the file name
        if not table_name:
            base_name = os.path.basename(file_path)
            # Remove common suffixes and extension
            for suffix in ['_data', '_schema', '_table', '_dump']:
                if base_name.endswith(suffix + '.sql'):
                    table_name = base_name[:-len(suffix + '.sql')]
                    break
            if not table_name:
                # Just remove extension
                table_name = os.path.splitext(base_name)[0]
            
            self.logger.info(f"Using table name from filename: {table_name}")
        
        # Separate CREATE statements from data statements
        create_statements = []
        data_statement = None
        
        for stmt in statements:
            stmt_upper = stmt.upper()
            if 'CREATE' in stmt_upper:
                create_statements.append(stmt)
            elif 'INSERT' in stmt_upper:
                if data_statement:
                    # Append to existing data statement
                    data_statement += "; " + stmt
                else:
                    data_statement = stmt
        
        return db_name, create_statements, data_statement, table_name, expected_rows

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

    def _import_file(self, file_path, import_mode: str) -> ImportResult:
        """Import data from a single file.

        Args:
            file_path: Path to file.
            import_mode: Mode to use when importing data.

        Returns:
            ImportResult: Object containing information about the import operation.
        """
        start_time = time.time()
        result = ImportResult(
            file_name=os.path.basename(file_path),
            source_path=str(file_path),
            status="error",  # Default to error, will be updated on success
            table_name="",
            rows_imported=0,
            expected_rows=0,
            error_message="",
            duration=0,
            warnings=[]
        )
        
        try:
            # Check if file exists
            if not os.path.isfile(file_path):
                result.error_message = f"File not found: {file_path}"
                return result
            
            # Process the file to extract database name and SQL content
            try:
                db_name, create_statements, data_statement, table_name, expected_rows = self._process_sql_file(file_path)
                result.table_name = table_name or os.path.basename(file_path).split('.')[0]
                result.expected_rows = expected_rows
            except Exception as e:
                error_msg = f"Error processing SQL file: {str(e)}"
                self.logger.error(error_msg)
                result.error_message = error_msg
                return result
            
            # Check if database was properly identified
            if not db_name:
                error_msg = "No database selected or created from SQL file"
                self.logger.error(error_msg)
                result.error_message = error_msg
                return result
            
            # Connect to the database
            try:
                self.db.connect()
                
                # Select or create the database
                if not self.db.database_exists(db_name):
                    self.logger.info(f"Creating database {db_name}")
                    self.db.create_database(db_name)
                
                self.db.select_database(db_name)
                
                # Execute create statements
                for statement in create_statements:
                    try:
                        self.db.execute(statement)
                        self.logger.debug(f"Executed table structure statement")
                    except Exception as e:
                        result.warnings.append(f"Error executing create statement: {str(e)}")
                        self.logger.warning(f"Error executing create statement: {str(e)}")
                
                # Check if table exists
                if not table_name or not self.db.table_exists(table_name):
                    error_msg = f"Table {table_name} not found after executing create statements"
                    self.logger.error(error_msg)
                    result.error_message = error_msg
                    return result
                
                # Handle import mode
                self._handle_import_mode(table_name, import_mode)
                
                # Execute data statements
                if data_statement:
                    try:
                        affected_rows = self.db.execute(data_statement)
                        self.logger.debug(f"Executed data statement, affected rows: {affected_rows}")
                        result.rows_imported = affected_rows
                    except Exception as e:
                        error_msg = f"Error executing data statement: {str(e)}"
                        self.logger.error(error_msg)
                        result.error_message = error_msg
                        return result
                
                # Validate imported table data
                validation_result = self._validate_imported_table(table_name, result.expected_rows, result.rows_imported)
                if validation_result.get("status") == "warning":
                    result.status = "warning"
                    result.error_message = validation_result.get("message", "")
                else:
                    # If we reach here, import was successful
                    result.status = "success"
                
            except Exception as e:
                error_msg = f"Database error: {str(e)}"
                self.logger.error(error_msg)
                result.error_message = error_msg
                return result
            finally:
                self.db.disconnect()
        except Exception as e:
            error_msg = f"Unexpected error during import: {str(e)}"
            self.logger.error(error_msg)
            result.error_message = error_msg
        
        # Calculate duration
        result.duration = time.time() - start_time
        return result

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