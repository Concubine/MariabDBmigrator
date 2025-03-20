"""Import service for MariaDB data."""
import logging
import time
import os
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

# SUGGESTION: Add support for different input formats (CSV, JSON, Parquet)
# SUGGESTION: Implement data validation with checksums during import

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
        # SUGGESTION: Add option for data transformation/filtering pipeline
        # SUGGESTION: Support target table name mapping for importing to different table structures
        self.database = database
        self.files = files
        self.config = config
        self.db = MariaDB(database)
        self.storage = SQLStorage()
        self.start_time = 0
        self.files_processed = 0
        self.total_statements = 0
        self.statements_processed = 0
        
    def import_data(self) -> List[ImportResult]:
        """Import tables into the database.
        
        Returns:
            List of import results
            
        Raises:
            ImportError: If import fails
        """
        # SUGGESTION: Add pre and post import hooks for extensibility
        # SUGGESTION: Implement dry-run mode for validation without execution
        self.start_time = time.time()
        try:
            # Connect to database
            self.db.connect()
            
            # Track tables that have been dropped during import
            # This helps ensure we create tables before inserting data
            self.dropped_tables = set()
            
            # Track failed constraints to retry later
            self.failed_constraints = []
            
            # First check if a database name was specified in the import config
            if self.config.database:
                logger.info(f"Using database name from import configuration: {self.config.database}")
                self.database.database = self.config.database
            # Then check if database is specified in database configuration
            elif not self.database.database:
                logger.info("No target database specified in configuration. Attempting to extract from SQL files...")
                
                # Initialize target database variable
                target_database = None
                
                # First check if there are any database schema files
                schema_files = [f for f in self.files if f.endswith('_schema.sql')]
                if schema_files:
                    logger.info(f"Found database schema files: {', '.join(schema_files)}")
                    for schema_file in schema_files:
                        try:
                            with open(Path(schema_file), 'r', encoding='utf-8') as f:
                                content = f.read()
                                # Extract database name from schema file
                                # SUGGESTION: Use more robust SQL parsing instead of regex
                                db_match = re.search(r'--\s*Database:\s*([^\s\r\n]+)', content, re.IGNORECASE)
                                if db_match:
                                    target_database = db_match.group(1)
                                    logger.info(f"Found database name in schema file: {target_database}")
                                    break
                        except Exception as e:
                            logger.warning(f"Error reading schema file {schema_file}: {str(e)}")
                            continue
                
                # If no schema file found, try to extract from regular SQL files
                if not target_database:
                    for file_path in self.files:
                        try:
                            # Read the first few lines of the SQL file to look for database name
                            with open(Path(file_path), 'r', encoding='utf-8') as f:
                                content = ''.join(f.readline() for _ in range(20))  # Read first 20 lines
                                
                                # Look for CREATE DATABASE or USE statements
                                # SUGGESTION: Use sqlparse library for more robust SQL parsing
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
                if not target_database:
                    # Instead of using a hardcoded default name, check for database name in configuration
                    if self.database.database:
                        target_database = self.database.database
                        logger.info(f"Using database name from configuration: {target_database}")
                    else:
                        # Use the original name if specified in file, otherwise fall back to default
                        # SUGGESTION: Allow configuring default database name
                        target_database = "imported_db" 
                        logger.info(f"No database found in SQL files or configuration. Using default name: {target_database}")
                    
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
            
            # Disable foreign key checks globally if configured
            # SUGGESTION: Add option to selectively disable foreign keys only for specific tables
            foreign_keys_disabled = False
            if self.config.disable_foreign_keys:
                logger.info("Disabling foreign key checks globally for import process")
                self.db.execute("SET FOREIGN_KEY_CHECKS=0")
                foreign_keys_disabled = True
            
            try:
                # Import each file
                results = []
                
                # Sort files to ensure schema files are processed before data files
                # SUGGESTION: Move file sorting logic to a separate method for better maintainability
                def file_sort_key(file_path):
                    """Sort key function for import files.
                    
                    Priority (lowest to highest):
                    1. Database schema files (lowest priority, process first)
                    2. Data files containing INSERT statements
                    """
                    path = Path(file_path)
                    
                    # Schema files first
                    if path.name.endswith('_schema.sql'):
                        return 0
                    # Data files last
                    elif path.name.endswith('_data.sql'):
                        return 1
                    
                    # For more accurate categorization, peek into file content
                    try:
                        with open(path, 'r', encoding='utf-8') as f:
                            # Read a sample of the file to determine type
                            sample = ''.join(f.readline() for _ in range(10))
                            
                            # Check for CREATE TABLE statements
                            if re.search(r'CREATE\s+TABLE', sample, re.IGNORECASE):
                                return 0  # Schema files first
                            
                            # Check for INSERT statements
                            if re.search(r'INSERT\s+INTO', sample, re.IGNORECASE):
                                return 1  # Data files last
                    except Exception as e:
                        logger.warning(f"Error reading file {path} for sorting: {str(e)}")
                    
                    # Default to middle priority if can't determine
                    return 0.5  # Between schema and data
                
                # Sort the files to process schema files first
                sorted_files = sorted(self.files, key=file_sort_key)
                
                # Count total statements in all files for progress tracking
                # SUGGESTION: Make this count optional with a flag for faster startup with large files
                self.total_statements = self._count_statements_in_files(sorted_files)
                logger.info(f"Found {self.total_statements} SQL statements to process in {len(sorted_files)} files")
                
                # Process each file
                file_count = len(sorted_files)
                for i, file_path in enumerate(sorted_files, 1):
                    # SUGGESTION: Add progress callback for UI integration
                    logger.info(f"Processing file {i}/{file_count}: {Path(file_path).name}")
                    
                    # Display progress estimation
                    if i > 1:
                        elapsed = time.time() - self.start_time
                        progress_factor = max(0.25, (i-1)/file_count)  # Start at 0.25 (4x) and approach 1.0
                        estimated_total = (elapsed / ((i-1) * progress_factor)) * file_count
                        remaining = estimated_total - elapsed
                        logger.info(f"Progress: {((i-1)/file_count)*100:.1f}% ({i-1}/{file_count} files). Estimated time remaining: {self._format_time(remaining)}")
                    
                    try:
                        # Import the file
                        result = self._import_file(Path(file_path))
                        results.append(result)
                        self.files_processed += 1
                    except Exception as e:
                        logger.error(f"Error processing file {file_path}: {str(e)}")
                        # Add a failed result
                        results.append(ImportResult(
                            table_name=Path(file_path).stem,
                            status="ERROR",
                            error_message=str(e)
                        ))
                        
                        # Re-raise if not configured to continue on error
                        if not self.config.continue_on_error:
                            raise ImportError(f"Import failed: {str(e)}")
                
                # Retry any failed constraints at the end
                # SUGGESTION: Implement more sophisticated constraint dependency resolution
                if self.failed_constraints:
                    logger.info(f"Attempting to apply {len(self.failed_constraints)} failed constraints")
                    successful_constraints = 0
                    
                    for constraint in self.failed_constraints:
                        try:
                            self.db.execute(constraint)
                            successful_constraints += 1
                        except Exception as e:
                            logger.warning(f"Failed to apply constraint: {str(e)}\nConstraint: {constraint}")
                    
                    logger.info(f"Successfully applied {successful_constraints}/{len(self.failed_constraints)} previously failed constraints")
                
                # Display statistics
                total_time = time.time() - self.start_time
                logger.info(f"Import completed in {self._format_time(total_time)}")
                logger.info(f"Processed {self.files_processed} files with {self.statements_processed} SQL statements")
                
                return results
                
            finally:
                # Re-enable foreign key checks if they were disabled
                if foreign_keys_disabled:
                    logger.info("Re-enabling foreign key checks")
                    try:
                        self.db.execute("SET FOREIGN_KEY_CHECKS=1")
                    except Exception as e:
                        logger.warning(f"Error re-enabling foreign key checks: {str(e)}")
        
        except Exception as e:
            raise ImportError(f"Import failed: {str(e)}")
            
        finally:
            self.db.disconnect()
    
    def _format_time(self, seconds: float) -> str:
        """Format time in seconds to a human-readable string.
        
        Args:
            seconds: Time in seconds
            
        Returns:
            Formatted time string
        """
        if seconds < 60:
            return f"{seconds:.1f} seconds"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f} minutes"
        else:
            hours = seconds / 3600
            return f"{hours:.1f} hours"
    
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
            
            # For consolidated schema/data files, extract database name
            file_name = file_path.stem
            if file_name.endswith('_schema') or file_name.endswith('_data'):
                database_name = file_name.replace('_schema', '').replace('_data', '')
                logger.info(f"Processing {file_name} for database {database_name}")
                
                # Make sure we're using the right database
                if database_name != self.database.database:
                    logger.info(f"Switching to database {database_name}")
                    # Check if database exists, create if not
                    available_dbs = self.db.get_available_databases()
                    if database_name not in available_dbs:
                        logger.info(f"Database '{database_name}' does not exist. Creating it.")
                        self.db.execute(f"CREATE DATABASE IF NOT EXISTS `{database_name}`")
                    self.db.select_database(database_name)
                    self.database.database = database_name
            else:
                # Extract table name from CREATE TABLE statement if present
                create_table_match = re.search(r'CREATE\s+TABLE\s+(?:`?([^`\s\.]+)`?\.)?`?([^`\s]+)`?', sql_content, re.IGNORECASE)
                
                if create_table_match:
                    # Group 1 is the database name (if present), group 2 is the table name
                    db_name = create_table_match.group(1)
                    table_name = create_table_match.group(2)
                else:
                    # If no CREATE TABLE statement found, check for INSERT INTO statements
                    insert_match = re.search(r'INSERT\s+INTO\s+(?:`?([^`\s\.]+)`?\.)?`?([^`\s(]+)`?', sql_content, re.IGNORECASE)
                    if insert_match:
                        # Group 1 is the database name (if present), group 2 is the table name
                        db_name = insert_match.group(1)
                        table_name = insert_match.group(2)
                    else:
                        # If no CREATE TABLE or INSERT INTO statement found, try to extract from filename
                        filename = file_path.stem
                        if '_data' in filename:
                            # Remove _data suffix to get the table name
                            table_name = filename.replace('_data', '')
                        else:
                            # Use the filename as table name
                            table_name = filename
                
                logger.info(f"Identified table name '{table_name}' for file {file_path.name}")
                
                # Handle database name if found
                if 'db_name' in locals() and db_name and db_name != self.database.database:
                    logger.info(f"SQL file contains a different database name ({db_name}) than the configured one ({self.database.database})")
                    
                    # If no database was specified in configuration or import options, use the one from the file
                    if not self.config.database and (not self.database.database or self.database.database == "imported_db"):
                        logger.info(f"Using original database name from SQL file: {db_name}")
                        self.database.database = db_name
                        # Make sure the database exists
                        self.db.execute(f"CREATE DATABASE IF NOT EXISTS `{self.database.database}`")
                        # Select the database
                        self.db.select_database(self.database.database)
                    # Otherwise replace database name in SQL content with the configured one
                    else:
                        # Replace database name in SQL content if needed
                        sql_content = re.sub(
                            r'(`?' + re.escape(db_name) + r'`?\.)',
                            f'`{self.database.database}`.',
                            sql_content
                        )
            
            # Check for any tables that might already exist in the target database
            if file_path.stem.endswith('_schema'):
                create_tables = re.finditer(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?([^`\s\.]+)`?\.)?`?([^`\s]+)`?', sql_content, re.IGNORECASE)
                for match in create_tables:
                    table_name = match.group(2)
                    if self.db.table_exists(table_name):
                        if self.config.mode == ImportMode.OVERWRITE.value:
                            logger.info(f"Dropping existing table {table_name} (mode: overwrite)")
                            self.db.drop_table(table_name)
                            self.dropped_tables.add(table_name)
                        elif self.config.mode == ImportMode.SKIP.value:
                            logger.info(f"Table {table_name} already exists and mode is 'skip', will skip this table")
                        elif self.config.mode == ImportMode.CANCEL.value:
                            logger.info(f"Table {table_name} already exists and mode is 'cancel', import will be aborted")
                            return ImportResult(
                                table_name=table_name,
                                status="error",
                                error_message=f"Table {table_name} already exists"
                            )
            
            # Split SQL file into statements
            statements = sqlparse.split(sql_content)
            
            # Execute each statement
            schema_statements = []
            create_table_statements = []  # Separated to ensure table creation comes first
            data_statements = []
            constraint_statements = []
            
            for stmt in statements:
                stmt = stmt.strip()
                if not stmt:
                    continue
                
                stmt_upper = stmt.upper()
                
                # Categorize statements
                if stmt_upper.startswith('INSERT') and self.config.import_data:
                    data_statements.append(stmt)
                elif 'CREATE TABLE' in stmt_upper and self.config.import_schema:
                    create_table_statements.append(stmt)
                elif stmt_upper.startswith(('CREATE INDEX', 'ALTER TABLE ADD CONSTRAINT', 'ALTER TABLE ADD FOREIGN KEY')):
                    if not self.config.exclude_constraints:
                        constraint_statements.append(stmt)
                    else:
                        logger.info(f"Skipping constraint (exclude_constraints=true)")
                elif stmt_upper.startswith(('CREATE', 'ALTER', 'DROP')) and self.config.import_schema:
                    schema_statements.append(stmt)
            
            # Execute CREATE TABLE statements first to ensure table exists
            if create_table_statements:
                logger.info(f"Executing {len(create_table_statements)} CREATE TABLE statements")
                for i, stmt in enumerate(create_table_statements):
                    try:
                        logger.info(f"Executing CREATE TABLE statement {i+1}/{len(create_table_statements)}")
                        self.db.execute(stmt)
                        
                        # Track progress for time estimation
                        self.statements_processed += 1
                        if self.total_statements > 0:
                            progress_percent = (self.statements_processed / self.total_statements) * 100
                            elapsed_time = time.time() - self.start_time
                            if self.statements_processed > 1:
                                # Start with 4x higher estimate and gradually decrease as we make progress
                                progress_factor = max(0.25, self.statements_processed/self.total_statements)  # Start at 0.25 (4x) and approach 1.0
                                estimated_total = (elapsed_time / (self.statements_processed * progress_factor)) * self.total_statements
                                remaining_time = max(0, estimated_total - elapsed_time)
                                logger.info(f"Statement progress: {progress_percent:.1f}% ({self.statements_processed}/{self.total_statements}). TOTAL statements in queue: {self.total_statements}. Estimated time remaining: {self._format_time(remaining_time)}")
                            
                    except Exception as e:
                        # If we're in continue-on-error mode, just log the error and continue
                        if self.config.continue_on_error:
                            logger.warning(f"Error executing CREATE TABLE statement: {str(e)}")
                        else:
                            raise ImportError(f"Error executing CREATE TABLE statement: {str(e)}")
            
            # Execute other schema statements
            if schema_statements:
                logger.info(f"Executing {len(schema_statements)} other schema statements")
                for i, stmt in enumerate(schema_statements):
                    try:
                        # Extract table name from the statement if possible
                        table_name = "unknown"
                        table_match = re.search(r'(?:TABLE|VIEW|PROCEDURE|FUNCTION|TRIGGER|EVENT)\s+(?:`([^`]+)`|"([^"]+)")', stmt, re.IGNORECASE)
                        if table_match:
                            # Get the first non-None capture group
                            table_name = next(group for group in table_match.groups() if group is not None)
                        
                        logger.info(f"Executing schema statement {i+1}/{len(schema_statements)} for {table_name}")
                        self.db.execute(stmt)
                        
                        # Track progress for time estimation
                        self.statements_processed += 1
                        if self.total_statements > 0:
                            progress_percent = (self.statements_processed / self.total_statements) * 100
                            elapsed_time = time.time() - self.start_time
                            if self.statements_processed > 1:
                                # Start with 4x higher estimate and gradually decrease as we make progress
                                progress_factor = max(0.25, self.statements_processed/self.total_statements)  # Start at 0.25 (4x) and approach 1.0
                                estimated_total = (elapsed_time / (self.statements_processed * progress_factor)) * self.total_statements
                                remaining_time = max(0, estimated_total - elapsed_time)
                                logger.info(f"Statement progress: {progress_percent:.1f}% ({self.statements_processed}/{self.total_statements}). TOTAL statements in queue: {self.total_statements}. Estimated time remaining: {self._format_time(remaining_time)}")
                        
                    except Exception as e:
                        if self.config.continue_on_error:
                            logger.warning(f"Error executing schema statement: {str(e)}")
                        else:
                            raise ImportError(f"Error executing schema statement: {str(e)}")
            
            # Execute constraint statements
            if constraint_statements:
                logger.info(f"Executing {len(constraint_statements)} constraint statements")
                for i, stmt in enumerate(constraint_statements):
                    try:
                        # Extract table name from the statement
                        table_name = "unknown"
                        table_match = re.search(r'ALTER\s+TABLE\s+(?:`([^`]+)`|"([^"]+)")', stmt, re.IGNORECASE)
                        if table_match:
                            # Get the first non-None capture group
                            table_name = next(group for group in table_match.groups() if group is not None)
                        
                        logger.info(f"Executing constraint statement {i+1}/{len(constraint_statements)} for {table_name}")
                        self.db.execute(stmt)
                        
                        # Track progress for time estimation
                        self.statements_processed += 1
                        if self.total_statements > 0:
                            progress_percent = (self.statements_processed / self.total_statements) * 100
                            elapsed_time = time.time() - self.start_time
                            if self.statements_processed > 1:
                                # Start with 4x higher estimate and gradually decrease as we make progress
                                progress_factor = max(0.25, self.statements_processed/self.total_statements)  # Start at 0.25 (4x) and approach 1.0
                                estimated_total = (elapsed_time / (self.statements_processed * progress_factor)) * self.total_statements
                                remaining_time = max(0, estimated_total - elapsed_time)
                                logger.info(f"Statement progress: {progress_percent:.1f}% ({self.statements_processed}/{self.total_statements}). TOTAL statements in queue: {self.total_statements}. Estimated time remaining: {self._format_time(remaining_time)}")
                        
                    except Exception as e:
                        # Store failed constraints to retry later after all tables are created
                        self.failed_constraints.append({
                            'table': table_name,
                            'stmt': stmt
                        })
                        logger.warning(f"Failed to create constraint for {table_name}, will retry after all tables: {str(e)}")
            
            # Execute data statements
            total_rows = 0
            if data_statements:
                logger.info(f"Executing {len(data_statements)} data insert statements")
                
                # Configure worker for parallel processing if needed
                if self.config.parallel_workers > 1:
                    worker_config = WorkerConfig(
                        num_workers=self.config.parallel_workers,
                        batch_size=self.config.batch_size
                    )
                    with ParallelWorker(worker_config) as worker:
                        # Process inserts in parallel batches
                        # Each batch will be a list of statements
                        batches = [data_statements[i:i+self.config.batch_size] 
                                for i in range(0, len(data_statements), self.config.batch_size)]
                        
                        # Function to execute a batch of statements
                        def execute_batch(batch):
                            rows = 0
                            executed = 0
                            for stmt in batch:
                                try:
                                    self.db.execute(stmt)
                                    rows += 1
                                    executed += 1
                                except Exception as e:
                                    if self.config.continue_on_error:
                                        logger.warning(f"Error executing data statement: {str(e)}")
                                    else:
                                        raise ImportError(f"Error executing data statement: {str(e)}")
                            return (executed, rows)
                        
                        # Execute batches in parallel
                        results = worker.map(execute_batch, batches)
                        
                        # Sum up total rows inserted
                        executed_stmts = sum(r[0] for r in results)
                        total_rows = sum(r[1] for r in results)
                        
                        # Update progress
                        self.statements_processed += executed_stmts
                        if self.total_statements > 0:
                            progress_percent = (self.statements_processed / self.total_statements) * 100
                            elapsed_time = time.time() - self.start_time
                            if self.statements_processed > 1:
                                # Start with 4x higher estimate and gradually decrease as we make progress
                                progress_factor = max(0.25, self.statements_processed/self.total_statements)  # Start at 0.25 (4x) and approach 1.0
                                estimated_total = (elapsed_time / (self.statements_processed * progress_factor)) * self.total_statements
                                remaining_time = max(0, estimated_total - elapsed_time)
                                logger.info(f"Statement progress: {progress_percent:.1f}% ({self.statements_processed}/{self.total_statements}). TOTAL statements in queue: {self.total_statements}. Estimated time remaining: {self._format_time(remaining_time)}")
                else:
                    # Sequential processing
                    for i, stmt in enumerate(data_statements):
                        try:
                            # Extract table name from INSERT statement
                            table_name = "unknown"
                            table_match = re.search(r'INSERT\s+INTO\s+(?:`([^`]+)`|"([^"]+)")', stmt, re.IGNORECASE)
                            if table_match:
                                # Get the first non-None capture group
                                table_name = next(group for group in table_match.groups() if group is not None)
                            
                            if i % 1000 == 0 or i == len(data_statements) - 1:
                                logger.info(f"Executing data statement {i+1}/{len(data_statements)} for {table_name}")
                            
                            self.db.execute(stmt)
                            total_rows += 1
                            
                            # Track progress for time estimation
                            self.statements_processed += 1
                            if self.total_statements > 0 and (i % 100 == 0 or i == len(data_statements) - 1):
                                progress_percent = (self.statements_processed / self.total_statements) * 100
                                elapsed_time = time.time() - self.start_time
                                if self.statements_processed > 1:
                                    # Start with 4x higher estimate and gradually decrease as we make progress
                                    progress_factor = max(0.25, self.statements_processed/self.total_statements)  # Start at 0.25 (4x) and approach 1.0
                                    estimated_total = (elapsed_time / (self.statements_processed * progress_factor)) * self.total_statements
                                    remaining_time = max(0, estimated_total - elapsed_time)
                                    logger.info(f"Statement progress: {progress_percent:.1f}% ({self.statements_processed}/{self.total_statements}). TOTAL statements in queue: {self.total_statements}. Estimated time remaining: {self._format_time(remaining_time)}")
                            
                        except Exception as e:
                            if self.config.continue_on_error:
                                logger.warning(f"Error executing data statement: {str(e)}")
                            else:
                                raise ImportError(f"Error executing data statement: {str(e)}")
            
            # For consolidated files, use the database name as the table name in the result
            table_name = self.database.database
            if file_path.stem.endswith('_schema'):
                table_name = f"{file_path.stem.replace('_schema', '')} (schema)"
            elif file_path.stem.endswith('_data'):
                table_name = f"{file_path.stem.replace('_data', '')} (data)"
                
            logger.info(f"Successfully imported file: {file_path.name}")
            return ImportResult(
                table_name=table_name,
                status="success",
                total_rows=total_rows
            )
            
        except Exception as e:
            logger.error(f"Error importing file {file_path}: {str(e)}", exc_info=True)
            return ImportResult(
                table_name=file_path.stem,
                status="error",
                error_message=str(e)
            )
    
    def _import_table(self, table: str) -> None:
        """Import a single table."""
        try:
            logger.info(f"Importing table: {table}")
            
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