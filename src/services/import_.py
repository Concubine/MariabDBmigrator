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
                
                # First check if there are any database info files
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
            available_dbs = self.db.get_available_databases()
            if self.database.database not in available_dbs:
                logger.info(f"Database '{self.database.database}' does not exist. Creating it.")
                self.db.execute(f"CREATE DATABASE IF NOT EXISTS `{self.database.database}`")
            
            # Select the database
            self.db.select_database(self.database.database)
            logger.info(f"Using database '{self.database.database}' for import")
            
            # Disable foreign key checks globally if configured
            foreign_keys_disabled = False
            if self.config.disable_foreign_keys:
                logger.info("Disabling foreign key checks globally for import process")
                self.db.execute("SET FOREIGN_KEY_CHECKS=0")
                foreign_keys_disabled = True
            
            try:
                # Import each file
                results = []
                
                # Sort files to ensure schema files are processed before data files
                # This is crucial because data files depend on tables created by schema files
                def file_sort_key(file_path):
                    """Sort key function for import files.
                    
                    Priority (lowest to highest):
                    1. Database info files (lowest priority, process first)
                    2. Schema files containing CREATE TABLE statements
                    3. Data files containing INSERT statements
                    """
                    path = Path(file_path)
                    
                    # Quick check based on filename
                    if path.name.endswith('_info.sql'):
                        return 0  # Database info files first
                    
                    # For more accurate categorization, peek into file content
                    try:
                        with open(path, 'r', encoding='utf-8') as f:
                            # Read a sample of the file to determine type
                            sample = ''.join(f.readline() for _ in range(10))
                            
                            # Check for CREATE TABLE statements
                            if re.search(r'CREATE\s+TABLE', sample, re.IGNORECASE):
                                return 1  # Schema files second
                            
                            # Check for INSERT statements
                            if re.search(r'INSERT\s+INTO', sample, re.IGNORECASE):
                                return 2  # Data files last
                    except Exception as e:
                        logger.warning(f"Error reading file {path} for sorting: {str(e)}")
                    
                    # Fallback to filename-based ordering
                    if '_data' in path.name:
                        return 2  # Assume data file based on name
                    else:
                        return 1  # Assume schema file based on name
                
                sorted_files = sorted(self.files, key=file_sort_key)
                logger.info(f"Processing files in order: {', '.join([Path(f).name for f in sorted_files])}")
                
                for file_path in sorted_files:
                    result = self._import_file(Path(file_path))
                    results.append(result)
                
                # After all files have been imported, try to apply any failed constraints
                if self.failed_constraints:
                    logger.info(f"Retrying {len(self.failed_constraints)} failed constraints after all tables have been created")
                    for constraint_info in self.failed_constraints:
                        try:
                            table_name = constraint_info['table']
                            stmt = constraint_info['stmt']
                            logger.info(f"Retrying constraint for table {table_name}: {stmt[:50]}...")
                            self.db.execute(stmt)
                            logger.info(f"Successfully added constraint for {table_name}")
                        except Exception as e:
                            logger.warning(f"Error applying constraint in second pass: {str(e)}")
                    
                return results
            finally:
                # Re-enable foreign key checks if they were disabled
                if foreign_keys_disabled:
                    logger.info("Re-enabling foreign key checks")
                    self.db.execute("SET FOREIGN_KEY_CHECKS=1")
            
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
            
            # Check if table exists
            if self.db.table_exists(table_name):
                if self.config.mode == ImportMode.SKIP.value:
                    logger.info(f"Skipping table {table_name} - already exists")
                    return ImportResult(table_name=table_name, status="skipped")
                    
                elif self.config.mode == ImportMode.OVERWRITE.value:
                    logger.info(f"Dropping existing table {table_name}")
                    
                    # Drop the table (foreign keys are already disabled globally if configured)
                    self.db.drop_table(table_name)
                    
                    # Add to dropped tables set to track that we need to recreate it
                    if hasattr(self, 'dropped_tables'):
                        self.dropped_tables.add(table_name)
                    
                elif self.config.mode == ImportMode.CANCEL.value:
                    logger.info(f"Skipping table {table_name} - already exists")
                    return ImportResult(
                        table_name=table_name,
                        status="error",
                        error_message="Table already exists"
                    )
                    
            # Check if this is a data file for a table that was previously dropped
            is_data_file = '_data' in file_path.name or (any('INSERT INTO' in stmt for stmt in sqlparse.split(sql_content) if stmt.strip()))
            table_was_dropped = hasattr(self, 'dropped_tables') and table_name in self.dropped_tables
            if is_data_file and table_was_dropped and not self.db.table_exists(table_name):
                logger.warning(f"Table {table_name} was dropped but schema hasn't been recreated yet. Looking for schema file.")
                
                # Try to find schema file
                schema_file = None
                for potential_schema in self.files:
                    if Path(potential_schema).stem == table_name:
                        schema_file = potential_schema
                        break
                
                if schema_file:
                    logger.info(f"Found schema file {schema_file}. Importing schema before data.")
                    
                    # Check if this would cause a recursive import
                    if schema_file == str(file_path):
                        logger.warning(f"Avoiding recursive import of {schema_file}")
                        return ImportResult(
                            table_name=table_name,
                            status="error",
                            error_message=f"Cannot import schema from the same file as data"
                        )
                        
                    schema_result = self._import_file(Path(schema_file))
                    if schema_result.status != "success":
                        return ImportResult(
                            table_name=table_name,
                            status="error",
                            error_message=f"Failed to import schema for {table_name}: {schema_result.error_message}"
                        )
                else:
                    logger.error(f"Could not find schema file for {table_name}. Cannot import data.")
                    return ImportResult(
                        table_name=table_name,
                        status="error",
                        error_message=f"Table {table_name} doesn't exist and no schema file found"
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
                        logger.info(f"Skipping constraint for {table_name} (exclude_constraints=true)")
                elif stmt_upper.startswith(('CREATE', 'ALTER', 'DROP')) and self.config.import_schema:
                    schema_statements.append(stmt)
            
            # Execute CREATE TABLE statements first to ensure table exists
            if create_table_statements:
                logger.info(f"Creating table {table_name} with {len(create_table_statements)} CREATE TABLE statements")
                for i, stmt in enumerate(create_table_statements):
                    try:
                        logger.info(f"Executing CREATE TABLE statement {i+1}/{len(create_table_statements)} for {table_name}")
                        self.db.execute(stmt)
                        
                        # Verify the table was actually created
                        if self.db.table_exists(table_name):
                            logger.info(f"Successfully created table {table_name}")
                            # If this created a table that was previously dropped, remove it from the dropped tables set
                            if hasattr(self, 'dropped_tables') and table_name in self.dropped_tables:
                                self.dropped_tables.remove(table_name)
                                logger.info(f"Table {table_name} has been recreated")
                        else:
                            logger.warning(f"CREATE TABLE executed but table {table_name} still doesn't exist")
                    except Exception as e:
                        logger.error(f"Error creating table {table_name}: {str(e)}")
                        logger.error(f"Failed CREATE TABLE statement: {stmt[:100]}...")
                        if not self.config.continue_on_error:
                            return ImportResult(
                                table_name=table_name,
                                status="error",
                                error_message=f"Failed to create table: {str(e)}"
                            )
            
            # Execute other schema statements
            if schema_statements:
                logger.info(f"Executing {len(schema_statements)} schema statements for {table_name}")
                for stmt in schema_statements:
                    try:
                        self.db.execute(stmt)
                        
                        # If this created a table that was previously dropped, remove it from the dropped tables set
                        if hasattr(self, 'dropped_tables') and table_name in self.dropped_tables and 'CREATE TABLE' in stmt.upper():
                            self.dropped_tables.remove(table_name)
                            logger.info(f"Table {table_name} has been recreated")
                    except Exception as e:
                        logger.warning(f"Error executing schema statement: {str(e)}")
                        if not self.config.continue_on_error:
                            raise
            
            # After all schema operations, do an explicit check to see if the table exists
            if (create_table_statements or schema_statements) and not self.db.table_exists(table_name):
                logger.warning(f"After schema processing, table {table_name} still does not exist")
                
                # If we're processing a data file and the table doesn't exist, check for standalone CREATE TABLE
                if is_data_file:
                    # Look for a standalone CREATE TABLE statement that might have been missed
                    logger.info(f"Searching for CREATE TABLE for {table_name} in schema files...")
                    create_stmt = None
                    
                    # Try to find a CREATE TABLE statement in other files
                    for potential_file in self.files:
                        if potential_file == str(file_path):
                            continue  # Skip the current file
                            
                        try:
                            with open(Path(potential_file), 'r', encoding='utf-8') as f:
                                content = f.read()
                                # Look for CREATE TABLE statement for this table
                                match = re.search(rf'CREATE\s+TABLE\s+(?:`?{table_name}`?|`?{self.database.database}`?\.`?{table_name}`?)\s*\(', content, re.IGNORECASE)
                                if match:
                                    # Get the entire CREATE TABLE statement
                                    create_stmt = sqlparse.split(content)[0]
                                    logger.info(f"Found CREATE TABLE statement for {table_name} in {potential_file}")
                                    break
                        except Exception as e:
                            logger.warning(f"Error reading file {potential_file}: {str(e)}")
                    
                    # Execute the CREATE TABLE statement if found
                    if create_stmt:
                        try:
                            logger.info(f"Executing CREATE TABLE for {table_name}: {create_stmt[:100]}...")
                            self.db.execute(create_stmt)
                            if self.db.table_exists(table_name):
                                logger.info(f"Successfully created table {table_name}")
                            else:
                                logger.warning(f"CREATE TABLE executed but table {table_name} still doesn't exist")
                        except Exception as e:
                            logger.error(f"Error creating table {table_name}: {str(e)}")
            
            # Verify table exists before attempting to insert data
            total_rows = 0
            if data_statements and self.config.import_data:
                # Verify table exists before attempting to insert data
                if not self.db.table_exists(table_name):
                    # Try to create the table as a last resort
                    create_stmt = self._extract_create_table_statement(table_name)
                    if create_stmt:
                        try:
                            logger.info(f"Attempting to create table {table_name} before importing data")
                            self.db.execute(create_stmt)
                            if self.db.table_exists(table_name):
                                logger.info(f"Successfully created table {table_name}")
                            else:
                                logger.warning(f"CREATE TABLE executed but table {table_name} still doesn't exist")
                        except Exception as e:
                            logger.error(f"Error creating table {table_name}: {str(e)}")
                    
                    # Check again if table exists after create attempt
                    if not self.db.table_exists(table_name):
                        error_msg = f"Cannot import data - table {table_name} does not exist"
                        logger.error(error_msg)
                        if not self.config.continue_on_error:
                            return ImportResult(
                                table_name=table_name,
                                status="error",
                                error_message=error_msg
                            )
                        else:
                            logger.warning(f"Skipping data import for {table_name} due to missing table")
                    else:
                        # Table was created, proceed with data import
                        batch_size = self.config.batch_size
                        for i in range(0, len(data_statements), batch_size):
                            batch = data_statements[i:i+batch_size]
                            self.db.execute_batch(batch)
                            total_rows += len(batch)
                        
                        logger.info(f"Successfully imported {total_rows} rows into table {table_name}")
                else:
                    # Execute in batches
                    batch_size = self.config.batch_size
                    for i in range(0, len(data_statements), batch_size):
                        batch = data_statements[i:i+batch_size]
                        self.db.execute_batch(batch)
                        total_rows += len(batch)
                    
                    logger.info(f"Successfully imported {total_rows} rows into table {table_name}")
            
            # Execute constraint statements last
            if constraint_statements:
                # Only apply constraints if the table exists 
                if self.db.table_exists(table_name):
                    logger.info(f"Executing {len(constraint_statements)} constraint statements for {table_name}")
                    for stmt in constraint_statements:
                        try:
                            # Check if it's an index creation that might be duplicated
                            if 'CREATE INDEX' in stmt.upper():
                                # Extract index name
                                index_match = re.search(r'INDEX\s+[`"]?(\w+)[`"]?', stmt, re.IGNORECASE)
                                if index_match:
                                    index_name = index_match.group(1)
                                    # Skip if index already exists
                                    try:
                                        # Check if index already exists
                                        check_result = self.db.execute_query(
                                            f"SHOW INDEX FROM `{table_name}` WHERE Key_name = '{index_name}'"
                                        )
                                        if check_result:
                                            logger.info(f"Skipping creation of index '{index_name}' - already exists")
                                            continue
                                    except Exception:
                                        # If error checking index, proceed with trying to create it
                                        pass
                            
                            # Check if it's a foreign key constraint
                            elif 'FOREIGN KEY' in stmt.upper() and 'CONSTRAINT' in stmt.upper():
                                # Extract constraint name
                                fk_match = re.search(r'CONSTRAINT\s+[`"]?(\w+)[`"]?', stmt, re.IGNORECASE)
                                if fk_match:
                                    constraint_name = fk_match.group(1)
                                    # Skip if constraint already exists
                                    try:
                                        # Check if constraint already exists in information_schema
                                        check_result = self.db.execute_query(
                                            f"SELECT * FROM information_schema.TABLE_CONSTRAINTS " +
                                            f"WHERE CONSTRAINT_SCHEMA = '{self.database.database}' " +
                                            f"AND TABLE_NAME = '{table_name}' " +
                                            f"AND CONSTRAINT_NAME = '{constraint_name}'"
                                        )
                                        if check_result:
                                            logger.info(f"Skipping creation of constraint '{constraint_name}' - already exists")
                                            continue
                                    except Exception as check_error:
                                        logger.warning(f"Error checking constraint existence: {str(check_error)}")
                                        # If error checking constraint, proceed with trying to create it
                                        pass
                            
                            self.db.execute(stmt)
                        except Exception as e:
                            # Check for duplicate key error
                            if '1061' in str(e) and 'Duplicate key name' in str(e):
                                logger.info(f"Skipping duplicate key: {str(e)}")
                            # Check for foreign key constraint error - these often need to be retried later
                            elif ('1452' in str(e) or '1215' in str(e)) and ('foreign key constraint fails' in str(e) or 'Cannot add foreign key' in str(e)):
                                logger.info(f"Saving foreign key constraint for retry after all tables are created: {str(e)}")
                                if hasattr(self, 'failed_constraints'):
                                    self.failed_constraints.append({
                                        'table': table_name,
                                        'stmt': stmt,
                                        'error': str(e)
                                    })
                            else:
                                logger.warning(f"Error adding constraint for {table_name}: {str(e)}")
                                if not self.config.continue_on_error:
                                    raise
                else:
                    logger.warning(f"Skipping constraints for {table_name} - table does not exist")
                    if not self.config.continue_on_error:
                        return ImportResult(
                            table_name=table_name,
                            status="error",
                            error_message=f"Cannot apply constraints - table {table_name} does not exist"
                        )
            
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

    def _extract_create_table_statement(self, table_name: str) -> Optional[str]:
        """Extract CREATE TABLE statement for a specific table from files.
        
        Args:
            table_name: Name of the table
            
        Returns:
            CREATE TABLE statement if found, None otherwise
        """
        logger.info(f"Searching for CREATE TABLE statement for {table_name}...")
        
        # Try to find schema file for this table first
        schema_file = None
        for potential_file in self.files:
            if Path(potential_file).stem == table_name:
                schema_file = potential_file
                break
                
        if schema_file:
            try:
                with open(Path(schema_file), 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                    # Look for CREATE TABLE statement
                    for stmt in sqlparse.split(content):
                        if 'CREATE TABLE' in stmt.upper() and table_name.lower() in stmt.lower():
                            logger.info(f"Found CREATE TABLE statement in {schema_file}")
                            return stmt
            except Exception as e:
                logger.warning(f"Error reading schema file {schema_file}: {str(e)}")
        
        # If not found in schema file, try other files
        for potential_file in self.files:
            if potential_file == schema_file:
                continue  # Skip already checked file
                
            try:
                with open(Path(potential_file), 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                    # Look for CREATE TABLE statement
                    for stmt in sqlparse.split(content):
                        if 'CREATE TABLE' in stmt.upper() and table_name.lower() in stmt.lower():
                            logger.info(f"Found CREATE TABLE statement in {potential_file}")
                            return stmt
            except Exception as e:
                logger.warning(f"Error reading file {potential_file}: {str(e)}")
                
        logger.warning(f"No CREATE TABLE statement found for {table_name}")
        return None 