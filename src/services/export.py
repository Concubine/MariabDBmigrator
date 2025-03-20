"""Export service for MariaDB data."""
import os
import logging
from pathlib import Path
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.core.exceptions import ExportError
from src.core.logging import get_logger
from src.core.config import ExportConfig, DatabaseConfig
from src.domain.models import ExportResult, ExportOptions, TableInfo
from src.infrastructure.mariadb import MariaDB
from src.infrastructure.storage import SQLStorage
from src.infrastructure.parallel import ParallelWorker, WorkerConfig
from src.domain.interfaces import DatabaseInterface
from .validation import MetadataValidator

logger = get_logger(__name__)

class ExportService:
    """Service for exporting database tables."""
    
    def __init__(self, database: DatabaseConfig, config: ExportConfig):
        """Initialize the export service.
        
        Args:
            database: Database configuration
            config: Export configuration
        """
        # Implementation: Initialize components needed for database export
        self.database = database
        self.config = config
        self.db = MariaDB(database)  # Database access layer
        self.storage = SQLStorage()  # File storage layer
        self.validator = MetadataValidator(self.db)  # Schema validation
        self.total_exported = 0
        self.results = []
        self.live_view_adapter = None
        
    def export_data(self) -> List[ExportResult]:
        """Export database data.
        
        Returns:
            List of export results
        """
        # Implementation: Export data from database
        
        # Determine if current database is specified or if we should export multiple databases
        if self.database.database:
            # Export tables from the specified database
            self.db.connect()
            results = self._export_database_tables()
            return results
        else:
            # No database specified, export all available databases
            # TODO: Implement multi-database export
            logger.warning("No database specified. Please specify a database to export.")
            return []

    def _export_database_tables(self) -> List[ExportResult]:
        """Export tables from the current database.
        
        Returns:
            List of export results
        """
        # Implementation: Export all selected tables from current database
        
        # Get current database name and ensure it's set in the config
        current_db = self.db.get_current_database()
        if current_db:
            self.db.config['database'] = current_db
            logger.info(f"Exporting from database: {current_db}")
        else:
            logger.warning("No database selected for export")
            return []
        
        # Create a database metadata file to store information about this database
        # Implementation: Generate database creation script for import
        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        db_info_file = output_dir / f"{current_db}_info.sql"
        
        with open(db_info_file, 'w', encoding='utf-8') as f:
            f.write(f"-- Database: {current_db}\n")
            f.write(f"-- Generated by MariaDB Export Tool\n\n")
            f.write(f"CREATE DATABASE IF NOT EXISTS `{current_db}`;\n")
            f.write(f"USE `{current_db}`;\n\n")
            f.write(f"-- Database structure and tables exported separately\n")
        
        # Get tables to export
        # Implementation: Filter tables based on configuration options
        try:
            tables_to_export = self._get_tables_to_export()
            if not tables_to_export:
                logger.warning(f"No tables found to export in database {current_db}")
                return []
                
            logger.info(f"Found {len(tables_to_export)} tables to export in database {current_db}")
            
            # If live view adapter is set, create TableInfo objects and start the export view
            if hasattr(self, 'live_view_adapter') and self.live_view_adapter:
                try:
                    from src.domain.models import TableInfo
                    # Create TableInfo objects for each table
                    table_info_list = []
                    for table_name in tables_to_export:
                        try:
                            # Get row count for each table with proper error handling
                            try:
                                row_count = self.db.get_row_count(table_name)
                            except Exception as e:
                                logger.warning(f"Error getting row count for {table_name}: {e}")
                                row_count = None
                                
                            table_info = TableInfo(
                                name=table_name,
                                row_count=row_count,
                                size_bytes=None,  # Can be filled later if needed
                                has_schema=not self.config.exclude_schema,
                                has_data=table_name not in self.config.exclude_data
                            )
                            table_info_list.append(table_info)
                        except Exception as e:
                            logger.warning(f"Error creating TableInfo for {table_name}: {e}")
                            # Add a minimal TableInfo to avoid breaking the UI
                            table_info_list.append(TableInfo(name=table_name))
                    
                    # Start export monitoring with the table info list
                    if table_info_list:
                        self.live_view_adapter.start_export(table_info_list)
                    else:
                        logger.warning("No TableInfo objects created, live view may not display correctly")
                except Exception as e:
                    logger.exception(f"Error setting up live view: {e}")
                    logger.warning("Continuing export without live view")
            
            # Export each table
            # Implementation: Sequential table export
            results = []
            for table in tables_to_export:
                try:
                    result = self._export_single_table(table)
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error exporting table {table}: {e}")
                    # Add a failed result
                    results.append(ExportResult(
                        table_name=table,
                        total_rows=0,
                        schema_file=None,
                        data_file=None
                    ))
                
            logger.info(f"Export completed successfully for database {current_db}")
            return results
            
        except Exception as e:
            logger.exception(f"Error during export of database {current_db}: {e}")
            return []
        
    def _get_tables_to_export(self) -> List[str]:
        """Get list of tables to export.
        
        Returns:
            List of table names
        """
        # Implementation: Apply table filtering based on configuration settings
        # Get all tables from database
        all_tables = self.db.get_table_names()
        
        # Log actual tables found for debugging
        logger.debug(f"All tables found in database: {', '.join(all_tables)}")
        
        # Filter tables based on configuration
        if self.config.tables:
            # Implementation: Export only explicitly included tables
            tables = [t for t in all_tables if t in self.config.tables]
            logger.debug(f"Filtered to specified tables: {', '.join(tables)}")
        else:
            # Export all tables except excluded ones
            tables = [t for t in all_tables if t not in self.config.exclude_tables]
            logger.debug(f"Filtered excluding tables: {', '.join(tables)}")
            
        # Ensure we don't have an empty list due to case sensitivity issues
        if not tables and all_tables:
            logger.warning(f"No tables matched after filtering. Using all available tables.")
            tables = all_tables
            
        return tables
        
    def _export_single_table(self, table: str) -> ExportResult:
        """Export a single table.
        
        Args:
            table: Table name
            
        Returns:
            Export result
        """
        # Implementation: Multi-step table export process:
        # 1. Export table schema if requested
        # 2. Export table data if not in exclude_data list
        # 3. Track progress
        
        logger.info(f"Exporting table: {table}")
        
        # Create output directory
        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Get current database name
        current_db = self.db.get_current_database()
        if not current_db:
            current_db = self.db.config.get('database', '')
            logger.warning(f"Using database name from config: {current_db}")
        
        # Export schema if not excluded
        schema_file = None
        if not self.config.exclude_schema and table not in self.config.exclude_data:
            schema_file = output_dir / f"{table}.sql"
            try:
                # Get table metadata and validate/complete it
                raw_metadata = self.db.get_table_metadata(table)
                
                # Use the validator to ensure complete metadata
                validated_metadata = self.validator.validate_table_metadata(
                    {
                        'name': raw_metadata.name,
                        'columns': raw_metadata.columns,
                        'primary_key': raw_metadata.primary_key,
                        'foreign_keys': raw_metadata.foreign_keys,
                        'indexes': raw_metadata.indexes,
                        'constraints': raw_metadata.constraints,
                        'schema': raw_metadata.schema,
                    },
                    table_name=table
                )
                
                # Convert back to TableMetadata object
                complete_metadata = self.validator.convert_to_table_metadata(validated_metadata)
                
                # Write schema with database information
                with open(schema_file, 'w', encoding='utf-8') as f:
                    f.write(f"-- Database: {current_db}\n")
                    f.write(f"-- Table: {table}\n")
                    f.write(f"-- Generated by MariaDB Export Tool\n\n")
                    
                    # Add USE statement to ensure correct database
                    f.write(f"USE `{current_db}`;\n\n")
                    
                    # Write the schema
                    f.write(f"-- Table structure for {table}\n")
                    f.write(f"{complete_metadata.schema};\n\n")
                    
                    # Write additional metadata like indexes, foreign keys, etc.
                    if complete_metadata.indexes and not self.config.exclude_indexes:
                        f.write(f"-- Indexes for table {table}\n")
                        for index in complete_metadata.indexes:
                            if index.get('name') != 'PRIMARY':  # Skip primary key index as it's part of CREATE TABLE
                                columns = ', '.join([f"`{col}`" for col in index.get('columns', [])])
                                f.write(f"CREATE INDEX `{index.get('name')}` ON `{table}` ({columns});\n")
                        f.write("\n")
                        
                    if complete_metadata.constraints and not self.config.exclude_constraints:
                        f.write(f"-- Constraints for table {table}\n")
                        for constraint in complete_metadata.constraints:
                            f.write(f"{constraint};\n")
                        f.write("\n")
            except Exception as e:
                logger.error(f"Failed to get table metadata: {str(e)}", exc_info=True)
            
        # Export data if not excluded
        data_file = None
        total_rows = 0
        if table not in self.config.exclude_data:
            data_file = output_dir / f"{table}_data.sql"
            
            # Add database info to data file
            with open(data_file, 'w', encoding='utf-8') as f:
                f.write(f"-- Database: {current_db}\n")
                f.write(f"-- Table: {table}\n")
                f.write(f"-- Generated by MariaDB Export Tool\n\n")
                
                # Add USE statement to ensure correct database
                f.write(f"USE `{current_db}`;\n\n")
            
            # Configure parallel worker
            worker_config = WorkerConfig(
                max_workers=self.config.parallel_workers,
                batch_size=self.config.batch_size
            )
            
            # Export data in parallel
            with ParallelWorker(worker_config) as worker:
                # Get all data from the table
                data = self.db.get_table_data(table, self.config.batch_size, self.config.where)
                
                # Process data in parallel
                processed_data = worker.map(
                    lambda batch: self._format_batch_for_export(batch, table),
                    data
                )
                
                # Write processed data to file
                with open(data_file, 'a', encoding='utf-8') as f:
                    for batch in processed_data:
                        f.write(batch)
                        
                total_rows = sum(len(batch) for batch in data)
            
        logger.info(f"Successfully exported table: {table}")
        return ExportResult(
            table_name=table,
            total_rows=total_rows,
            schema_file=schema_file,
            data_file=data_file
        )

    def set_live_view_adapter(self, adapter) -> None:
        """Set the live view adapter for real-time progress display.
        
        Args:
            adapter: Live view adapter instance
        """
        # Implementation: Configure real-time progress monitoring
        self.live_view_adapter = adapter

    def _format_batch_for_export(self, batch: List[dict], table: str) -> str:
        """Format a batch of rows for SQL export.
        
        Args:
            batch: List of row dictionaries
            table: Table name
            
        Returns:
            SQL insert statement
        """
        # Implementation: Generate optimized SQL INSERT statements for data import
        output = []
        for row in batch:
            values = [self.db._format_value(v) for v in row.values()]
            output.append(f"INSERT INTO {table} VALUES ({', '.join(values)});")
        return '\n'.join(output) + '\n\n'

    def _export_sequential(self, tables: List[str]) -> None:
        """Export tables sequentially.
        
        Args:
            tables: List of table names
        """
        # Implementation: Sequential export using single database connection
        with MariaDB(self.database) as db:
            for table in tables:
                self._export_table(db, table)
    
    def _export_parallel(self, tables: List[str]) -> None:
        """Export tables in parallel.
        
        Args:
            tables: List of table names
        """
        # Implementation: Parallel export using worker pool and multiple connections
        with ThreadPoolExecutor(max_workers=self.config.parallel_workers) as executor:
            future_to_table = {
                executor.submit(self._export_table_with_new_connection, table): table
                for table in tables
            }
            
            for future in as_completed(future_to_table):
                table = future_to_table[future]
                try:
                    future.result()
                except Exception as e:
                    raise ExportError(f"Failed to export table {table}: {str(e)}")
    
    def _export_table_with_new_connection(self, table: str) -> None:
        """Export a table with a new database connection (for parallel processing).
        
        Args:
            table: Table name
        """
        # Implementation: Export table with dedicated database connection (parallel worker)
        with MariaDB(self.database) as db:
            self._export_table(db, table)
    
    def _export_table(self, db: DatabaseInterface, table: str) -> None:
        """Export a table using provided database connection.
        
        Args:
            db: Database interface
            table: Table name
        """
        # Implementation: Core table export logic using provided database connection
        try:
            logger.info(f"Exporting table: {table}")
            
            # Get table metadata
            metadata = db.get_table_metadata(table)
            
            # Prepare output file
            output_file = os.path.join(self.config.output_dir, f"{table}.sql")
            if self.config.compress:
                output_file += ".gz"
            
            # Write schema if not excluded
            if not self.config.exclude_schema:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(f"-- Table: {table}\n")
                    f.write(f"{metadata.schema};\n\n")
            
            # Write data
            with open(output_file, 'a', encoding='utf-8') as f:
                for batch in db.get_table_data(table, self.config.batch_size, self.config.where):
                    for row in batch:
                        values = [db._format_value(v) for v in row.values()]
                        f.write(f"INSERT INTO {table} VALUES ({', '.join(values)});\n")
                    f.write("\n")
            
            # Write indexes if not excluded
            if not self.config.exclude_indexes and metadata.indexes:
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write(f"-- Indexes for table {table}\n")
                    for index in metadata.indexes:
                        f.write(f"CREATE INDEX {index['name']} ON {table} ({', '.join(index['columns'])});\n")
                    f.write("\n")
            
            # Write constraints if not excluded
            if not self.config.exclude_constraints and metadata.constraints:
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write(f"-- Constraints for table {table}\n")
                    for constraint in metadata.constraints:
                        f.write(f"ALTER TABLE {table} ADD CONSTRAINT {constraint['name']} ")
                        if constraint['type'] == 'UNIQUE':
                            f.write(f"UNIQUE ({', '.join(constraint['columns'])});\n")
                        elif constraint['type'] == 'CHECK':
                            f.write(f"CHECK ({constraint['columns'][0]});\n")
                    f.write("\n")
            
            logger.info(f"Successfully exported table: {table}")
            
        except Exception as e:
            raise ExportError(f"Failed to export table {table}: {str(e)}") 