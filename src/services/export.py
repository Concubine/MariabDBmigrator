"""Export service for MariaDB data."""
import os
import logging
import time
from pathlib import Path
from typing import List, Optional, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.core.exceptions import ExportError
from src.core.logging import get_logger
from src.core.config import ExportConfig, DatabaseConfig
from src.domain.models import ExportResult, ExportOptions
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
        self.database = database
        self.config = config
        self.db = MariaDB(database)
        self.storage = SQLStorage()
        self.validator = MetadataValidator(self.db)
        self.total_exported = 0
        self.results = []
        self.start_time = 0
        self.tables_processed = 0
        self.total_tables = 0
        
    def export_data(self) -> List[ExportResult]:
        """Export tables from the database.
        
        Returns:
            List of export results
            
        Raises:
            ExportError: If export fails
        """
        self.start_time = time.time()
        try:
            # Connect to database
            self.db.connect()
            
            # Special case for information_schema
            if hasattr(self.config, 'include_information_schema') and self.config.include_information_schema:
                logger.info("Exporting information_schema tables")
                self.db.select_database('information_schema')
                info_schema_results = self._export_database_tables()
                return info_schema_results
            
            # Handle the case when no specific database is selected
            if 'database' not in self.db.config or not self.db.config.get('database', ''):
                # List available databases
                databases = self.db.get_available_databases()
                # Filter out system databases
                user_databases = [db for db in databases if db not in ['information_schema', 'mysql', 'performance_schema', 'sys']]
                
                if not user_databases:
                    logger.warning("No user databases found to export")
                    return []
                    
                logger.info(f"Found {len(user_databases)} databases available for export")
                logger.info(f"Available databases: {', '.join(user_databases)}")
                
                # Count total tables across all databases for progress estimation
                self.total_tables = 0
                for db_name in user_databases:
                    self.db.select_database(db_name)
                    tables = self.db.get_table_names()
                    filtered_tables = [t for t in tables if t not in self.config.exclude_tables]
                    self.total_tables += len(filtered_tables)
                
                logger.info(f"Total tables to process: {self.total_tables}")
                
                # If no specific database was chosen, we'll need to handle each database separately
                results = []
                db_count = len(user_databases)
                for i, db_name in enumerate(user_databases, 1):
                    self.db.select_database(db_name)
                    logger.info(f"Exporting database: {db_name} ({i}/{db_count})")
                    
                    # Display progress estimation for current database
                    elapsed = time.time() - self.start_time
                    if i > 1:
                        estimated_total = (elapsed / (i - 1)) * db_count
                        remaining = estimated_total - elapsed
                        logger.info(f"Progress: {i-1}/{db_count} databases completed. Estimated time remaining: {self._format_time(remaining)}")
                    
                    # Get tables to export for this database
                    db_results = self._export_database_tables()
                    results.extend(db_results)
                
                # Display final statistics
                total_time = time.time() - self.start_time
                logger.info(f"Total export completed in {self._format_time(total_time)}")
                logger.info(f"Exported {self.tables_processed} tables across {len(user_databases)} databases")
                
                return results
            else:
                # Normal export of the specified database
                return self._export_database_tables()
            
        except Exception as e:
            raise ExportError(f"Export failed: {str(e)}")
            
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

    def _export_database_tables(self) -> List[ExportResult]:
        """Export tables from the currently selected database.
        
        Returns:
            List of export results
        """
        # Get current database name and ensure it's set in the config
        current_db = self.db.get_current_database()
        if current_db:
            self.db.config['database'] = current_db
            logger.info(f"Exporting from database: {current_db}")
        else:
            logger.warning("No database selected for export")
            return []
        
        # Create output directory
        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create a database schema file and a database data file
        schema_file = output_dir / f"{current_db}_schema.sql"
        data_file = output_dir / f"{current_db}_data.sql"
        
        # Initialize the schema file with database creation
        with open(schema_file, 'w', encoding='utf-8') as f:
            f.write(f"-- Database: {current_db}\n")
            f.write(f"-- Generated by MariaDB Export Tool\n\n")
            f.write(f"CREATE DATABASE IF NOT EXISTS `{current_db}`;\n")
            f.write(f"USE `{current_db}`;\n\n")
        
        # Initialize the data file with database selection
        with open(data_file, 'w', encoding='utf-8') as f:
            f.write(f"-- Database: {current_db}\n")
            f.write(f"-- Generated by MariaDB Export Tool\n\n")
            f.write(f"USE `{current_db}`;\n\n")
        
        # Get tables to export
        tables_to_export = self._get_tables_to_export()
        if not tables_to_export:
            logger.warning(f"No tables found to export in database {current_db}")
            return []
            
        logger.info(f"Found {len(tables_to_export)} tables to export in database {current_db}")
        
        # Export tables to the consolidated schema and data files
        results = []
        table_count = len(tables_to_export)
        
        for i, table in enumerate(tables_to_export, 1):
            # Update progress and estimate time
            self.tables_processed += 1
            elapsed_time = time.time() - self.start_time
            if self.total_tables > 0:
                progress_percent = (self.tables_processed / self.total_tables) * 100
                if self.tables_processed > 1:
                    estimated_total = (elapsed_time / self.tables_processed) * self.total_tables
                    remaining_time = estimated_total - elapsed_time
                    logger.info(f"Progress: {progress_percent:.1f}% ({self.tables_processed}/{self.total_tables} tables). Estimated time remaining: {self._format_time(remaining_time)}")
            
            logger.info(f"Exporting table: {table} ({i}/{table_count})")
            result = self._export_single_table(table, schema_file, data_file)
            results.append(result)
            
        logger.info(f"Export completed successfully for database {current_db}")
        return results
        
    def _get_tables_to_export(self) -> List[str]:
        """Get list of tables to export.
        
        Returns:
            List of table names
        """
        # Get all tables from database
        all_tables = self.db.get_table_names()
        
        # Log actual tables found for debugging
        logger.debug(f"All tables found in database: {', '.join(all_tables)}")
        
        # Filter tables based on configuration
        if self.config.tables:
            # Export only specified tables
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
        
    def _export_single_table(self, table: str, schema_file: Path, data_file: Path) -> ExportResult:
        """Export a single table to the schema and data files.
        
        Args:
            table: Table to export
            schema_file: Path to the schema file for the database
            data_file: Path to the data file for the database
            
        Returns:
            ExportResult: Result of the export
        """
        # Get current database name
        current_db = self.db.get_current_database()
        if not current_db:
            current_db = self.db.config.get('database', '')
            logger.warning(f"Using database name from config: {current_db}")
        
        total_rows = 0
        
        # Export schema if not excluded
        if not self.config.exclude_schema and table not in self.config.exclude_data:
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
                
                # Append schema to the database schema file
                with open(schema_file, 'a', encoding='utf-8') as f:
                    f.write(f"-- Table structure for {table}\n")
                    f.write(f"{complete_metadata.schema};\n\n")
                    
                    # Only write constraints that aren't indexes
                    if complete_metadata.constraints and not self.config.exclude_constraints:
                        f.write(f"-- Constraints for table {table}\n")
                        for constraint in complete_metadata.constraints:
                            f.write(f"{constraint};\n")
                        f.write("\n")
            except Exception as e:
                logger.error(f"Failed to get table metadata: {str(e)}", exc_info=True)
            
        # Export data if not excluded
        if table not in self.config.exclude_data:
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
                
                # Write processed data to the database data file
                with open(data_file, 'a', encoding='utf-8') as f:
                    f.write(f"-- Data for table {table}\n")
                    for batch in processed_data:
                        f.write(batch)
                    f.write("\n")
                        
                total_rows = sum(len(batch) for batch in data)
            
        logger.info(f"Successfully exported table: {table} ({total_rows} rows)")
        return ExportResult(
            table_name=table,
            total_rows=total_rows,
            schema_file=schema_file,
            data_file=data_file
        )

    def _format_batch_for_export(self, batch: List[dict], table: str) -> str:
        """Format a batch of rows for export.
        
        Args:
            batch: List of rows to format
            table: Name of the table
            
        Returns:
            Formatted SQL statements
        """
        output = []
        for row in batch:
            values = [self.db._format_value(v) for v in row.values()]
            output.append(f"INSERT INTO `{table}` VALUES ({', '.join(values)});")
        return '\n'.join(output) + '\n'

    def _export_sequential(self, tables: List[str]) -> None:
        """Export tables sequentially."""
        with MariaDB(self.database) as db:
            for table in tables:
                self._export_table(db, table)
    
    def _export_parallel(self, tables: List[str]) -> None:
        """Export tables in parallel."""
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
        """Export a single table with a new database connection."""
        with MariaDB(self.database) as db:
            self._export_table(db, table)
    
    def _export_table(self, db: DatabaseInterface, table: str) -> None:
        """Export a single table."""
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
                        f.write(f"INSERT INTO `{table}` VALUES ({', '.join(values)});\n")
                    f.write("\n")
            
            # Note: Skip writing separate CREATE INDEX statements since they're already in the table schema
            # obtained from SHOW CREATE TABLE
            
            # Write constraints if not excluded
            if not self.config.exclude_constraints and metadata.constraints:
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write(f"-- Constraints for table {table}\n")
                    for constraint in metadata.constraints:
                        f.write(f"ALTER TABLE `{table}` ADD CONSTRAINT {constraint['name']} ")
                        if constraint['type'] == 'UNIQUE':
                            f.write(f"UNIQUE ({', '.join(constraint['columns'])});\n")
                        elif constraint['type'] == 'CHECK':
                            f.write(f"CHECK ({constraint['columns'][0]});\n")
                    f.write("\n")
            
            logger.info(f"Successfully exported table: {table}")
            
        except Exception as e:
            raise ExportError(f"Failed to export table {table}: {str(e)}") 