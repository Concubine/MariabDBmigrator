"""Export service for MariaDB data."""
import os
import logging
from pathlib import Path
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.core.exceptions import ExportError
from src.core.logging import get_logger
from src.core.config import ExportConfig, DatabaseConfig
from src.domain.models import ExportResult, ExportOptions
from src.infrastructure.mariadb import MariaDB
from src.infrastructure.storage import SQLStorage
from src.infrastructure.parallel import ParallelWorker, WorkerConfig
from src.domain.interfaces import DatabaseInterface

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
        
    def export_data(self) -> List[ExportResult]:
        """Export tables from the database.
        
        Returns:
            List of export results
            
        Raises:
            ExportError: If export fails
        """
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
                
                # If no specific database was chosen, we'll need to handle each database separately
                results = []
                for db_name in user_databases:
                    self.db.select_database(db_name)
                    logger.info(f"Exporting database: {db_name}")
                    # Get tables to export for this database
                    db_results = self._export_database_tables()
                    results.extend(db_results)
                
                return results
            else:
                # Normal export of the specified database
                return self._export_database_tables()
            
        except Exception as e:
            raise ExportError(f"Export failed: {str(e)}")
            
        finally:
            self.db.disconnect()

    def _export_database_tables(self) -> List[ExportResult]:
        """Export tables from the currently selected database.
        
        Returns:
            List of export results
        """
        # Get tables to export
        tables_to_export = self._get_tables_to_export()
        if not tables_to_export:
            logger.warning(f"No tables found to export in database {self.db.config.get('database', '')}")
            return []
            
        logger.info(f"Found {len(tables_to_export)} tables to export in database {self.db.config.get('database', '')}")
        
        # Export each table
        results = []
        for table in tables_to_export:
            result = self._export_single_table(table)
            results.append(result)
            
        logger.info(f"Export completed successfully for database {self.db.config.get('database', '')}")
        return results
        
    def _get_tables_to_export(self) -> List[str]:
        """Get list of tables to export.
        
        Returns:
            List of table names
        """
        # Get all tables from database
        all_tables = self.db.get_table_names()
        
        # Filter tables based on configuration
        if self.config.tables:
            # Export only specified tables
            tables = [t for t in all_tables if t in self.config.tables]
        else:
            # Export all tables except excluded ones
            tables = [t for t in all_tables if t not in self.config.exclude_tables]
            
        return tables
        
    def _export_single_table(self, table: str) -> ExportResult:
        """Export a single table.
        
        Args:
            table: Name of table to export
            
        Returns:
            Export result
        """
        logger.info(f"Exporting table: {table}")
        
        # Create output directory
        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Export schema if not excluded
        schema_file = None
        if not self.config.exclude_schema and table not in self.config.exclude_data:
            schema_file = output_dir / f"{table}.sql"
            metadata = self.db.get_table_metadata(table)
            self.storage.write_schema(schema_file, metadata)
            
        # Export data if not excluded
        data_file = None
        total_rows = 0
        if table not in self.config.exclude_data:
            data_file = output_dir / f"{table}_data.sql"
            
            # Configure parallel worker
            worker_config = WorkerConfig(
                max_workers=self.config.parallel_workers,
                chunk_size=self.config.batch_size
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
                with open(data_file, 'w', encoding='utf-8') as f:
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
            output.append(f"INSERT INTO {table} VALUES ({', '.join(values)});")
        return '\n'.join(output) + '\n\n'

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