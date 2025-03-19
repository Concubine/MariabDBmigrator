"""Export service for MariaDB data."""
import os
import logging
from pathlib import Path
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.core.exceptions import ExportError
from src.domain.models import ExportOptions, ExportResult, DatabaseConfig
from src.infrastructure.mariadb import MariaDB
from src.infrastructure.storage import SQLStorage
from src.infrastructure.parallel import ParallelWorker, WorkerConfig
from src.domain.interfaces import DatabaseInterface

logger = logging.getLogger(__name__)

class ExportService:
    """Service for exporting database data."""
    
    def __init__(
        self,
        config: DatabaseConfig,
        output_dir: str,
        tables: Optional[List[str]] = None,
        batch_size: int = 1000,
        where_clause: Optional[str] = None,
        exclude_schema: bool = False,
        exclude_indexes: bool = False,
        exclude_constraints: bool = False,
        compress: bool = False,
        parallel_workers: int = 1
    ):
        """Initialize the export service."""
        self.config = config
        self.output_dir = output_dir
        self.tables = tables
        self.batch_size = batch_size
        self.where_clause = where_clause
        self.exclude_schema = exclude_schema
        self.exclude_indexes = exclude_indexes
        self.exclude_constraints = exclude_constraints
        self.compress = compress
        self.parallel_workers = parallel_workers
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
    def export(self) -> None:
        """Export database data."""
        try:
            # Get list of tables to export
            with MariaDB(self.config) as db:
                if self.tables:
                    available_tables = db.get_table_names()
                    invalid_tables = [t for t in self.tables if t not in available_tables]
                    if invalid_tables:
                        raise ExportError(f"Invalid tables specified: {', '.join(invalid_tables)}")
                    tables_to_export = self.tables
                else:
                    tables_to_export = db.get_table_names()
            
            if not tables_to_export:
                logger.warning("No tables found to export")
                return
            
            logger.info(f"Found {len(tables_to_export)} tables to export")
            
            if self.parallel_workers > 1:
                self._export_parallel(tables_to_export)
            else:
                self._export_sequential(tables_to_export)
            
            logger.info("Export completed successfully")
            
        except Exception as e:
            raise ExportError(f"Export failed: {str(e)}")
    
    def _export_sequential(self, tables: List[str]) -> None:
        """Export tables sequentially."""
        with MariaDB(self.config) as db:
            for table in tables:
                self._export_table(db, table)
    
    def _export_parallel(self, tables: List[str]) -> None:
        """Export tables in parallel."""
        with ThreadPoolExecutor(max_workers=self.parallel_workers) as executor:
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
        with MariaDB(self.config) as db:
            self._export_table(db, table)
    
    def _export_table(self, db: DatabaseInterface, table: str) -> None:
        """Export a single table."""
        try:
            logger.info(f"Exporting table: {table}")
            
            # Get table metadata
            metadata = db.get_table_metadata(table)
            
            # Prepare output file
            output_file = os.path.join(self.output_dir, f"{table}.sql")
            if self.compress:
                output_file += ".gz"
            
            # Write schema if not excluded
            if not self.exclude_schema:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(f"-- Table: {table}\n")
                    f.write(f"{metadata.schema};\n\n")
            
            # Write data
            with open(output_file, 'a', encoding='utf-8') as f:
                for batch in db.get_table_data(table, self.batch_size, self.where_clause):
                    for row in batch:
                        values = [db._format_value(v) for v in row.values()]
                        f.write(f"INSERT INTO {table} VALUES ({', '.join(values)});\n")
                    f.write("\n")
            
            # Write indexes if not excluded
            if not self.exclude_indexes and metadata.indexes:
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write(f"-- Indexes for table {table}\n")
                    for index in metadata.indexes:
                        f.write(f"CREATE INDEX {index['name']} ON {table} ({', '.join(index['columns'])});\n")
                    f.write("\n")
            
            # Write constraints if not excluded
            if not self.exclude_constraints and metadata.constraints:
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