"""Export service for MariaDB data."""
import os
import logging
import time
import sys
import json
from pathlib import Path
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.core.exceptions import ExportError
from src.core.logging import get_logger
from src.core.config import ExportConfig, DatabaseConfig
from src.domain.models import ExportResult, ExportOptions
from src.infrastructure.mariadb import MariaDB
from src.infrastructure.storage import SQLStorage
from src.infrastructure.parallel import ParallelWorker, WorkerConfig
from src.domain.interfaces import DatabaseInterface
from src.ui.progress import ProgressTracker, ProgressStats
from .validation import MetadataValidator

# SUGGESTION: Add support for different output formats (CSV, JSON, Parquet)
# SUGGESTION: Implement data checksums for validation during import

logger = get_logger(__name__)

class ExportService:
    """Service for exporting database tables."""
    
    def __init__(self, database: DatabaseConfig, config: ExportConfig, ui_interface: Any = None):
        """Initialize the export service.
        
        Args:
            database: Database configuration
            config: Export configuration
            ui_interface: UI interface for progress and results
        """
        # SUGGESTION: Add option for data transformation/filtering pipeline
        # SUGGESTION: Support dependency-aware export ordering for tables with foreign keys
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
        self.ui = ui_interface

        # Initialize flags for exporting additional database objects
        self.config.exclude_triggers = getattr(self.config, 'exclude_triggers', False)
        self.config.exclude_procedures = getattr(self.config, 'exclude_procedures', False)
        self.config.exclude_views = getattr(self.config, 'exclude_views', False)
        self.config.exclude_events = getattr(self.config, 'exclude_events', False)
        self.config.exclude_functions = getattr(self.config, 'exclude_functions', False)
        self.config.exclude_user_types = getattr(self.config, 'exclude_user_types', False)
        
    def export_data(self) -> List[ExportResult]:
        """Export tables from the database.
        
        Returns:
            List of export results
            
        Raises:
            ExportError: If export fails
        """
        # SUGGESTION: Add pre and post export hooks for extensibility
        # SUGGESTION: Consider implementing export as stream/iterator for very large datasets
        self.start_time = time.time()
        try:
            # Connect to database
            self.db.connect()
            
            # Special case for information_schema
            if hasattr(self.config, 'include_information_schema') and self.config.include_information_schema:
                logger.info("Exporting information_schema tables")
                self.db.select_database('information_schema')
                info_schema_results = self._export_database_tables()
                
                # Display summary if UI is available
                if self.ui and hasattr(self.ui, 'display_export_summary'):
                    self.ui.display_export_summary(info_schema_results)
                    
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
                # SUGGESTION: Make this count optional with a flag for faster startup with large databases
                self.total_tables = 0
                for db_name in user_databases:
                    self.db.select_database(db_name)
                    tables = self.db.get_table_names()
                    filtered_tables = [t for t in tables if t not in self.config.exclude_tables]
                    self.total_tables += len(filtered_tables)
                
                # Print progress header for standard output
                print("\n--- MariaDB Export Tool ---")
                print(f"Total tables to process: {self.total_tables}")
                
                logger.info(f"Total tables to process in queue: {self.total_tables}")
                
                # If no specific database was chosen, we'll need to handle each database separately
                # SUGGESTION: Implement option to export databases in parallel
                results = []
                db_count = len(user_databases)
                
                # Keep track of exported objects counts
                total_triggers = 0
                total_procedures = 0
                total_views = 0
                total_events = 0
                total_functions = 0
                total_user_types = 0
                
                for i, db_name in enumerate(user_databases, 1):
                    self.db.select_database(db_name)
                    logger.info(f"Exporting database: {db_name} ({i}/{db_count})")
                    print(f"Exporting database: {db_name} ({i}/{db_count})")
                    
                    # Display progress estimation for current database
                    elapsed = time.time() - self.start_time
                    if i > 1:
                        # Start with more conservative 4x estimate and decrease as progress is made
                        progress_factor = max(0.25, (i-1)/db_count)  # Start at 0.25 (4x) and approach 1.0
                        estimated_total = (elapsed / ((i-1) * progress_factor)) * db_count
                        remaining = estimated_total - elapsed
                        logger.info(f"Progress: {i-1}/{db_count} databases completed. TOTAL databases in queue: {db_count}. Estimated time remaining: {self._format_time(remaining)}")
                        print(f"Progress: {((i-1)/db_count)*100:.1f}% of databases. Est. remaining: {self._format_time(remaining)}")
                    
                    # Get tables to export for this database
                    db_results = self._export_database_tables()

                    # Export additional database objects
                    trigger_count, proc_count, view_count, event_count, func_count, type_count = self._export_additional_objects(db_name)
                    
                    # Add to totals
                    total_triggers += trigger_count
                    total_procedures += proc_count
                    total_views += view_count
                    total_events += event_count
                    total_functions += func_count
                    total_user_types += type_count
                    
                    results.extend(db_results)
                
                # Display final statistics
                total_time = time.time() - self.start_time
                logger.info(f"Total export completed in {self._format_time(total_time)}")
                logger.info(f"Exported {self.tables_processed} tables across {len(user_databases)} databases")
                logger.info(f"Additional database objects exported: {total_triggers} triggers, {total_procedures} procedures, {total_views} views, {total_events} events, {total_functions} functions, {total_user_types} user-defined types")
                
                print(f"\nExport completed in {self._format_time(total_time)}")
                print(f"Exported {self.tables_processed} tables across {len(user_databases)} databases")
                print(f"Additional database objects exported:")
                print(f"  - Triggers: {total_triggers}")
                print(f"  - Stored procedures: {total_procedures}")
                print(f"  - Views: {total_views}")
                print(f"  - Events: {total_events}")
                print(f"  - Functions: {total_functions}")
                print(f"  - User-defined types: {total_user_types}")
                
                # Display summary if UI is available
                if self.ui and hasattr(self.ui, 'display_export_summary'):
                    self.ui.display_export_summary(results)
                
                return results
            else:
                # Normal export of the specified database
                print("\n--- MariaDB Export Tool ---")
                print(f"Exporting database: {self.db.config.get('database', '')}")
                
                results = self._export_database_tables()
                
                # Export additional database objects
                trigger_count, proc_count, view_count, event_count, func_count, type_count = self._export_additional_objects(self.db.config.get('database', ''))
                
                # Display summary if UI is available
                if self.ui and hasattr(self.ui, 'display_export_summary'):
                    self.ui.display_export_summary(results)
                    
                total_time = time.time() - self.start_time
                print(f"\nExport completed in {self._format_time(total_time)}")
                print(f"Exported {len(results)} tables")
                print(f"Additional database objects exported:")
                print(f"  - Triggers: {trigger_count}")
                print(f"  - Stored procedures: {proc_count}")
                print(f"  - Views: {view_count}")
                print(f"  - Events: {event_count}")
                print(f"  - Functions: {func_count}")
                print(f"  - User-defined types: {type_count}")
                
                return results
            
        except Exception as e:
            raise ExportError(f"Export failed: {str(e)}")
            
        finally:
            self.db.disconnect()

    def _export_additional_objects(self, database_name: str) -> tuple:
        """Export additional database objects like triggers, procedures, views, etc.
        
        Args:
            database_name: Database name
            
        Returns:
            Tuple of (trigger_count, procedure_count, view_count, event_count, function_count, user_type_count)
        """
        output_dir = Path(self.config.output_dir) / database_name
        os.makedirs(output_dir, exist_ok=True)
        
        # First, create the metadata file for this database to assist with imports
        self._export_metadata(database_name, output_dir)
        
        logger.info(f"Exporting additional database objects for {database_name}")
        print(f"Exporting additional database objects for {database_name}")
        
        # Initialize counts
        trigger_count = 0
        procedure_count = 0
        view_count = 0
        event_count = 0
        function_count = 0
        user_type_count = 0
        
        # Export triggers
        if not self.config.exclude_triggers:
            try:
                trigger_count = self._export_triggers(database_name, output_dir)
            except Exception as e:
                logger.error(f"Failed to export triggers: {str(e)}")
                print(f"[ERROR] Failed to export triggers: {str(e)}")
            
        # Export stored procedures
        if not self.config.exclude_procedures:
            try:
                procedure_count = self._export_stored_procedures(database_name, output_dir)
            except Exception as e:
                logger.error(f"Failed to export stored procedures: {str(e)}")
                print(f"[ERROR] Failed to export stored procedures: {str(e)}")
            
        # Export views
        if not self.config.exclude_views:
            try:
                view_count = self._export_views(database_name, output_dir)
            except Exception as e:
                logger.error(f"Failed to export views: {str(e)}")
                print(f"[ERROR] Failed to export views: {str(e)}")
            
        # Export events
        if not self.config.exclude_events:
            try:
                event_count = self._export_events(database_name, output_dir)
            except Exception as e:
                logger.error(f"Failed to export events: {str(e)}")
                print(f"[ERROR] Failed to export events: {str(e)}")
                
        # Export functions
        if not self.config.exclude_functions:
            try:
                function_count = self._export_functions(database_name, output_dir)
            except Exception as e:
                logger.error(f"Failed to export functions: {str(e)}")
                print(f"[ERROR] Failed to export functions: {str(e)}")
                
        # Export user-defined types
        if not self.config.exclude_user_types:
            try:
                user_type_count = self._export_user_types(database_name, output_dir)
            except Exception as e:
                logger.error(f"Failed to export user-defined types: {str(e)}")
                print(f"[ERROR] Failed to export user-defined types: {str(e)}")
        
        return (trigger_count, procedure_count, view_count, event_count, function_count, user_type_count)

    def _export_triggers(self, database_name: str, output_dir: Path) -> int:
        """Export database triggers.
        
        Args:
            database_name: Database name
            output_dir: Output directory
            
        Returns:
            Number of triggers exported
        """
        try:
            # Get all triggers for this database
            triggers = self.db.get_triggers()
            
            if not triggers:
                logger.info(f"No triggers found in database {database_name}")
                return 0
                
            logger.info(f"Exporting {len(triggers)} triggers from {database_name}")
            print(f"Exporting {len(triggers)} triggers from {database_name}")
            
            file_path = output_dir / "triggers.sql"
            
            with open(file_path, 'w', encoding='utf-8') as f:
                # Write header
                f.write(f"-- MariaDB Export Tool - Triggers export for database: {database_name}\n")
                f.write(f"-- Exported: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                # Write delimiter change to allow multi-statement triggers
                f.write("DELIMITER ;;\n\n")
                
                # Write each trigger definition
                for trigger in triggers:
                    f.write(f"-- Trigger: {trigger['name']}\n")
                    f.write(f"DROP TRIGGER IF EXISTS `{trigger['name']}`;;\n")
                    f.write(f"CREATE TRIGGER `{trigger['name']}` {trigger['timing']} {trigger['event']} ON `{trigger['table']}` FOR EACH ROW\n")
                    f.write(f"{trigger['statement']};;\n\n")
                    
                # Reset delimiter
                f.write("DELIMITER ;\n")
                
            logger.info(f"Triggers exported to {file_path}")
            print(f"[OK] Triggers exported to {file_path}")
            
            return len(triggers)
                
        except Exception as e:
            logger.error(f"Error exporting triggers: {str(e)}")
            print(f"[ERROR] Error exporting triggers: {str(e)}")
            return 0

    def _export_stored_procedures(self, database_name: str, output_dir: Path) -> int:
        """Export stored procedures.
        
        Args:
            database_name: Database name
            output_dir: Output directory
            
        Returns:
            Number of procedures exported
        """
        try:
            # Get all stored procedures for this database
            procedures = self.db.get_procedures()
            
            if not procedures:
                logger.info(f"No stored procedures found in database {database_name}")
                return 0
                
            logger.info(f"Exporting {len(procedures)} stored procedures from {database_name}")
            print(f"Exporting {len(procedures)} stored procedures from {database_name}")
            
            file_path = output_dir / "procedures.sql"
            
            with open(file_path, 'w', encoding='utf-8') as f:
                # Write header
                f.write(f"-- MariaDB Export Tool - Stored Procedures export for database: {database_name}\n")
                f.write(f"-- Exported: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                # Write delimiter change for procedures
                f.write("DELIMITER ;;\n\n")
                
                # Write each procedure definition
                for procedure in procedures:
                    f.write(f"-- Procedure: {procedure['name']}\n")
                    f.write(f"DROP PROCEDURE IF EXISTS `{procedure['name']}`;;\n")
                    f.write(f"CREATE PROCEDURE `{procedure['name']}`({procedure['param_list']})\n")
                    f.write(f"{procedure['body']};;\n\n")
                    
                # Reset delimiter
                f.write("DELIMITER ;\n")
                
            logger.info(f"Stored procedures exported to {file_path}")
            print(f"[OK] Stored procedures exported to {file_path}")
            
            return len(procedures)
                
        except Exception as e:
            logger.error(f"Error exporting stored procedures: {str(e)}")
            print(f"[ERROR] Error exporting stored procedures: {str(e)}")
            return 0

    def _export_views(self, database_name: str, output_dir: Path) -> int:
        """Export database views.
        
        Args:
            database_name: Database name
            output_dir: Output directory
            
        Returns:
            Number of views exported
        """
        try:
            # Get all views for this database
            views = self.db.get_views()
            
            if not views:
                logger.info(f"No views found in database {database_name}")
                return 0
                
            logger.info(f"Exporting {len(views)} views from {database_name}")
            print(f"Exporting {len(views)} views from {database_name}")
            
            file_path = output_dir / "views.sql"
            
            with open(file_path, 'w', encoding='utf-8') as f:
                # Write header
                f.write(f"-- MariaDB Export Tool - Views export for database: {database_name}\n")
                f.write(f"-- Exported: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                # Write each view definition
                for view in views:
                    f.write(f"-- View: {view['name']}\n")
                    f.write(f"DROP VIEW IF EXISTS `{view['name']}`;\n")
                    f.write(f"CREATE VIEW `{view['name']}` AS {view['definition']};\n\n")
                    
            logger.info(f"Views exported to {file_path}")
            print(f"[OK] Views exported to {file_path}")
            
            return len(views)
                
        except Exception as e:
            logger.error(f"Error exporting views: {str(e)}")
            print(f"[ERROR] Error exporting views: {str(e)}")
            return 0

    def _export_events(self, database_name: str, output_dir: Path) -> int:
        """Export database events.
        
        Args:
            database_name: Database name
            output_dir: Output directory
            
        Returns:
            Number of events exported
        """
        try:
            # Get all events for this database
            events = self.db.get_events()
            
            if not events:
                logger.info(f"No events found in database {database_name}")
                return 0
                
            logger.info(f"Exporting {len(events)} events from {database_name}")
            print(f"Exporting {len(events)} events from {database_name}")
            
            file_path = output_dir / "events.sql"
            
            with open(file_path, 'w', encoding='utf-8') as f:
                # Write header
                f.write(f"-- MariaDB Export Tool - Events export for database: {database_name}\n")
                f.write(f"-- Exported: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                # Enable event scheduler
                f.write("SET GLOBAL event_scheduler = ON;\n\n")
                
                # Write delimiter change for events
                f.write("DELIMITER ;;\n\n")
                
                # Write each event definition
                for event in events:
                    f.write(f"-- Event: {event['name']}\n")
                    f.write(f"DROP EVENT IF EXISTS `{event['name']}`;;")
                    f.write(f"CREATE EVENT `{event['name']}` ON SCHEDULE {event['schedule']}\n")
                    
                    # Add event options if available
                    if event.get('on_completion'):
                        f.write(f"ON COMPLETION {event['on_completion']}\n")
                    
                    if 'status' in event:
                        f.write(f"{event['status']}\n")
                        
                    # Add event body
                    f.write(f"DO {event['body']};;\n\n")
                    
                # Reset delimiter
                f.write("DELIMITER ;\n")
                
            logger.info(f"Events exported to {file_path}")
            print(f"[OK] Events exported to {file_path}")
            
            return len(events)
                
        except Exception as e:
            logger.error(f"Error exporting events: {str(e)}")
            print(f"[ERROR] Error exporting events: {str(e)}")
            return 0

    def _export_functions(self, database_name: str, output_dir: Path) -> int:
        """Export database functions.
        
        Args:
            database_name: Database name
            output_dir: Output directory
            
        Returns:
            Number of functions exported
        """
        try:
            # Get all functions for this database
            functions = self.db.get_functions()
            
            if not functions:
                logger.info(f"No functions found in database {database_name}")
                return 0
                
            logger.info(f"Exporting {len(functions)} functions from {database_name}")
            print(f"Exporting {len(functions)} functions from {database_name}")
            
            file_path = output_dir / "functions.sql"
            
            with open(file_path, 'w', encoding='utf-8') as f:
                # Write header
                f.write(f"-- MariaDB Export Tool - Functions export for database: {database_name}\n")
                f.write(f"-- Exported: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                # Set security to allow function creation
                f.write("SET GLOBAL log_bin_trust_function_creators = 1;\n\n")
                
                # Write delimiter change for functions
                f.write("DELIMITER ;;\n\n")
                
                # Write each function definition
                for function in functions:
                    f.write(f"-- Function: {function['name']}\n")
                    f.write(f"DROP FUNCTION IF EXISTS `{function['name']}`;;\n")
                    f.write(f"CREATE FUNCTION `{function['name']}`({function['param_list']}) RETURNS {function['returns']}\n")
                    
                    # Add function attributes
                    if function.get('characteristic'):
                        f.write(f"{function['characteristic']}\n")
                        
                    # Add function body
                    f.write(f"{function['body']};;\n\n")
                    
                # Reset delimiter
                f.write("DELIMITER ;\n")
                
            logger.info(f"Functions exported to {file_path}")
            print(f"[OK] Functions exported to {file_path}")
            
            return len(functions)
                
        except Exception as e:
            logger.error(f"Error exporting functions: {str(e)}")
            print(f"[ERROR] Error exporting functions: {str(e)}")
            return 0

    def _export_user_types(self, database_name: str, output_dir: Path) -> int:
        """Export user-defined types.
        
        Args:
            database_name: Database name
            output_dir: Output directory
            
        Returns:
            Number of user-defined types exported
        """
        try:
            # Get all user-defined types for this database
            user_types = self.db.get_user_defined_types()
            
            if not user_types:
                logger.info(f"No user-defined types found in database {database_name}")
                return 0
                
            logger.info(f"Exporting {len(user_types)} user-defined types from {database_name}")
            print(f"Exporting {len(user_types)} user-defined types from {database_name}")
            
            file_path = output_dir / "user_types.sql"
            
            with open(file_path, 'w', encoding='utf-8') as f:
                # Write header
                f.write(f"-- MariaDB Export Tool - User-defined types export for database: {database_name}\n")
                f.write(f"-- Exported: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                # Write each user-defined type definition
                for user_type in user_types:
                    f.write(f"-- User-defined type: {user_type['name']}\n")
                    f.write(f"DROP TYPE IF EXISTS `{user_type['name']}`;\n")
                    f.write(f"CREATE TYPE `{user_type['name']}` AS {user_type['definition']};\n\n")
                    
            logger.info(f"User-defined types exported to {file_path}")
            print(f"[OK] User-defined types exported to {file_path}")
            
            return len(user_types)
                
        except Exception as e:
            logger.error(f"Error exporting user-defined types: {str(e)}")
            print(f"[ERROR] Error exporting user-defined types: {str(e)}")
            return 0

    def _format_time(self, seconds: float) -> str:
        """Format time in seconds to a human-readable string.
        
        Args:
            seconds: Time in seconds
            
        Returns:
            Formatted time string
        """
        # SUGGESTION: Use humanize library for better time formatting
        if seconds < 60:
            return f"{seconds:.1f} seconds"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f} minutes"
        else:
            hours = seconds / 3600
            return f"{hours:.1f} hours"

    def _format_size(self, size_bytes: int) -> str:
        """Format size in bytes to a human-readable string.
        
        Args:
            size_bytes: Size in bytes
            
        Returns:
            Formatted size string
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f}PB"

    def _export_table(self, table_name: str) -> ExportResult:
        """Export a single table.
        
        Args:
            table_name: Table name to export
            
        Returns:
            Export result
            
        Raises:
            ExportError: If export fails
        """
        logger.info(f"Exporting table: {table_name}")
        print(f"Exporting table: {table_name}")
        
        start_time = time.time()
        table_output_dir = Path(self.config.output_dir) / self.db.config.get('database', 'default')
        
        try:
            # Create output directory
            os.makedirs(table_output_dir, exist_ok=True)
            
            # Get table metadata
            meta = self.db.get_table_metadata(table_name)
            
            # Export table schema
            schema_path = None
            if not self.config.exclude_schema:
                schema_path = self._export_table_schema(table_name, table_output_dir)
            
            # Export table data if not in exclude_data list
            data_path = None
            rows_exported = 0
            file_size = 0
            
            if table_name not in self.config.exclude_data:
                data_path, rows_exported, file_size = self._export_table_data(
                    table_name, 
                    table_output_dir
                )
            
            # Update total exported rows
            self.total_exported += rows_exported
            
            # Calculate elapsed time
            elapsed_time = time.time() - start_time
            
            # Create result object
            result = ExportResult(
                table_name=table_name,
                success=True,
                rows_exported=rows_exported,
                total_rows=rows_exported,  # For backward compatibility
                file_path=str(data_path) if data_path else None,
                file_size=file_size,
                duration=elapsed_time,
                schema_file=schema_path,
                data_file=data_path
            )
            
            # Log result
            logger.info(
                f"Exported {table_name}: {rows_exported} rows, "
                f"size: {self._format_size(file_size)}, "
                f"time: {self._format_time(elapsed_time)}"
            )
            
            # Print to console too
            print(f"[OK] {table_name}: {rows_exported} rows, {self._format_size(file_size)}, {self._format_time(elapsed_time)}")
            
            # Update UI if available
            if self.ui and hasattr(self.ui, 'display_export_result'):
                self.ui.display_export_result(result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error exporting table {table_name}: {str(e)}")
            print(f"[ERROR] Error exporting {table_name}: {str(e)}")
            
            # Create failure result
            result = ExportResult(
                table_name=table_name,
                success=False,
                error_message=str(e),
                duration=time.time() - start_time
            )
            
            # Update UI if available
            if self.ui and hasattr(self.ui, 'display_export_result'):
                self.ui.display_export_result(result)
            
            return result

    def _export_table_data(self, table_name: str, output_dir: Path) -> tuple:
        """Export table data to a file.
        
        Args:
            table_name: Table name to export
            output_dir: Output directory
            
        Returns:
            Tuple of (path, rows_exported, file_size)
            
        Raises:
            ExportError: If export fails
        """
        # SUGGESTION: Implement option to split large tables into multiple files
        logger.info(f"Exporting data for table: {table_name}")
        
        # Get row count for progress tracking
        row_count = self.db.get_row_count(table_name, self.config.where)
        logger.info(f"Table {table_name} has {row_count} rows to export")
        print(f"Table {table_name} has {row_count} rows to export")
        
        # Create data file path
        file_path = output_dir / f"{table_name}_data.sql"
        
        # Create progress tracker
        tracker = None
        if self.ui and hasattr(self.ui, 'display_progress'):
            tracker = ProgressTracker(
                total_items=max(1, row_count),  # Ensure at least 1 item to avoid division by zero
                update_callback=self.ui.display_progress,
                update_interval=0.5  # Update more frequently for better visual feedback
            )
            
            # Initialize progress at 0
            tracker.update(0)
        
        # Export data
        rows_exported = 0
        with open(file_path, 'w', encoding='utf-8') as f:
            # Write header
            database = self.db.config.get('database', '')
            f.write(f"-- MariaDB Export Tool - Data export for table: {table_name}\n")
            f.write(f"-- Database: {database}\n")
            f.write(f"-- Exported: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- Rows: {row_count}\n\n")
            
            # Disable keys and unique checks for faster import
            f.write("SET FOREIGN_KEY_CHECKS=0;\n")
            f.write("SET UNIQUE_CHECKS=0;\n\n")
            
            # Get data in batches
            f.write(f"-- Data for table {table_name}\n")
            f.write(f"INSERT INTO `{table_name}` VALUES\n")
            
            first_batch = True
            
            # Get data in batches
            for batch_index, batch in enumerate(self.db.get_table_data(
                table_name, 
                self.config.batch_size, 
                self.config.where
            )):
                if not batch:
                    continue
                    
                # Write comma between batches except for the first one
                if not first_batch:
                    f.write(",\n")
                first_batch = False
                
                # Write batch data
                for i, row in enumerate(batch):
                    if i > 0:
                        f.write(",\n")
                    
                    # Format row values according to their types
                    values = []
                    for value in row.values():
                        if value is None:
                            values.append("NULL")
                        elif isinstance(value, (int, float)):
                            values.append(str(value))
                        elif isinstance(value, (bytes, bytearray)):
                            # Handle binary data
                            hex_value = value.hex()
                            values.append(f"0x{hex_value}")
                        else:
                            # Escape string values
                            escaped = str(value).replace("'", "''")
                            values.append(f"'{escaped}'")
                    
                    # Write row values
                    f.write(f"({', '.join(values)})")
                
                # Update progress
                rows_exported += len(batch)
                if tracker:
                    # Print progress to console as well for stdout tracking
                    percent = (rows_exported / max(1, row_count)) * 100
                    if percent % 10 < 2 or percent > 98:  # Log at 0%, 10%, 20%... and > 98%
                        logger.info(f"Progress: {percent:.1f}% of {row_count} rows for {table_name}")
                        
                        # Print progress bar to stdout
                        bar_width = 40
                        filled_width = int(bar_width * percent / 100)
                        bar = '=' * filled_width + '-' * (bar_width - filled_width)
                        print(f"[{bar}] {percent:.1f}% ({rows_exported}/{row_count})", end='\r')
                        sys.stdout.flush()
                        
                    tracker.update(rows_exported)
            
            # Finalize SQL
            f.write(";\n\n")
            f.write("SET FOREIGN_KEY_CHECKS=1;\n")
            f.write("SET UNIQUE_CHECKS=1;\n")
        
        # Get file size
        file_size = os.path.getsize(file_path)
        
        # Complete progress
        if tracker:
            tracker.complete()
        
        # End progress line
        print()  # Move to next line after progress updates
        
        return file_path, rows_exported, file_size

    def _export_table_schema(self, table_name: str, output_dir: Path) -> Path:
        """Export table schema to a file.
        
        Args:
            table_name: Table name to export
            output_dir: Output directory
            
        Returns:
            Path to the schema file
            
        Raises:
            ExportError: If export fails
        """
        logger.info(f"Exporting schema for table: {table_name}")
        
        file_path = output_dir / f"{table_name}_schema.sql"
        
        try:
            # Get table metadata which contains the schema (CREATE TABLE statement)
            metadata = self.db.get_table_metadata(table_name)
            schema = metadata.schema
            
            with open(file_path, 'w', encoding='utf-8') as f:
                # Write header
                database = self.db.config.get('database', '')
                f.write(f"-- MariaDB Export Tool - Schema export for table: {table_name}\n")
                f.write(f"-- Database: {database}\n")
                f.write(f"-- Exported: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                # Write schema
                f.write(f"DROP TABLE IF EXISTS `{table_name}`;\n\n")
                f.write(f"{schema};\n")
                
                # Add indexes if needed and not already in the schema
                if not self.config.exclude_indexes and hasattr(metadata, 'indexes'):
                    if metadata.indexes:
                        f.write("\n-- Table indexes\n")
                        for index in metadata.indexes:
                            # Format index as SQL statement
                            index_name = index.get('name', '')
                            index_columns = index.get('columns', [])
                            if index_name and index_columns:
                                index_cols_str = ', '.join([f"`{col}`" for col in index_columns])
                                f.write(f"CREATE INDEX `{index_name}` ON `{table_name}` ({index_cols_str});\n")
                
                # Add foreign keys if needed and not already in the schema
                if not self.config.exclude_constraints and hasattr(metadata, 'foreign_keys'):
                    if metadata.foreign_keys:
                        f.write("\n-- Foreign key constraints\n")
                        for fk in metadata.foreign_keys:
                            # Format foreign key as SQL statement
                            column = fk.get('column', '')
                            ref_table = fk.get('ref_table', '')
                            ref_column = fk.get('ref_column', '')
                            if column and ref_table and ref_column:
                                f.write(f"ALTER TABLE `{table_name}` ADD CONSTRAINT `fk_{table_name}_{column}` "
                                       f"FOREIGN KEY (`{column}`) REFERENCES `{ref_table}` (`{ref_column}`);\n")
                
            return file_path
            
        except Exception as e:
            raise ExportError(f"Failed to export schema for table {table_name}: {str(e)}")
            
    def _export_metadata(self, database_name: str, output_dir: Path) -> Path:
        """Export metadata about the export operation.
        
        Args:
            database_name: Database name
            output_dir: Output directory
            
        Returns:
            Path to the metadata file
        """
        logger.info(f"Exporting metadata for database: {database_name}")
        
        metadata = {
            'database': database_name,
            'timestamp': time.time(),
            'date': time.strftime('%Y-%m-%d %H:%M:%S'),
            'exported_by': 'MariaDB Export Tool',
            'version': '1.0.0',  # Tool version
        }
        
        file_path = output_dir / "metadata.json"
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"Metadata exported to {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Error exporting metadata: {str(e)}")
            return None

    def _export_database_tables(self) -> List[ExportResult]:
        """Export tables from the current database.
        
        Returns:
            List of export results
        """
        # Get the current database name
        database = self.db.config.get('database', '')
        logger.info(f"Exporting from database: {database}")
        
        # Ensure output directory exists
        output_dir = Path(self.config.output_dir) / database
        os.makedirs(output_dir, exist_ok=True)
        
        # Export metadata first
        self._export_metadata(database, output_dir)
        
        # Get list of tables
        tables = self.db.get_table_names()
        logger.debug(f"All tables found in database: {', '.join(tables)}")
        
        # Filter tables to export
        if self.config.tables:
            # Export only the specified tables
            tables_to_export = [t for t in tables if t in self.config.tables]
            logger.debug(f"Filtered to specified tables: {', '.join(tables_to_export)}")
        else:
            # Export all tables except those in exclude_tables
            tables_to_export = [t for t in tables if t not in self.config.exclude_tables]
            logger.debug(f"Filtered excluding tables: {', '.join(tables_to_export)}")
        
        # Check if we found any tables
        if not tables_to_export:
            logger.warning(f"No tables found to export in database {database}")
            return []
            
        logger.info(f"Found {len(tables_to_export)} tables to export in database {database}")
        
        # Export each table
        results = []
        for i, table in enumerate(tables_to_export, 1):
            logger.info(f"Exporting table {i}/{len(tables_to_export)}: {table}")
            
            try:
                result = self._export_table(table)
                results.append(result)
                self.tables_processed += 1
                
                # Display progress
                if i < len(tables_to_export):
                    elapsed = time.time() - self.start_time
                    progress_factor = max(0.25, i/len(tables_to_export))
                    estimated_total = (elapsed / (i * progress_factor)) * len(tables_to_export)
                    remaining = estimated_total - elapsed
                    logger.info(f"Progress: {(i/len(tables_to_export))*100:.1f}% ({i}/{len(tables_to_export)} tables). "
                               f"TOTAL tables in queue: {self.total_tables}. "
                               f"Estimated time remaining: {self._format_time(remaining)}")
                
            except Exception as e:
                logger.error(f"Failed to export table {table}: {str(e)}")
                result = ExportResult(
                    table_name=table,
                    success=False,
                    error_message=str(e)
                )
                results.append(result)
                
                # Update UI if available
                if self.ui and hasattr(self.ui, 'display_export_result'):
                    self.ui.display_export_result(result)
        
        return results 