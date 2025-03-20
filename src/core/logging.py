"""Global logging configuration for the application."""
import logging
import sys
import os
import json
from pathlib import Path
from typing import Optional, Dict, Any, List, Set
from dataclasses import dataclass

@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str               # Logging level (DEBUG, INFO, etc.)
    file: str                # Path to log file (optional)
    format: str              # Format string for log messages
    data_diff_tracking: bool = False  # Enable tracking data differences between export/import
    data_diff_file: str = ""          # File to store data difference reports

def setup_logging(config: LoggingConfig) -> None:
    """Set up global logging configuration.
    
    Args:
        config: Logging configuration
    """
    # Create formatters for different output destinations
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s\n'
        '  Location: %(pathname)s:%(lineno)d\n'
        '  Function: %(funcName)s\n'
        '  Thread: %(threadName)s\n'
        '  Process: %(processName)s'
    )
    
    simple_formatter = logging.Formatter(config.format)
    
    # Create handlers collection
    handlers = []
    
    # Always add console handler with simple format for user-friendly output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(simple_formatter)
    handlers.append(console_handler)
    
    # Add file handler with detailed format if file path is specified
    if config.file:
        log_file = Path(config.file)
        # Ensure log directory exists
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(detailed_formatter)
        handlers.append(file_handler)
    
    # Configure root logger with specified level
    root_logger = logging.getLogger()
    root_logger.setLevel(config.level)
    
    # Remove any existing handlers to avoid duplicate log entries
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add new handlers to root logger
    for handler in handlers:
        root_logger.addHandler(handler)
    
    # Log initial configuration details
    root_logger.info("Logging system initialized")
    root_logger.info(f"Log level: {config.level}")
    if config.file:
        root_logger.info(f"Log file: {config.file}")
    root_logger.info(f"Log format: {config.format}")
    
    # Initialize data difference tracking if enabled
    if hasattr(config, 'data_diff_tracking') and config.data_diff_tracking:
        root_logger.info("Data difference tracking enabled")
        if hasattr(config, 'data_diff_file') and config.data_diff_file:
            root_logger.info(f"Data difference report file: {config.data_diff_file}")
            
    # Register global exception handler to ensure exceptions are logged
    sys.excepthook = _global_exception_handler

def _global_exception_handler(exc_type, exc_value, exc_traceback):
    """Global exception handler to ensure all unhandled exceptions are logged."""
    # Log the exception if it's not a KeyboardInterrupt
    if not issubclass(exc_type, KeyboardInterrupt):
        logger = get_logger("exception_handler")
        logger.error(
            "Unhandled exception", 
            exc_info=(exc_type, exc_value, exc_traceback)
        )
    
    # Call the default exception handler
    sys.__excepthook__(exc_type, exc_value, exc_traceback)

def get_logger(name: str = None) -> logging.Logger:
    """Get a logger instance with the specified name.
    
    This is the preferred way to get a logger in this application.
    The logger will inherit the root logger's configuration.
    
    Args:
        name: The name for the logger. If None, returns the root logger.
        
    Returns:
        A configured logger instance
    """
    return logging.getLogger(name) if name else logging.getLogger()

def log_config(config: dict) -> None:
    """Log configuration settings.
    
    Args:
        config: Configuration dictionary
    """
    logger = get_logger(__name__)
    
    # Log database configuration (excluding sensitive data like passwords)
    db_config = config.get('database', {})
    logger.info("Database Configuration:")
    logger.info(f"  Host: {db_config.get('host', 'localhost')}")
    logger.info(f"  Port: {db_config.get('port', 3306)}")
    logger.info(f"  Database: {db_config.get('database', '')}")
    logger.info(f"  Use Pure: {db_config.get('use_pure', True)}")
    logger.info(f"  Auth Plugin: {db_config.get('auth_plugin', 'mysql_native_password')}")
    logger.info(f"  SSL: {db_config.get('ssl', False)}")
    
    # Log SSL details only if SSL is enabled (avoid cluttering logs otherwise)
    if db_config.get('ssl', False):
        logger.info(f"  SSL CA: {db_config.get('ssl_ca', None)}")
        logger.info(f"  SSL Cert: {db_config.get('ssl_cert', None)}")
        logger.info(f"  SSL Key: {db_config.get('ssl_key', None)}")
        logger.info(f"  SSL Verify Cert: {db_config.get('ssl_verify_cert', False)}")
        logger.info(f"  SSL Verify Identity: {db_config.get('ssl_verify_identity', False)}")
    
    # Log export configuration details
    export_config = config.get('export', {})
    logger.info("Export Configuration:")
    logger.info(f"  Output Directory: {export_config.get('output_dir', 'exports')}")
    logger.info(f"  Batch Size: {export_config.get('batch_size', 1000)}")
    logger.info(f"  Compression: {export_config.get('compression', False)}")
    logger.info(f"  Parallel Workers: {export_config.get('parallel_workers', '')}")
    logger.info(f"  Tables: {export_config.get('tables', [])}")
    logger.info(f"  Where Clause: {export_config.get('where', '')}")
    logger.info(f"  Exclude Schema: {export_config.get('exclude_schema', False)}")
    logger.info(f"  Exclude Indexes: {export_config.get('exclude_indexes', False)}")
    logger.info(f"  Exclude Constraints: {export_config.get('exclude_constraints', False)}")
    logger.info(f"  Exclude Tables: {export_config.get('exclude_tables', [])}")
    logger.info(f"  Exclude Data: {export_config.get('exclude_data', [])}")
    
    # Log import configuration details
    import_config = config.get('import', {})
    logger.info("Import Configuration:")
    logger.info(f"  Input Directory: {import_config.get('input_dir', 'exports')}")
    logger.info(f"  Batch Size: {import_config.get('batch_size', 1000)}")
    logger.info(f"  Compression: {import_config.get('compression', False)}")
    logger.info(f"  Parallel Workers: {import_config.get('parallel_workers', '')}")
    logger.info(f"  Mode: {import_config.get('mode', 'skip')}")
    logger.info(f"  Exclude Schema: {import_config.get('exclude_schema', False)}")
    logger.info(f"  Exclude Indexes: {import_config.get('exclude_indexes', False)}")
    logger.info(f"  Exclude Constraints: {import_config.get('exclude_constraints', False)}")
    logger.info(f"  Disable Foreign Keys: {import_config.get('disable_foreign_keys', False)}")
    logger.info(f"  Continue on Error: {import_config.get('continue_on_error', False)}")
    logger.info(f"  Import Schema: {import_config.get('import_schema', True)}")
    logger.info(f"  Import Data: {import_config.get('import_data', True)}")
    
    # Log logging configuration details
    logging_config = config.get('logging', {})
    logger.info("Logging Configuration:")
    logger.info(f"  Level: {logging_config.get('level', 'INFO')}")
    logger.info(f"  File: {logging_config.get('file', '')}")
    logger.info(f"  Format: {logging_config.get('format', '')}")
    logger.info(f"  Data Diff Tracking: {logging_config.get('data_diff_tracking', False)}")
    if logging_config.get('data_diff_tracking', False):
        logger.info(f"  Data Diff File: {logging_config.get('data_diff_file', '')}")

# Data tracking utilities
class DataTracker:
    """Utility class for tracking data differences between export and import operations."""
    
    _instance = None  # Singleton instance
    
    @classmethod
    def get_instance(cls) -> 'DataTracker':
        """Get the singleton instance of DataTracker."""
        if cls._instance is None:
            cls._instance = DataTracker()
        return cls._instance
    
    def __init__(self):
        """Initialize the data tracker."""
        self.logger = get_logger(__name__)
        self.export_stats: Dict[str, Dict[str, Any]] = {}  # Stats for exported tables
        self.import_stats: Dict[str, Dict[str, Any]] = {}  # Stats for imported tables
        self.report_file = None  # File for detailed difference reports
        self.is_enabled = False  # Tracking disabled by default
    
    def enable(self, report_file: Optional[str] = None):
        """Enable data tracking.
        
        Args:
            report_file: Path to file where data difference reports will be written
        """
        self.is_enabled = True
        if report_file:
            self.report_file = Path(report_file)
            # Ensure directory exists
            self.report_file.parent.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Data difference reports will be written to {report_file}")
    
    def track_export_table(self, table_name: str, row_count: int, bytes_size: int):
        """Record export statistics for a table.
        
        Args:
            table_name: Name of the table
            row_count: Number of rows exported
            bytes_size: Size of the exported data in bytes
        """
        # Skip if tracking is disabled
        if not self.is_enabled:
            return
            
        # Store export metrics with timestamp
        self.export_stats[table_name] = {
            'rows': row_count,
            'bytes': bytes_size,
            'timestamp': int(Path.timestamp())
        }
        self.logger.debug(f"Tracked export data for {table_name}: {row_count} rows, {bytes_size} bytes")
    
    def track_import_table(self, table_name: str, row_count: int, bytes_size: int):
        """Record import statistics for a table.
        
        Args:
            table_name: Name of the table
            row_count: Number of rows imported
            bytes_size: Size of the imported data in bytes
        """
        # Skip if tracking is disabled
        if not self.is_enabled:
            return
            
        # Store import metrics with timestamp
        self.import_stats[table_name] = {
            'rows': row_count,
            'bytes': bytes_size,
            'timestamp': int(Path.timestamp())
        }
        self.logger.debug(f"Tracked import data for {table_name}: {row_count} rows, {bytes_size} bytes")
        
        # If we have both export and import stats for this table, log differences
        if table_name in self.export_stats:
            self._log_table_differences(table_name)
    
    def _log_table_differences(self, table_name: str):
        """Log differences between export and import for a table.
        
        Args:
            table_name: Name of the table to compare
        """
        export_data = self.export_stats[table_name]
        import_data = self.import_stats[table_name]
        
        # Calculate differences
        row_diff = import_data['rows'] - export_data['rows']
        byte_diff = import_data['bytes'] - export_data['bytes']
        
        # Only log when differences are detected
        if row_diff != 0 or byte_diff != 0:
            self.logger.warning(
                f"Data difference detected for table {table_name}:\n"
                f"  Row count: {export_data['rows']} (export) vs {import_data['rows']} (import), "
                f"difference: {row_diff:+d}\n"
                f"  Bytes: {export_data['bytes']} (export) vs {import_data['bytes']} (import), "
                f"difference: {byte_diff:+d}"
            )
            
            # Write detailed report to file if configured
            if self.report_file:
                report = {
                    'table': table_name,
                    'export': export_data,
                    'import': import_data,
                    'differences': {
                        'rows': row_diff,
                        'bytes': byte_diff,
                        'percent_rows': (row_diff / export_data['rows']) * 100 if export_data['rows'] > 0 else 0,
                        'percent_bytes': (byte_diff / export_data['bytes']) * 100 if export_data['bytes'] > 0 else 0
                    }
                }
                
                # Append to report file (one JSON object per line)
                with open(self.report_file, 'a') as f:
                    f.write(json.dumps(report) + '\n')
    
    def generate_summary_report(self):
        """Generate a summary report of all data differences.
        
        Returns:
            Summary report as a string
        """
        # Early return if tracking not enabled
        if not self.is_enabled:
            return "Data tracking not enabled"
            
        # Get all unique table names from both export and import
        all_tables = set(self.export_stats.keys()) | set(self.import_stats.keys())
        
        # Initialize counters for summary statistics
        tables_with_differences = 0
        total_row_diff = 0
        missing_in_export = []
        missing_in_import = []
        
        # Process each table
        rows = []
        for table in sorted(all_tables):
            if table in self.export_stats and table in self.import_stats:
                export_rows = self.export_stats[table]['rows']
                import_rows = self.import_stats[table]['rows']
                row_diff = import_rows - export_rows
                
                # Track tables with differences
                if row_diff != 0:
                    tables_with_differences += 1
                    total_row_diff += row_diff
                
                # Add table data to report
                rows.append({
                    'table': table,
                    'export_rows': export_rows,
                    'import_rows': import_rows,
                    'difference': row_diff,
                    'percent': f"{(row_diff / export_rows * 100):.2f}%" if export_rows > 0 else "N/A"
                })
            elif table in self.export_stats:
                # Table exported but not imported
                missing_in_import.append(table)
                rows.append({
                    'table': table,
                    'export_rows': self.export_stats[table]['rows'],
                    'import_rows': 0,
                    'difference': -self.export_stats[table]['rows'],
                    'percent': "-100.00%"
                })
                tables_with_differences += 1
            else:
                # Table imported but not exported
                missing_in_export.append(table)
                rows.append({
                    'table': table,
                    'export_rows': 0,
                    'import_rows': self.import_stats[table]['rows'],
                    'difference': self.import_stats[table]['rows'],
                    'percent': "N/A"
                })
                tables_with_differences += 1
        
        # Generate text report
        report = [
            "=== Data Differences Summary ===",
            f"Total tables: {len(all_tables)}",
            f"Tables with differences: {tables_with_differences}",
            f"Total row difference: {total_row_diff:+d}",
        ]
        
        if missing_in_export:
            report.append(f"Tables missing in export: {', '.join(missing_in_export)}")
        if missing_in_import:
            report.append(f"Tables missing in import: {', '.join(missing_in_import)}")
            
        # Add table-by-table details
        report.append("\nTable Details:")
        report.append(f"{'Table':<30} {'Export Rows':<12} {'Import Rows':<12} {'Difference':<12} {'Percent':<10}")
        report.append("-" * 80)
        
        for row in rows:
            report.append(
                f"{row['table']:<30} {row['export_rows']:<12} {row['import_rows']:<12} "
                f"{row['difference']:+d:<12} {row['percent']:<10}"
            )
            
        return "\n".join(report)

def get_data_tracker() -> DataTracker:
    """Get the singleton DataTracker instance.
    
    Returns:
        The global DataTracker instance
    """
    return DataTracker.get_instance()
