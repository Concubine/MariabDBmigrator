"""Global logging configuration for the application."""
import logging
import sys
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str
    file: str
    format: str

def setup_logging(config: LoggingConfig) -> None:
    """Set up global logging configuration.
    
    Args:
        config: Logging configuration
    """
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s\n'
        '  Location: %(pathname)s:%(lineno)d\n'
        '  Function: %(funcName)s\n'
        '  Thread: %(threadName)s\n'
        '  Process: %(processName)s'
    )
    
    simple_formatter = logging.Formatter(config.format)
    
    # Create handlers
    handlers = []
    
    # Console handler with simple format
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(simple_formatter)
    handlers.append(console_handler)
    
    # File handler with detailed format if file path is specified
    if config.file:
        log_file = Path(config.file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(detailed_formatter)
        handlers.append(file_handler)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(config.level)
    
    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add new handlers
    for handler in handlers:
        root_logger.addHandler(handler)
    
    # Log initial configuration
    root_logger.info("Logging system initialized")
    root_logger.info(f"Log level: {config.level}")
    if config.file:
        root_logger.info(f"Log file: {config.file}")
    root_logger.info(f"Log format: {config.format}")

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
    
    # Log database configuration (excluding sensitive data)
    db_config = config.get('database', {})
    logger.info("Database Configuration:")
    logger.info(f"  Host: {db_config.get('host', 'localhost')}")
    logger.info(f"  Port: {db_config.get('port', 3306)}")
    logger.info(f"  Database: {db_config.get('database', '')}")
    logger.info(f"  Use Pure: {db_config.get('use_pure', True)}")
    logger.info(f"  Auth Plugin: {db_config.get('auth_plugin', 'mysql_native_password')}")
    
    # Log export configuration
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
    
    # Log import configuration
    import_config = config.get('import', {})
    logger.info("Import Configuration:")
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
    
    # Log logging configuration
    logging_config = config.get('logging', {})
    logger.info("Logging Configuration:")
    logger.info(f"  Level: {logging_config.get('level', 'INFO')}")
    logger.info(f"  File: {logging_config.get('file', '')}")
    logger.info(f"  Format: {logging_config.get('format', '')}")
