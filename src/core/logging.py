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

def setup_logging(config) -> None:
    """Set up global logging configuration.
    
    Args:
        config: Either a LoggingConfig object or an integer log level
    """
    # Create handlers
    handlers = []
    
    # If config is an integer, it's a log level
    if isinstance(config, int):
        # Handle basic setup with just a log level
        log_level = config
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        log_file = None
        
        # Create simple formatter for console
        simple_formatter = logging.Formatter(log_format)
        
        # Console handler with simple format
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(simple_formatter)
        handlers.append(console_handler)
        
    else:
        # Assume it's a LoggingConfig object
        log_level = config.level
        log_format = config.format
        log_file = config.file
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s\n'
            '  Location: %(pathname)s:%(lineno)d\n'
            '  Function: %(funcName)s\n'
            '  Thread: %(threadName)s\n'
            '  Process: %(processName)s'
        )
        
        simple_formatter = logging.Formatter(log_format)
        
        # Console handler with simple format
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(simple_formatter)
        handlers.append(console_handler)
        
        # File handler with detailed format if file path is specified
        if log_file:
            log_file_path = Path(log_file)
            log_file_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setFormatter(detailed_formatter)
            handlers.append(file_handler)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add new handlers
    for handler in handlers:
        root_logger.addHandler(handler)
    
    # Log initial configuration
    root_logger.info("Logging system initialized")
    root_logger.info(f"Log level: {log_level}")
    if isinstance(config, LoggingConfig) and config.file:
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

def log_config(config) -> None:
    """Log configuration settings.
    
    Args:
        config: Configuration object or dictionary
    """
    logger = get_logger(__name__)
    
    # Handle different config types
    if hasattr(config, 'database') and hasattr(config, 'export') and hasattr(config, 'import_'):
        # It's our config object structure
        db_config = config.database
        export_config = config.export
        import_config = config.import_
        
        # Log database configuration (excluding sensitive data)
        logger.info("Database Configuration:")
        logger.info(f"  Host: {getattr(db_config, 'host', 'localhost')}")
        logger.info(f"  Port: {getattr(db_config, 'port', 3306)}")
        logger.info(f"  Database: {getattr(db_config, 'database', '')}")
        logger.info(f"  Use Pure: {getattr(db_config, 'use_pure', True)}")
        logger.info(f"  Auth Plugin: {getattr(db_config, 'auth_plugin', 'mysql_native_password')}")
        logger.info(f"  SSL: {getattr(db_config, 'ssl', False)}")
        
        # Log export configuration
        logger.info("Export Configuration:")
        logger.info(f"  Output Directory: {getattr(export_config, 'output_dir', '')}")
        logger.info(f"  Batch Size: {getattr(export_config, 'batch_size', 1000)}")
        logger.info(f"  Compression: {getattr(export_config, 'compression', False)}")
        logger.info(f"  Parallel Workers: {getattr(export_config, 'parallel_workers', 1)}")
        logger.info(f"  Tables: {getattr(export_config, 'tables', [])}")
        logger.info(f"  Where Clause: {getattr(export_config, 'where', '')}")
        logger.info(f"  Exclude Schema: {getattr(export_config, 'exclude_schema', False)}")
        logger.info(f"  Exclude Indexes: {getattr(export_config, 'exclude_indexes', False)}")
        logger.info(f"  Exclude Constraints: {getattr(export_config, 'exclude_constraints', False)}")
        logger.info(f"  Exclude Tables: {getattr(export_config, 'exclude_tables', [])}")
        logger.info(f"  Exclude Data: {getattr(export_config, 'exclude_data', [])}")
        
        # Log import configuration
        logger.info("Import Configuration:")
        logger.info(f"  Batch Size: {getattr(import_config, 'batch_size', 1000)}")
        logger.info(f"  Compression: {getattr(import_config, 'compression', False)}")
        logger.info(f"  Parallel Workers: {getattr(import_config, 'parallel_workers', 1)}")
        logger.info(f"  Mode: {getattr(import_config, 'mode', 'skip')}")
        logger.info(f"  Exclude Schema: {getattr(import_config, 'exclude_schema', False)}")
        logger.info(f"  Exclude Indexes: {getattr(import_config, 'exclude_indexes', False)}")
        logger.info(f"  Exclude Constraints: {getattr(import_config, 'exclude_constraints', False)}")
        logger.info(f"  Disable Foreign Keys: {getattr(import_config, 'disable_foreign_keys', False)}")
        logger.info(f"  Continue on Error: {getattr(import_config, 'continue_on_error', False)}")
        logger.info(f"  Import Schema: {getattr(import_config, 'import_schema', True)}")
        logger.info(f"  Import Data: {getattr(import_config, 'import_data', True)}")
        
    elif isinstance(config, dict):
        # Dictionary-based config
        
        # Log database configuration (excluding sensitive data)
        db_config = config.get('database', {})
        logger.info("Database Configuration:")
        logger.info(f"  Host: {db_config.get('host', 'localhost')}")
        logger.info(f"  Port: {db_config.get('port', 3306)}")
        logger.info(f"  Database: {db_config.get('database', '')}")
        logger.info(f"  Use Pure: {db_config.get('use_pure', True)}")
        logger.info(f"  Auth Plugin: {db_config.get('auth_plugin', 'mysql_native_password')}")
        logger.info(f"  SSL: {db_config.get('ssl', False)}")
        
        # Log SSL details only if SSL is enabled
        if db_config.get('ssl', False):
            logger.info(f"  SSL CA: {db_config.get('ssl_ca', None)}")
            logger.info(f"  SSL Cert: {db_config.get('ssl_cert', None)}")
            logger.info(f"  SSL Key: {db_config.get('ssl_key', None)}")
            logger.info(f"  Verify Cert: {db_config.get('ssl_verify_cert', False)}")
            logger.info(f"  Verify Identity: {db_config.get('ssl_verify_identity', False)}")
        
        # Log export configuration
        export_config = config.get('export', {})
        logger.info("Export Configuration:")
        logger.info(f"  Output Directory: {export_config.get('output_dir', '')}")
        logger.info(f"  Batch Size: {export_config.get('batch_size', 1000)}")
        logger.info(f"  Compression: {export_config.get('compression', False)}")
        logger.info(f"  Parallel Workers: {export_config.get('parallel_workers', 1)}")
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
        logger.info(f"  Parallel Workers: {import_config.get('parallel_workers', 1)}")
        logger.info(f"  Mode: {import_config.get('mode', 'skip')}")
        logger.info(f"  Exclude Schema: {import_config.get('exclude_schema', False)}")
        logger.info(f"  Exclude Indexes: {import_config.get('exclude_indexes', False)}")
        logger.info(f"  Exclude Constraints: {import_config.get('exclude_constraints', False)}")
        logger.info(f"  Disable Foreign Keys: {import_config.get('disable_foreign_keys', False)}")
        logger.info(f"  Continue on Error: {import_config.get('continue_on_error', False)}")
        logger.info(f"  Import Schema: {import_config.get('import_schema', True)}")
        logger.info(f"  Import Data: {import_config.get('import_data', True)}")
    else:
        # Unknown config type
        logger.info(f"Config type not recognized: {type(config)}")
