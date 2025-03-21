"""Configuration management for the MariaDB export tool."""
import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
import yaml
from dataclasses import dataclass, field, is_dataclass, asdict
import logging

from src.core.exceptions import ConfigError
from src.infrastructure.parallel import parse_worker_count

# SUGGESTION: Use environment variables with defaults for containerized deployments
# SUGGESTION: Add support for config validation using JSON Schema or Pydantic
# SUGGESTION: Implement configuration versioning for backward compatibility

# Set up the default configuration locations
DEFAULT_CONFIG_FILE = "config/config.yaml"
DEFAULT_CONFIG_DIRS = [
    ".",
    "~/.mariadbexport",
    "/etc/mariadbexport",
]

@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    host: str
    port: int
    user: str
    password: str = ""
    database: str = ""  # Optional with default empty string
    use_pure: bool = True  # Add default value
    auth_plugin: Optional[str] = None  # Already has default None
    ssl: bool = False  # Add default value
    ssl_ca: Optional[str] = None  # Already has default None
    ssl_cert: Optional[str] = None  # Already has default None
    ssl_key: Optional[str] = None  # Already has default None
    ssl_verify_cert: bool = False  # Add default value
    ssl_verify_identity: bool = False  # Add default value
    # SUGGESTION: Add connection timeout settings
    # SUGGESTION: Add connection pool settings (pool_size, pool_timeout)

@dataclass
class ExportConfig:
    """Export operation configuration."""
    output_dir: str
    batch_size: int
    compression: bool
    parallel_workers: int
    tables: list[str]
    where: str
    exclude_schema: bool
    exclude_indexes: bool
    exclude_constraints: bool
    exclude_tables: list[str]
    exclude_data: list[str]
    include_information_schema: bool
    exclude_triggers: bool
    exclude_procedures: bool
    exclude_views: bool
    exclude_events: bool
    exclude_functions: bool
    exclude_user_types: bool

    def __init__(self, output_dir: str = 'exports', batch_size: int = 1000, compression: bool = False,
                 parallel_workers: int = 1, tables: list[str] = None, where: str = '',
                 exclude_schema: bool = False, exclude_indexes: bool = False,
                 exclude_constraints: bool = False, exclude_tables: list[str] = None,
                 exclude_data: list[str] = None, include_information_schema: bool = False,
                 exclude_triggers: bool = False, exclude_procedures: bool = False,
                 exclude_views: bool = False, exclude_events: bool = False,
                 exclude_functions: bool = False, exclude_user_types: bool = False):
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.compression = compression
        self.parallel_workers = parallel_workers
        self.tables = tables or []
        self.where = where
        self.exclude_schema = exclude_schema
        self.exclude_indexes = exclude_indexes
        self.exclude_constraints = exclude_constraints
        self.exclude_tables = exclude_tables or []
        self.exclude_data = exclude_data or []
        self.include_information_schema = include_information_schema
        self.exclude_triggers = exclude_triggers
        self.exclude_procedures = exclude_procedures
        self.exclude_views = exclude_views
        self.exclude_events = exclude_events
        self.exclude_functions = exclude_functions
        self.exclude_user_types = exclude_user_types
    # SUGGESTION: Add option for partition handling (export/import all or specific partitions)
    # SUGGESTION: Add option for data transformation during export (filtering, anonymization)

@dataclass
class ImportConfig:
    """Import operation configuration."""
    input_dir: Union[str, List[str]]
    batch_size: int
    compression: bool
    parallel_workers: int
    mode: str
    exclude_schema: bool
    exclude_indexes: bool
    exclude_constraints: bool
    disable_foreign_keys: bool
    continue_on_error: bool
    import_schema: bool
    import_data: bool
    import_triggers: bool
    import_procedures: bool
    import_views: bool
    import_events: bool
    import_functions: bool
    import_user_types: bool
    exclude_tables: List[str]
    exclude_data: List[str]
    exclude_triggers: bool
    exclude_procedures: bool
    exclude_views: bool
    exclude_events: bool
    exclude_functions: bool
    exclude_user_types: bool
    database: str = ""  # Target database for import
    force_drop: bool = False  # Whether to force drop existing database objects before importing

    def __init__(self, input_dir: Union[str, List[str]] = 'exports', batch_size: int = 1000, compression: bool = False,
                 parallel_workers: int = 1, mode: str = 'cancel',
                 exclude_schema: bool = False, exclude_indexes: bool = False,
                 exclude_constraints: bool = False, disable_foreign_keys: bool = True,
                 continue_on_error: bool = False, import_schema: bool = True,
                 import_data: bool = True, database: str = "",
                 import_triggers: bool = True, import_procedures: bool = True,
                 import_views: bool = True, import_events: bool = True,
                 import_functions: bool = True, import_user_types: bool = True,
                 exclude_tables: List[str] = None, exclude_data: List[str] = None,
                 exclude_triggers: bool = False, exclude_procedures: bool = False,
                 exclude_views: bool = False, exclude_events: bool = False,
                 exclude_functions: bool = False, exclude_user_types: bool = False,
                 force_drop: bool = False):
        self.input_dir = input_dir
        self.batch_size = batch_size
        self.compression = compression
        self.parallel_workers = parallel_workers
        self.mode = mode
        self.exclude_schema = exclude_schema
        self.exclude_indexes = exclude_indexes
        self.exclude_constraints = exclude_constraints
        self.disable_foreign_keys = disable_foreign_keys
        self.continue_on_error = continue_on_error
        self.import_schema = import_schema
        self.import_data = import_data
        self.database = database
        self.import_triggers = import_triggers
        self.import_procedures = import_procedures
        self.import_views = import_views
        self.import_events = import_events
        self.import_functions = import_functions
        self.import_user_types = import_user_types
        self.exclude_tables = exclude_tables or []
        self.exclude_data = exclude_data or []
        self.exclude_triggers = exclude_triggers
        self.exclude_procedures = exclude_procedures
        self.exclude_views = exclude_views
        self.exclude_events = exclude_events
        self.exclude_functions = exclude_functions
        self.exclude_user_types = exclude_user_types
        self.force_drop = force_drop
    # SUGGESTION: Add option for data validation before import
    # SUGGESTION: Add option for data transformation during import (mapping values)

@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str
    file: str
    format: str
    # SUGGESTION: Add log rotation settings (max_size, backup_count)
    # SUGGESTION: Add option for JSON formatted logs for better machine parsing

@dataclass
class Config:
    """Main configuration class."""
    database: DatabaseConfig
    export: ExportConfig
    import_: ImportConfig
    logging: LoggingConfig
    # SUGGESTION: Add option for performance monitoring/telemetry
    # SUGGESTION: Add plugin configuration for extensibility

def get_default_config_path() -> Path:
    """Get the default configuration file path.
    
    First checks for a config file in the same directory as the executable,
    then falls back to the bundled config file.
    """
    # Get the directory containing the executable
    if getattr(sys, 'frozen', False):
        # Running as compiled executable
        exe_dir = Path(sys.executable).parent
    else:
        # Running as script
        exe_dir = Path(__file__).parent.parent.parent
        
    # First check for external config in the same directory as executable
    external_config = exe_dir / "config" / "config.yaml"
    if external_config.exists():
        return external_config
        
    # Fall back to bundled config
    base_dir = getattr(sys, '_MEIPASS', Path(__file__).parent.parent.parent)
    return Path(base_dir) / "config" / "config.yaml"

def load_config(config_file: Optional[Union[Path, str]] = None) -> Config:
    """Load configuration from a file.
    
    Args:
        config_file: Path to the configuration file
        
    Returns:
        Config object with loaded settings
        
    Raises:
        ConfigError: If configuration is invalid or missing required fields
    """
    # Try to find the configuration file
    if config_file is None:
        config_file = _find_config_file()
    
    # Convert string path to Path object if needed
    if isinstance(config_file, str):
        config_file = Path(config_file)
        
    if not config_file.exists():
        raise ConfigError(f"Configuration file not found: {config_file}")
        
    with open(config_file, 'r') as f:
        config_dict = yaml.safe_load(f)
    
    # Load database config
    db_config = config_dict.get('database', {})
    database = DatabaseConfig(
        host=db_config.get('host', 'localhost'),
        port=db_config.get('port', 3306),
        user=db_config.get('user', 'root'),
        password=db_config.get('password', ''),
        database=db_config.get('database', ''),
        use_pure=db_config.get('use_pure', True),
        auth_plugin=db_config.get('auth_plugin'),
        ssl=db_config.get('ssl', False),
        ssl_ca=db_config.get('ssl_ca', None),
        ssl_cert=db_config.get('ssl_cert', None),
        ssl_key=db_config.get('ssl_key', None),
        ssl_verify_cert=db_config.get('ssl_verify_cert', False),
        ssl_verify_identity=db_config.get('ssl_verify_identity', False)
    )
    
    # Load export config
    export_config = config_dict.get('export', {})
    export = ExportConfig(
        output_dir=export_config.get('output_dir', 'exports'),
        batch_size=export_config.get('batch_size', 1000),
        compression=export_config.get('compression', False),
        parallel_workers=parse_worker_count(export_config.get('parallel_workers', '')),
        tables=export_config.get('tables', []),
        where=export_config.get('where', ''),
        exclude_schema=export_config.get('exclude_schema', False),
        exclude_indexes=export_config.get('exclude_indexes', False),
        exclude_constraints=export_config.get('exclude_constraints', False),
        exclude_tables=export_config.get('exclude_tables', []),
        exclude_data=export_config.get('exclude_data', []),
        include_information_schema=export_config.get('include_information_schema', False),
        exclude_triggers=export_config.get('exclude_triggers', False),
        exclude_procedures=export_config.get('exclude_procedures', False),
        exclude_views=export_config.get('exclude_views', False),
        exclude_events=export_config.get('exclude_events', False),
        exclude_functions=export_config.get('exclude_functions', False),
        exclude_user_types=export_config.get('exclude_user_types', False)
    )
    
    # Load import config
    import_config = config_dict.get('import', {})
    import_ = ImportConfig(
        input_dir=import_config.get('input_dir', export.output_dir),
        batch_size=import_config.get('batch_size', 1000),
        compression=import_config.get('compression', False),
        parallel_workers=parse_worker_count(import_config.get('parallel_workers', '')),
        mode=import_config.get('mode', 'cancel'),
        exclude_schema=import_config.get('exclude_schema', False),
        exclude_indexes=import_config.get('exclude_indexes', False),
        exclude_constraints=import_config.get('exclude_constraints', False),
        disable_foreign_keys=import_config.get('disable_foreign_keys', True),
        continue_on_error=import_config.get('continue_on_error', False),
        import_schema=import_config.get('import_schema', True),
        import_data=import_config.get('import_data', True),
        database=import_config.get('database', ''),
        import_triggers=import_config.get('import_triggers', True),
        import_procedures=import_config.get('import_procedures', True),
        import_views=import_config.get('import_views', True),
        import_events=import_config.get('import_events', True),
        import_functions=import_config.get('import_functions', True),
        import_user_types=import_config.get('import_user_types', True),
        exclude_tables=import_config.get('exclude_tables', []),
        exclude_data=import_config.get('exclude_data', []),
        exclude_triggers=import_config.get('exclude_triggers', False),
        exclude_procedures=import_config.get('exclude_procedures', False),
        exclude_views=import_config.get('exclude_views', False),
        exclude_events=import_config.get('exclude_events', False),
        exclude_functions=import_config.get('exclude_functions', False),
        exclude_user_types=import_config.get('exclude_user_types', False),
        force_drop=import_config.get('force_drop', False)
    )
    
    # Load logging config
    logging_config = config_dict.get('logging', {})
    logging = LoggingConfig(
        level=logging_config.get('level', 'INFO'),
        file=logging_config.get('file', ''),
        format=logging_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    
    return Config(
        database=database,
        export=export,
        import_=import_,
        logging=logging
    )

def _find_config_file() -> Optional[Path]:
    """Find the configuration file in the default locations.
    
    Returns:
        Path to the configuration file, or None if not found
    """
    # First check if the default config file exists
    if os.path.exists(DEFAULT_CONFIG_FILE):
        return Path(DEFAULT_CONFIG_FILE)
        
    # Check the default directories
    for directory in DEFAULT_CONFIG_DIRS:
        expanded_dir = os.path.expanduser(directory)
        config_path = os.path.join(expanded_dir, "config.yaml")
        if os.path.exists(config_path):
            return Path(config_path)
            
    return None

def save_config(config: Config, file_path: Path) -> None:
    """Save the configuration to a file.
    
    Args:
        config: Configuration object
        file_path: Path to the file
        
    Raises:
        ConfigError: If the configuration cannot be saved
    """
    # SUGGESTION: Add option to save only non-default values for cleaner configs
    try:
        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Convert the config to a dictionary
        config_dict = _config_to_dict(config)
        
        # Write the configuration to file
        with open(file_path, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False)
    except Exception as e:
        raise ConfigError(f"Failed to save configuration: {str(e)}")
        
def _config_to_dict(config: Config) -> Dict[str, Any]:
    """Convert a configuration object to a dictionary.
    
    Args:
        config: Configuration object
        
    Returns:
        Dictionary representation of the configuration
    """
    # SUGGESTION: Add support for serializing custom plugin configs
    
    result = {
        'database': asdict(config.database),
        'export': asdict(config.export),
        'import': asdict(config.import_),
        'logging': asdict(config.logging)
    }
    
    # Remove None values for cleaner output
    for section in result.values():
        keys_to_remove = [k for k, v in section.items() if v is None]
        for key in keys_to_remove:
            del section[key]
            
    return result
