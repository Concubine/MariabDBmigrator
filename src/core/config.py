"""Configuration management for the MariaDB export tool."""
import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional
import yaml
from dataclasses import dataclass

from src.core.exceptions import ConfigError
from src.infrastructure.parallel import parse_worker_count

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

    def __init__(self, output_dir: str = 'exports', batch_size: int = 1000, compression: bool = False,
                 parallel_workers: int = 1, tables: list[str] = None, where: str = '',
                 exclude_schema: bool = False, exclude_indexes: bool = False,
                 exclude_constraints: bool = False, exclude_tables: list[str] = None,
                 exclude_data: list[str] = None, include_information_schema: bool = False):
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

@dataclass
class ImportConfig:
    """Import operation configuration."""
    input_dir: str
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

    def __init__(self, input_dir: str = 'exports', batch_size: int = 1000, compression: bool = False,
                 parallel_workers: int = 1, mode: str = 'cancel',
                 exclude_schema: bool = False, exclude_indexes: bool = False,
                 exclude_constraints: bool = False, disable_foreign_keys: bool = False,
                 continue_on_error: bool = False, import_schema: bool = True,
                 import_data: bool = True):
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

@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str
    file: str
    format: str

@dataclass
class Config:
    """Main configuration class."""
    database: DatabaseConfig
    export: ExportConfig
    import_: ImportConfig
    logging: LoggingConfig

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

def load_config(config_path: Optional[Path] = None) -> Config:
    """Load configuration from YAML file.
    
    Args:
        config_path: Path to configuration file. If not provided, uses default path.
        
    Returns:
        Config object with loaded settings
        
    Raises:
        ConfigError: If configuration is invalid or missing required fields
    """
    if config_path is None:
        config_path = get_default_config_path()
        
    if not config_path.exists():
        raise ConfigError(f"Configuration file not found: {config_path}")
        
    with open(config_path, 'r') as f:
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
        include_information_schema=export_config.get('include_information_schema', False)
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
        disable_foreign_keys=import_config.get('disable_foreign_keys', False),
        continue_on_error=import_config.get('continue_on_error', False),
        import_schema=import_config.get('import_schema', True),
        import_data=import_config.get('import_data', True)
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
