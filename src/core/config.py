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
    password: str
    database: str

@dataclass
class ExportConfig:
    """Export operation configuration."""
    output_dir: str
    batch_size: int
    compression: bool
    parallel_workers: int

@dataclass
class ImportConfig:
    """Import operation configuration."""
    batch_size: int
    compression: bool
    parallel_workers: int

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
    """Get the default configuration file path."""
    # Get the base directory for the application
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
        database=db_config.get('database', '')
    )
    
    # Load export config
    export_config = config_dict.get('export', {})
    export = ExportConfig(
        output_dir=export_config.get('output_dir', 'exports'),
        batch_size=export_config.get('batch_size', 1000),
        compression=export_config.get('compression', False),
        parallel_workers=parse_worker_count(export_config.get('parallel_workers', ''))
    )
    
    # Load import config
    import_config = config_dict.get('import', {})
    import_ = ImportConfig(
        batch_size=import_config.get('batch_size', 1000),
        compression=import_config.get('compression', False),
        parallel_workers=parse_worker_count(import_config.get('parallel_workers', ''))
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
