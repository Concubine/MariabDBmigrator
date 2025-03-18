"""Configuration management for the MariaDB export tool."""
from pathlib import Path
from typing import Dict, Any
import yaml
from dataclasses import dataclass

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
    output_dir: Path
    batch_size: int
    include_schema: bool = True
    include_indexes: bool = True
    include_constraints: bool = True

class Config:
    """Main configuration class."""
    def __init__(self, config_path: Path):
        self.config_path = config_path
        self._config: Dict[str, Any] = {}
        self._load_config()

    def _load_config(self) -> None:
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            self._config = yaml.safe_load(f)

    @property
    def database(self) -> DatabaseConfig:
        """Get database configuration."""
        db_config = self._config.get('database', {})
        return DatabaseConfig(
            host=db_config.get('host', 'localhost'),
            port=db_config.get('port', 3306),
            user=db_config.get('user', ''),
            password=db_config.get('password', ''),
            database=db_config.get('database', '')
        )

    @property
    def export(self) -> ExportConfig:
        """Get export configuration."""
        export_config = self._config.get('export', {})
        return ExportConfig(
            output_dir=Path(export_config.get('output_dir', 'exports')),
            batch_size=export_config.get('batch_size', 1000),
            include_schema=export_config.get('include_schema', True),
            include_indexes=export_config.get('include_indexes', True),
            include_constraints=export_config.get('include_constraints', True)
        )

def load_config(config_path: Path) -> Config:
    """Load configuration from a YAML file."""
    return Config(config_path)
