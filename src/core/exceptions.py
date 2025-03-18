"""Custom exceptions for the MariaDB export tool."""

class ConfigError(Exception):
    """Configuration error."""
    pass

class DatabaseError(Exception):
    """Database operation error."""
    pass

class StorageError(Exception):
    """Storage operation error."""
    pass

class ExportError(Exception):
    """Export operation error."""
    pass

class ImportError(Exception):
    """Import operation error."""
    pass
