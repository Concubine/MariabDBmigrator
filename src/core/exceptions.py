"""Custom exceptions for the MariaDB export tool."""

class ConfigError(Exception):
    """Configuration error."""
    # Raised when there are issues with configuration loading or validation
    # e.g., missing config file, invalid values, or required fields missing
    pass

class DatabaseError(Exception):
    """Database operation error."""
    # Raised for database connection issues or SQL execution failures
    # Wraps underlying database adapter exceptions with contextual information
    pass

class StorageError(Exception):
    """Storage operation error."""
    # Raised when file operations fail during data storage or retrieval
    # e.g., permission issues, disk full, or file corruption
    pass

class ExportError(Exception):
    """Export operation error."""
    # Raised when export operations fail
    # e.g., failure to extract data, schema issues, or storage failures
    pass

class ImportError(Exception):
    """Import operation error."""
    # Raised when import operations fail
    # e.g., schema conflicts, constraint violations, or data incompatibilities
    pass

class DiagnosticError(Exception):
    """Exception raised for diagnostic errors."""
    # Raised when diagnostics fail to execute or produce results
    # e.g., mismatch in data verification or failure to access data sources
    pass
