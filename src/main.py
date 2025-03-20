"""Main entry point for the MariaDB export tool."""
import argparse
import sys
import datetime
import signal
from pathlib import Path
from typing import Optional
import logging

from src.core.config import load_config
from src.core.logging import setup_logging, log_config
from src.services.export import ExportService
from src.services.import_ import ImportService
from src.domain.models import ImportMode
from src.core.exceptions import ConfigError, DatabaseError, StorageError, ExportError, ImportError

# SUGGESTION: Consider moving license expiration to configuration file for easier updates
# SUGGESTION: Add version number constant at the top of the file for tracking
# SUGGESTION: Consider implementing a plugin system for extensibility

# License expiration date (March 20, 2027)
EXPIRATION_DATE = datetime.date(2027, 3, 20)

# Global service variables to access in signal handler
export_service = None
import_service = None

def signal_handler(sig, frame):
    """Handle SIGINT (Ctrl+C) signal for graceful exit.
    
    Args:
        sig: Signal number
        frame: Current stack frame
    """
    print("\nReceived interrupt signal. Cleaning up and exiting gracefully...")
    logging.info("Received interrupt signal. Performing cleanup before exit.")
    
    # Cleanup resources
    if export_service is not None and hasattr(export_service, 'db'):
        logging.info("Closing export database connection")
        export_service.db.disconnect()
    
    if import_service is not None and hasattr(import_service, 'db'):
        logging.info("Closing import database connection")
        import_service.db.disconnect()
    
    print("Cleanup complete. Exiting.")
    logging.info("Cleanup complete. Exiting.")
    sys.exit(0)

# SUGGESTION: Add a function to check for new version availability

def check_license_expiration() -> bool:
    """Check if the software license has expired.
    
    Returns:
        bool: True if the license is still valid, False if expired
    """
    # SUGGESTION: Add telemetry/usage tracking with opt-out option
    # SUGGESTION: Implement proper license validation with cryptographic signature instead of hardcoded date
    
    current_date = datetime.date.today()
    if current_date > EXPIRATION_DATE:
        print("\n****************************************************************")
        print("*                                                              *")
        print("*                  LICENSE EXPIRED                             *")
        print("*                                                              *")
        print("* This version of the MariaDB Export Tool has expired.         *")
        print("* Please contact your administrator (pamperloma@proton.me) for a renewed version.     *")
        print("*                                                              *")
        print("****************************************************************\n")
        return False
    
    # Calculate days remaining until expiration
    days_remaining = (EXPIRATION_DATE - current_date).days
    
    # Warn if expiration is approaching (within 30 days)
    # SUGGESTION: Make the warning threshold configurable
    if days_remaining <= 30:
        print("\n****************************************************************")
        print("*                                                              *")
        print(f"*  WARNING: License will expire in {days_remaining} days                  *")
        print("*  Please contact your administrator to renew your license.    *")
        print("*                                                              *")
        print("****************************************************************\n")
    
    return True

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    # SUGGESTION: Consider using a library like click or typer for more maintainable CLI code
    # SUGGESTION: Add command completion support for better user experience
    
    parser = argparse.ArgumentParser(description="MariaDB database export/import tool")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Export command
    export_parser = subparsers.add_parser("export", help="Export database tables")
    export_parser.add_argument("--tables", nargs="+", help="Specific tables to export")
    export_parser.add_argument("--databases", nargs="+", help="Databases to export (if not specified in config)")
    export_parser.add_argument("--output-dir", help="Output directory for exports")
    export_parser.add_argument("--batch-size", type=int, help="Number of rows per batch")
    export_parser.add_argument("--compression", action="store_true", help="Enable compression")
    export_parser.add_argument("--parallel-workers", type=int, help="Number of parallel workers")
    export_parser.add_argument("--where", help="WHERE clause for filtering data")
    export_parser.add_argument("--exclude-schema", action="store_true", help="Exclude schema from export")
    export_parser.add_argument("--exclude-indexes", action="store_true", help="Exclude indexes from export")
    export_parser.add_argument("--exclude-constraints", action="store_true", help="Exclude constraints from export")
    export_parser.add_argument("--exclude-tables", nargs="+", help="Tables to exclude from export")
    export_parser.add_argument("--exclude-data", nargs="+", help="Tables to export without data")
    export_parser.add_argument("--information-schema", action="store_true", help="Export information_schema database")
    export_parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    # SUGGESTION: Add --dry-run option to show what would be exported without actually exporting
    # SUGGESTION: Add --format option to support different export formats (SQL, CSV, JSON, etc.)
    
    # Import command
    import_parser = subparsers.add_parser("import", help="Import database tables")
    import_parser.add_argument("files", nargs="*", help="SQL files to import (if not specified, all files in input-dir will be used)")
    import_parser.add_argument("--input-dir", help="Directory containing SQL files to import (default: same as export output-dir)")
    import_parser.add_argument("--database", help="Target database for import (overrides automatic detection)")
    import_parser.add_argument("--mode", choices=["skip", "overwrite", "merge", "cancel"], help="Import mode")
    import_parser.add_argument("--batch-size", type=int, help="Number of rows per batch")
    import_parser.add_argument("--compression", action="store_true", help="Enable compression")
    import_parser.add_argument("--parallel-workers", type=int, help="Number of parallel workers")
    import_parser.add_argument("--exclude-schema", action="store_true", help="Exclude schema from import")
    import_parser.add_argument("--exclude-indexes", action="store_true", help="Exclude indexes from import")
    import_parser.add_argument("--exclude-constraints", action="store_true", help="Exclude constraints from import")
    import_parser.add_argument("--disable-foreign-keys", action="store_true", help="Disable foreign key checks")
    import_parser.add_argument("--continue-on-error", action="store_true", help="Continue on error")
    import_parser.add_argument("--import-schema", action="store_true", help="Import table schema")
    import_parser.add_argument("--import-data", action="store_true", help="Import table data")
    import_parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    # SUGGESTION: Add --dry-run option to parse and validate SQL without executing
    # SUGGESTION: Add progress bar option with rich or tqdm libraries
    
    # Global options
    parser.add_argument("--config", help="Path to configuration file")
    # SUGGESTION: Add --version flag to display version information
    # SUGGESTION: Add --quiet flag to suppress all output except errors
    
    args = parser.parse_args()
    
    # Show help if no command is specified
    if not args.command:
        parser.print_help()
        sys.exit(1)
        
    return args

def main() -> int:
    """Main entry point."""
    global export_service, import_service
    
    # Register signal handler for graceful exit on Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    
    # Check if license has expired
    if not check_license_expiration():
        return 1
        
    args = parse_args()
    
    # Load configuration
    config_path = Path(args.config) if args.config else None
    config = load_config(config_path)
    
    # Set up logging - preserve config file level unless verbose flag is used
    original_level = config.logging.level  # Store original level from config
    if args.verbose and original_level != "DEBUG":
        config.logging.level = "DEBUG"
        logging.info(f"Overriding log level from {original_level} to DEBUG due to --verbose flag")
    setup_logging(config.logging)
    
    # Log configuration
    # SUGGESTION: Mask sensitive information like passwords more systematically with dedicated utility function
    log_config({
        'database': {
            'host': config.database.host,
            'port': config.database.port,
            'user': config.database.user,
            'password': '********',  # Hide password in logs
            'database': config.database.database,
            'use_pure': config.database.use_pure,
            'auth_plugin': config.database.auth_plugin,
            'ssl': config.database.ssl,
            'ssl_ca': config.database.ssl_ca,
            'ssl_cert': config.database.ssl_cert,
            'ssl_key': config.database.ssl_key,
            'ssl_verify_cert': config.database.ssl_verify_cert,
            'ssl_verify_identity': config.database.ssl_verify_identity
        },
        'export': {
            'output_dir': config.export.output_dir,
            'batch_size': config.export.batch_size,
            'compression': config.export.compression,
            'parallel_workers': config.export.parallel_workers,
            'tables': config.export.tables,
            'where': config.export.where,
            'exclude_schema': config.export.exclude_schema,
            'exclude_indexes': config.export.exclude_indexes,
            'exclude_constraints': config.export.exclude_constraints,
            'exclude_tables': config.export.exclude_tables,
            'exclude_data': config.export.exclude_data
        },
        'import': {
            'batch_size': config.import_.batch_size,
            'compression': config.import_.compression,
            'parallel_workers': config.import_.parallel_workers,
            'mode': config.import_.mode,
            'exclude_schema': config.import_.exclude_schema,
            'exclude_indexes': config.import_.exclude_indexes,
            'exclude_constraints': config.import_.exclude_constraints,
            'disable_foreign_keys': config.import_.disable_foreign_keys,
            'continue_on_error': config.import_.continue_on_error,
            'import_schema': config.import_.import_schema,
            'import_data': config.import_.import_data
        },
        'logging': {
            'level': config.logging.level,
            'file': config.logging.file,
            'format': config.logging.format
        }
    })
    
    try:
        if args.command == "export":
            # Override config with command line arguments
            # SUGGESTION: Extract command-line overrides to a separate function for readability
            if args.tables:
                config.export.tables = args.tables
            if args.databases and not config.database.database:
                # Handle database selection manually via the export service
                # The service will handle this when no database is specified
                logging.info(f"Will export specified databases: {', '.join(args.databases)}")
            if args.output_dir:
                config.export.output_dir = args.output_dir
            if args.batch_size:
                config.export.batch_size = args.batch_size
            if args.compression:
                config.export.compression = True
            if args.parallel_workers:
                config.export.parallel_workers = args.parallel_workers
            if args.where:
                config.export.where = args.where
            if args.exclude_schema:
                config.export.exclude_schema = True
            if args.exclude_indexes:
                config.export.exclude_indexes = True
            if args.exclude_constraints:
                config.export.exclude_constraints = True
            if args.exclude_tables:
                config.export.exclude_tables = args.exclude_tables
            if args.exclude_data:
                config.export.exclude_data = args.exclude_data
            if args.information_schema:
                config.export.include_information_schema = True
                # Clear database name if information_schema is requested
                config.database.database = ""
            
            # Create export service
            export_service = ExportService(config.database, config.export)
            # SUGGESTION: Add progress reporting with a callback mechanism
            export_service.export_data()
            
        elif args.command == "import":
            # Override config with command line arguments
            # SUGGESTION: Extract command-line overrides to a separate function for readability
            if args.input_dir:
                config.import_.input_dir = args.input_dir
            if args.database:
                # Set both the import config database and the database config database
                config.import_.database = args.database
                config.database.database = args.database
            if args.mode:
                config.import_.mode = args.mode
            if args.batch_size:
                config.import_.batch_size = args.batch_size
            if args.compression:
                config.import_.compression = True
            if args.parallel_workers:
                config.import_.parallel_workers = args.parallel_workers
            if args.exclude_schema:
                config.import_.exclude_schema = True
            if args.exclude_indexes:
                config.import_.exclude_indexes = True
            if args.exclude_constraints:
                config.import_.exclude_constraints = True
            if args.disable_foreign_keys:
                config.import_.disable_foreign_keys = True
            if args.continue_on_error:
                config.import_.continue_on_error = True
            if args.import_schema:
                config.import_.import_schema = True
            if args.import_data:
                config.import_.import_data = True
            
            # If no files are specified, use all SQL files in the input directory
            import_files = args.files
            if not import_files:
                input_dir = Path(config.import_.input_dir)
                if input_dir.exists() and input_dir.is_dir():
                    # Get all SQL files in the input directory
                    import_files = [str(f) for f in input_dir.glob("*.sql")]
                    if not import_files:
                        logging.warning(f"No SQL files found in {input_dir}")
                        return 1
                    logging.info(f"Found {len(import_files)} SQL files to import from {input_dir}")
                else:
                    logging.error(f"Input directory not found: {input_dir}")
                    return 1
            
            # Create import service
            import_service = ImportService(config.database, import_files, config.import_)
            # SUGGESTION: Add progress reporting with a callback mechanism
            import_service.import_data()
            
        else:
            print("Error: No command specified")
            return 1
            
        return 0
        
    except ConfigError as e:
        logging.error(f"Configuration error: {str(e)}")
        print(f"Configuration error: {str(e)}")
        return 1
    except DatabaseError as e:
        logging.error(f"Database error: {str(e)}")
        print(f"Database error: {str(e)}")
        return 1
    except StorageError as e:
        logging.error(f"Storage error: {str(e)}")
        print(f"Storage error: {str(e)}")
        return 1
    except ExportError as e:
        logging.error(f"Export error: {str(e)}")
        print(f"Export error: {str(e)}")
        return 1
    except ImportError as e:
        logging.error(f"Import error: {str(e)}")
        print(f"Import error: {str(e)}")
        return 1
    except Exception as e:
        # SUGGESTION: Add option to capture and report crash details to developers
        logging.error(f"Unexpected error: {str(e)}", exc_info=True)
        print(f"Error: {str(e)}")
        return 1
    finally:
        # Clean up resources
        if export_service is not None and hasattr(export_service, 'db'):
            export_service.db.disconnect()
        if import_service is not None and hasattr(import_service, 'db'):
            import_service.db.disconnect()

if __name__ == "__main__":
    # SUGGESTION: Add profiling option to identify performance bottlenecks
    sys.exit(main())