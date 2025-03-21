"""Main entry point for the MariaDB export tool."""
import argparse
import sys
import datetime
import signal
from pathlib import Path
from typing import Optional
import logging
import os

from src.core.config import load_config
from src.core.logging import setup_logging, log_config
from src.services.export import ExportService
from src.services.import_ import ImportService
from src.domain.models import ImportMode
from src.core.exceptions import ConfigError, DatabaseError, StorageError, ExportError, ImportError
from src.ui.factory import create_interface

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
    import_parser.add_argument("--force-drop", action="store_true", help="Force drop existing database objects before importing")
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
        sys.exit(0)
    
    return args

def main() -> int:
    """Run the MariaDB export/import tool.
    
    Returns:
        int: Exit code
    """
    try:
        # Register signal handler
        signal.signal(signal.SIGINT, signal_handler)
        
        # Check license
        if not check_license_expiration():
            return 1
        
        # Parse arguments
        args = parse_args()
        
        # Load configuration first so we can access logging settings
        config_path = args.config
        config = load_config(config_path)
        
        # Setup logging using config file settings first, then consider command line override
        verbose = getattr(args, 'verbose', False)
        
        # First, set up logging using the config file settings
        setup_logging(config.logging)
        
        # If verbose flag is provided, override with DEBUG level
        if verbose:
            # Re-initialize with DEBUG level while keeping other settings
            config.logging.level = 'DEBUG'
            logging.info("Verbose flag detected, switching to DEBUG log level")
            setup_logging(config.logging)
        
        # Log configuration for debugging
        log_config(config)
        
        # Execute command
        if args.command == "export":
            # Update export configuration with command line arguments
            if args.tables:
                config.export.tables = args.tables
            if args.output_dir:
                config.export.output_dir = args.output_dir
            if args.batch_size:
                config.export.batch_size = args.batch_size
            if args.compression:
                config.export.compress = True
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
                config.export.exclude_tables.extend(args.exclude_tables)
            if args.exclude_data:
                config.export.exclude_data.extend(args.exclude_data)
            if args.information_schema:
                config.export.include_information_schema = True
            if args.databases:
                config.database.database = args.databases[0]  # Use the first database
            
            # Export data
            global export_service
            
            # Create export service with ASCII interface
            ui_interface = create_interface()
            export_service = ExportService(config.database, config.export, ui_interface)
            
            # Export data
            logging.info("Starting export process")
            results = export_service.export_data()
            logging.info(f"Export process completed with {len(results)} tables")
            
            # Return exit code
            success_count = sum(1 for r in results if getattr(r, 'success', False))
            if success_count == 0 and len(results) > 0:
                logging.error("All exports failed")
                return 1
            elif success_count < len(results):
                logging.warning(f"{len(results) - success_count} of {len(results)} exports failed")
                return 0
            else:
                logging.info("All exports successful")
                return 0
                
        elif args.command == "import":
            # Get files to import
            import_files = args.files
            
            # Update import configuration with command line arguments
            if args.input_dir:
                config.import_.input_dir = args.input_dir
            if args.database:
                config.database.database = args.database
            if args.mode:
                config.import_.mode = ImportMode[args.mode.upper()]
            if args.batch_size:
                config.import_.batch_size = args.batch_size
            if args.compression:
                config.import_.compress = True
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
            if args.force_drop:
                config.import_.force_drop = True
            
            # If no specific files were provided, scan the input directory
            if not import_files:
                input_dirs = config.import_.input_dir
                
                # Convert input_dir to list if it's a string
                if isinstance(input_dirs, str):
                    input_dirs = [input_dirs]
                
                # Process each input directory
                for input_dir in input_dirs:
                    logging.info(f"Scanning input directory: {input_dir}")
                    
                    if os.path.exists(input_dir) and os.path.isdir(input_dir):
                        # Recursively scan for SQL files
                        for root, _, files in os.walk(input_dir):
                            for file in files:
                                if file.endswith('.sql'):
                                    file_path = os.path.join(root, file)
                                    import_files.append(file_path)
                        
                        logging.info(f"Found {len(import_files)} SQL files in {input_dir} and its subdirectories")
                    else:
                        logging.warning(f"Input directory {input_dir} does not exist or is not a directory")
                
                if not import_files:
                    logging.warning("No SQL files found in the specified input directories")
                    return 1
            
            # Import data
            global import_service
            
            # Create export service with ASCII interface
            ui_interface = create_interface()
            
            # Create MariaDB instance from config
            from src.infrastructure.mariadb import MariaDB
            mariadb = MariaDB(config.database)
            
            # Create storage service
            from src.infrastructure.storage import SQLStorage
            storage_service = SQLStorage()
            
            # Update import configuration from command line args
            import_config = config.import_
            
            # Create import service with MariaDB instance
            import_service = ImportService(
                mariadb=mariadb,
                storage_service=storage_service,
                files=import_files,
                config=import_config
            )
            
            # Import the data
            results = import_service.import_data()
            
            # Count successes and failures
            success_count = sum(1 for r in results if r.status == "success")
            warning_count = sum(1 for r in results if r.status == "warning")
            error_count = sum(1 for r in results if r.status == "error")
            
            # Log results
            if success_count == 0 and len(results) > 0:
                logging.error("All imports failed")
                return 1
            elif error_count > 0:
                logging.warning(f"{error_count} of {len(results)} imports failed")
                return 0
            else:
                logging.info(f"All imports completed: {success_count} successful, {warning_count} with warnings")
                return 0
                
    except ConfigError as e:
        logging.error(f"Configuration error: {str(e)}")
        return 1
    except DatabaseError as e:
        logging.error(f"Database error: {str(e)}")
        return 1
    except StorageError as e:
        logging.error(f"Storage error: {str(e)}")
        return 1
    except ExportError as e:
        logging.error(f"Export error: {str(e)}")
        return 1
    except ImportError as e:
        logging.error(f"Import error: {str(e)}")
        return 1
    except Exception as e:
        logging.exception(f"Unexpected error: {str(e)}")
        return 1

if __name__ == "__main__":
    # SUGGESTION: Add profiling option to identify performance bottlenecks
    sys.exit(main())