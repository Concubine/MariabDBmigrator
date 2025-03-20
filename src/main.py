"""Main entry point for the MariaDB export tool."""
import argparse
import sys
from pathlib import Path
from typing import Optional
import logging
import os

from src.core.config import load_config
from src.core.logging import setup_logging, log_config, LoggingConfig, get_logger, get_data_tracker
from src.services.export import ExportService
from src.services.import_ import ImportService
from src.domain.models import ImportMode
from src.core.exceptions import ConfigError, DatabaseError, StorageError, ExportError, ImportError
from src.ui.liveview import get_live_view, LiveViewHandler

# Only import LiveView when needed to prevent import errors during compilation
def get_live_view_handler():
    """Get the LiveView handler, importing only when needed."""
    # Implementation: Lazy loading of UI components to avoid dependency issues during compilation
    from src.ui.liveview import get_live_view, LiveViewHandler
    return get_live_view(), LiveViewHandler

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    # Implementation: Set up argument parser with appropriate subcommands and options
    parser = argparse.ArgumentParser(description='MariaDB Export Tool')
    parser.add_argument('-c', '--config', help='Path to configuration file')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    
    # Add live-view as a global option
    parser.add_argument('--live-view', action='store_true', 
                       help='Enable real-time console interface showing export/import progress')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export data from MariaDB')
    export_parser.add_argument('-o', '--output-dir', help='Output directory for exported files')
    export_parser.add_argument("--tables", nargs="+", help="Specific tables to export")
    export_parser.add_argument("--databases", nargs="+", help="Databases to export (if not specified in config)")
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
    
    # Live-view is already available from the parent parser, but we can add 
    # additional details specific to export
    export_parser.add_argument('--track-data', action='store_true',
                             help='Track data differences during export (pairs with --live-view)')
    export_parser.add_argument('--data-diff-file', 
                             help='File to store data difference reports (requires --track-data)')
    
    # Import command
    import_parser = subparsers.add_parser('import', help='Import data to MariaDB')
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
    import_parser.add_argument('--track-data', action='store_true',
                             help='Track data differences during import (pairs with --live-view)')
    import_parser.add_argument('--data-diff-file',
                             help='File to store data difference reports (requires --track-data)')
    import_parser.add_argument("--log-sql-errors", action="store_true", help="Log detailed SQL error messages during import")
    
    # Add a diagnose command specifically for examining data discrepancies
    diagnose_parser = subparsers.add_parser("diagnose", help="Diagnose data discrepancies")
    diagnose_parser.add_argument("--export-dir", required=True, help="Directory containing export data")
    diagnose_parser.add_argument("--import-db", required=True, help="Database to compare against")
    diagnose_parser.add_argument("--tables", nargs="+", help="Specific tables to diagnose")
    diagnose_parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    diagnose_parser.add_argument("--output-file", help="File to save the diagnosis results")
    
    # Add a test command for diagnosing issues with the tool
    test_parser = subparsers.add_parser("test", help="Run diagnostics to test the tool functionality")
    test_parser.add_argument("--component", choices=["database", "export", "import", "liveview"], 
                           help="Specific component to test")
    
    args = parser.parse_args()
    
    # Show help if no command is specified
    if not args.command:
        parser.print_help()
        sys.exit(1)
        
    return args

def main() -> int:
    """Main entry point."""
    # Implementation: Core application workflow:
    # 1. Parse command line arguments
    # 2. Load configuration from file or defaults
    # 3. Set up logging with appropriate verbosity
    # 4. Initialize services based on command (export/import/diagnose)
    # 5. Execute requested operation
    args = parse_args()
    
    # Load configuration
    config_path = Path(args.config) if args.config else None
    config = load_config(config_path)
    
    # Set up logging - preserve config file level unless verbose flag is used
    # Implementation: Configures logging system with appropriate verbosity level
    original_level = config.logging.level  # Store original level from config
    if args.verbose and original_level != "DEBUG":
        config.logging.level = "DEBUG"
        logging.info(f"Overriding log level from {original_level} to DEBUG due to --verbose flag")
    
    # Configure data difference tracking
    # Implementation: Configures data tracking for verifying export/import consistency
    if hasattr(args, 'track_data') and args.track_data:
        config.logging.data_diff_tracking = True
        if hasattr(args, 'data_diff_file') and args.data_diff_file:
            config.logging.data_diff_file = args.data_diff_file
        else:
            # Default to a file in the export/import directory
            if args.command == "export" and hasattr(args, 'output_dir') and args.output_dir:
                diff_dir = args.output_dir
            elif args.command == "import" and hasattr(args, 'input_dir') and args.input_dir:
                diff_dir = args.input_dir
            else:
                diff_dir = "exports"
                
            # Create directory if it doesn't exist
            os.makedirs(diff_dir, exist_ok=True)
            
            # Generate a default filename with timestamp
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            config.logging.data_diff_file = os.path.join(diff_dir, f"data_diff_{args.command}_{timestamp}.json")
    
    setup_logging(config.logging)
    
    # Get a logger
    logger = get_logger(__name__)
    
    # Log configuration
    # Implementation: Logs sanitized configuration (hiding password)
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
            'format': config.logging.format,
            'data_diff_tracking': config.logging.data_diff_tracking,
            'data_diff_file': config.logging.data_diff_file
        }
    })
    
    # Enable data tracker if tracking is enabled
    # Implementation: Initialize data tracking if requested
    if config.logging.data_diff_tracking:
        data_tracker = get_data_tracker()
        data_tracker.enable(config.logging.data_diff_file)
    
    # Setup live view if enabled
    # Implementation: Initialize real-time console UI if requested
    if hasattr(args, 'live_view') and args.live_view:
        logger.info("Live view enabled")
        try:
            # Import the live view handler with better error handling
            from src.ui.liveview import get_live_view, LiveViewHandler
            
            # Get the live view singleton instance
            live_view = get_live_view()
            
            # Add a live view handler to the root logger
            root_logger = logging.getLogger()
            handler = LiveViewHandler(live_view)
            formatter = logging.Formatter('%(message)s')
            handler.setFormatter(formatter)
            root_logger.addHandler(handler)
            
            logger.debug("Live view handler initialized successfully")
        except ImportError as e:
            logger.warning(f"Could not import live view components: {str(e)}")
            logger.warning("Continuing without live view - check your installation")
        except Exception as e:
            logger.warning(f"Error initializing live view: {str(e)}")
            logger.warning("Continuing without live view")
    
    # Special test command for diagnostics
    if hasattr(args, 'command') and args.command == 'test':
        logger.info("Running test command")
        try:
            # Try importing all required components to verify they're available
            import yaml
            from src.core.config import Config
            from src.core.logging import get_logger
            from src.domain.models import TableInfo
            from src.domain.interfaces import DatabaseInterface
            from src.infrastructure.mariadb import MariaDB
            if args.live_view:
                from src.ui.liveview import get_live_view, LiveViewHandler
                from src.ui.integrations import ExportLiveViewAdapter, ImportLiveViewAdapter
            
            logger.info("All components imported successfully")
            logger.info("Configuration loaded successfully:")
            logger.info(f"  Database: {config.database.host}:{config.database.port}/{config.database.database}")
            logger.info(f"  Export dir: {config.export.output_dir}")
            logger.info(f"  Import dir: {config.import_.input_dir}")
            
            return 0
        except Exception as e:
            logger.exception(f"Test command failed: {e}")
            return 1
    
    try:
        if args.command == "export":
            # Implementation: Export workflow:
            # 1. Apply command line overrides to configuration
            # 2. Initialize export service
            # 3. Configure real-time monitoring if requested
            # 4. Execute export operation
            
            # Override config with command line arguments
            if args.tables:
                config.export.tables = args.tables
            if args.databases and not config.database.database:
                # Handle database selection manually via the export service
                # The service will handle this when no database is specified
                logger.info(f"Will export specified databases: {', '.join(args.databases)}")
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
            
            # Create export service and set up live view if enabled
            service = ExportService(config.database, config.export)
            
            # Enable live view if requested
            if args.live_view:
                try:
                    from src.ui.integrations import ExportLiveViewAdapter
                    export_view = ExportLiveViewAdapter()
                    service.set_live_view_adapter(export_view)
                except ImportError:
                    logger.warning("Could not initialize live view adapter. Continuing without live view.")
            
            service.export_data()
            
        elif args.command == "import":
            # Implementation: Import workflow:
            # 1. Determine files to import (from arguments or input directory)
            # 2. Override configuration with command line arguments
            # 3. Initialize import service
            # 4. Configure real-time monitoring if requested
            # 5. Execute import operation
            
            # Override config with command line arguments
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
            if args.log_sql_errors:
                config.import_.log_sql_errors = True
            
            # If no files are specified, use all SQL files in the input directory
            import_files = args.files
            if not import_files:
                input_dir = Path(config.import_.input_dir)
                if input_dir.exists() and input_dir.is_dir():
                    # Get all SQL files in the input directory
                    import_files = [str(f) for f in input_dir.glob("*.sql")]
                    if not import_files:
                        logger.warning(f"No SQL files found in {input_dir}")
                        return 1
                    logger.info(f"Found {len(import_files)} SQL files to import from {input_dir}")
                else:
                    logger.error(f"Input directory not found: {input_dir}")
                    return 1
            
            # Create import service and set up live view if enabled
            service = ImportService(config.database, import_files, config.import_)
            
            # Enable live view if requested
            if args.live_view:
                try:
                    from src.ui.integrations import ImportLiveViewAdapter
                    import_view = ImportLiveViewAdapter()
                    service.set_live_view_adapter(import_view)
                except ImportError:
                    logger.warning("Could not initialize live view adapter. Continuing without live view.")
            
            service.import_data()
            
        elif args.command == "diagnose":
            # Implementation: Diagnose workflow:
            # 1. Validate required options
            # 2. Initialize diagnostic service
            # 3. Execute diagnosis operation
            # 4. Save/display diagnostic results
            
            # Create diagnostic service
            try:
                # Only import when needed to prevent import errors
                from src.services.diagnostics import DiagnosticService
                
                diagnostic_service = DiagnosticService(
                    config.database,
                    export_dir=args.export_dir,
                    import_db=args.import_db,
                    tables=args.tables,
                    output_file=args.output_file
                )
                
                # Run diagnostics
                logger.info(f"Running diagnostics on tables: {args.tables or 'all'}")
                results = diagnostic_service.run_diagnostics()
                
                # Print results
                for table, result in results.items():
                    if result['has_discrepancy']:
                        logger.warning(f"Discrepancy in table {table}: {result['summary']}")
                        if 'details' in result and result['details']:
                            for detail in result['details']:
                                logger.info(f"  - {detail}")
                    else:
                        logger.info(f"No discrepancy in table {table}")
                        
                # Return non-zero exit code if any discrepancies were found
                if any(r['has_discrepancy'] for r in results.values()):
                    logger.error("Discrepancies found. See log for details.")
                    return 2
            except ImportError:
                logger.error("Diagnostic service not available. Please make sure the service is properly installed.")
                return 1
                
        else:
            print("Error: No command specified")
            return 1
            
        logger.info(f"{args.command.capitalize()} completed successfully")
        return 0
        
    except (ConfigError, DatabaseError, StorageError, ExportError, ImportError) as e:
        # Implementation: Error handling with appropriate error codes
        logger.error(f"Error: {str(e)}")
        return 1
        
    except Exception as e:
        # Implementation: Catch-all for unexpected errors
        logger.exception(f"Unexpected error: {str(e)}")
        return 2

if __name__ == "__main__":
    # Implementation: Entry point when run as script, returns appropriate exit code
    sys.exit(main())