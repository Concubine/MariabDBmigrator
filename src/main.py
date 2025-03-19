"""Main entry point for the MariaDB export tool."""
import argparse
import sys
from pathlib import Path
from typing import Optional
import logging

from src.core.config import load_config
from src.core.logging import setup_logging, log_config
from src.services.export import ExportService
from src.services.import_ import ImportService
from src.domain.models import ImportMode
from src.core.exceptions import ConfigError, DatabaseError, StorageError, ExportError, ImportError

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
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
    
    # Import command
    import_parser = subparsers.add_parser("import", help="Import database tables")
    import_parser.add_argument("files", nargs="*", help="SQL files to import (if not specified, all files in input-dir will be used)")
    import_parser.add_argument("--input-dir", help="Directory containing SQL files to import (default: same as export output-dir)")
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
    
    # Global options
    parser.add_argument("--config", help="Path to configuration file")
    
    args = parser.parse_args()
    
    # Show help if no command is specified
    if not args.command:
        parser.print_help()
        sys.exit(1)
        
    return args

def main() -> int:
    """Main entry point."""
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
            service = ExportService(config.database, config.export)
            service.export_data()
            
        elif args.command == "import":
            # Override config with command line arguments
            if args.input_dir:
                config.import_.input_dir = args.input_dir
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
            service = ImportService(config.database, import_files, config.import_)
            service.import_data()
            
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
        logging.error(f"Unexpected error: {str(e)}", exc_info=True)
        print(f"Error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())