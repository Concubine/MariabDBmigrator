"""Main entry point for the MariaDB export tool."""
import argparse
import logging
from pathlib import Path
from typing import List, Optional

from .core.config import load_config
from .core.exceptions import ConfigError, ExportError, ImportError
from .domain.models import ExportOptions, ImportOptions
from .infrastructure.mariadb import MariaDB
from .infrastructure.storage import SQLStorage
from .services.export import ExportService
from .services.import_ import ImportService

def setup_logging(level: str = "INFO") -> None:
    """Set up logging configuration."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Export and import data from/to MariaDB using SQL format"
    )
    
    subparsers = parser.add_subparsers(dest="mode", help="Operation mode")
    
    # Export mode
    export_parser = subparsers.add_parser("export", help="Export data from MariaDB")
    export_parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Output directory for exported files"
    )
    export_parser.add_argument(
        "--tables",
        nargs="+",
        help="List of tables to export (default: all tables)"
    )
    export_parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of rows to export in each batch"
    )
    export_parser.add_argument(
        "--where",
        help="WHERE clause for filtering data"
    )
    export_parser.add_argument(
        "--no-schema",
        action="store_true",
        help="Do not export table schemas"
    )
    export_parser.add_argument(
        "--no-indexes",
        action="store_true",
        help="Do not export indexes"
    )
    export_parser.add_argument(
        "--no-constraints",
        action="store_true",
        help="Do not export constraints"
    )
    export_parser.add_argument(
        "--compress",
        action="store_true",
        help="Compress output files"
    )
    
    # Import mode
    import_parser = subparsers.add_parser("import", help="Import data into MariaDB")
    import_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Input directory containing SQL files"
    )
    import_parser.add_argument(
        "--tables",
        nargs="+",
        help="List of tables to import (default: all tables)"
    )
    import_parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of rows to import in each batch"
    )
    import_parser.add_argument(
        "--compress",
        action="store_true",
        help="Input files are compressed"
    )
    
    # Common options
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/config.yaml"),
        help="Path to configuration file"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    
    return parser.parse_args()

def main() -> None:
    """Main entry point."""
    try:
        # Parse arguments
        args = parse_args()
        
        # Set up logging
        setup_logging("DEBUG" if args.debug else "INFO")
        logger = logging.getLogger(__name__)
        
        # Load configuration
        config = load_config(args.config)
        
        # Create database connection
        database = MariaDB(config.database)
        
        # Create storage
        storage = SQLStorage()
        
        if args.mode == "export":
            # Create export options
            export_options = ExportOptions(
                batch_size=args.batch_size,
                where_clause=args.where,
                include_schema=not args.no_schema,
                include_indexes=not args.no_indexes,
                include_constraints=not args.no_constraints,
                compression=args.compress
            )
            
            # Create export service
            export_service = ExportService(database, storage)
            
            # Get tables to export
            tables = args.tables or database.get_table_names()
            
            # Export tables
            results = export_service.export_tables(
                tables,
                args.output_dir,
                export_options
            )
            
            # Print summary
            logger.info("Export completed successfully:")
            for result in results:
                logger.info(
                    f"Table {result.table_name}: {result.total_rows} rows"
                )
                if result.schema_file:
                    logger.info(f"Schema saved to {result.schema_file}")
                logger.info(f"Data saved to {result.data_file}")
        
        elif args.mode == "import":
            # Create import options
            import_options = ImportOptions(
                batch_size=args.batch_size,
                compression=args.compress
            )
            
            # Create import service
            import_service = ImportService(database, storage)
            
            # Get tables to import
            tables = args.tables or [
                f.stem for f in args.input.glob("*.sql")
                if not f.stem.endswith(".schema")
            ]
            
            # Import tables
            results = import_service.import_tables(
                tables,
                args.input,
                import_options
            )
            
            # Print summary
            logger.info("Import completed successfully:")
            for result in results:
                logger.info(
                    f"Table {result.table_name}: {result.total_rows} rows"
                )
        
        else:
            logger.error("No operation mode specified")
            return 1
    
    except ConfigError as e:
        logger.error(f"Configuration error: {str(e)}")
        return 1
    except ExportError as e:
        logger.error(f"Export error: {str(e)}")
        return 1
    except ImportError as e:
        logger.error(f"Import error: {str(e)}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 