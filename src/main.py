"""Main entry point for the MariaDB export tool."""
import argparse
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional

# Add the root directory to sys.path to ensure imports work in both
# development and when bundled as an executable
root_dir = Path(__file__).parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from src.core.config import load_config
from src.core.exceptions import ConfigError, ExportError, ImportError
from src.domain.models import ExportOptions, ImportOptions, ImportMode, DatabaseConfig
from src.infrastructure.mariadb import MariaDB
from src.infrastructure.storage import SQLStorage
from src.services.export import ExportService
from src.services.import_ import ImportService

logger = logging.getLogger(__name__)

def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    """Set up logging configuration."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        filename=log_file
    )

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="MariaDB Export Tool")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Export command
    export_parser = subparsers.add_parser("export", help="Export data from MariaDB")
    export_parser.add_argument(
        "--output-dir",
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
    export_parser.add_argument(
        "--parallel-workers",
        type=int,
        help="Number of parallel workers for export"
    )
    
    return parser.parse_args()

def main() -> None:
    """Main entry point."""
    try:
        # Parse command line arguments
        args = parse_args()
        if not args.command:
            print("No command specified. Use --help for usage information.")
            sys.exit(1)
        
        # Load configuration
        config = load_config()
        
        # Set up logging
        setup_logging(
            level=config.logging.level,
            log_file=config.logging.file if config.logging.file else None
        )
        
        # Create database config
        db_config = DatabaseConfig(
            host=config.database.host,
            port=config.database.port,
            user=config.database.user,
            password=config.database.password,
            database=config.database.database,
            use_pure=True
        )
        
        if args.command == "export":
            # Create export service
            service = ExportService(
                config=db_config,
                output_dir=args.output_dir or config.export.output_dir,
                tables=args.tables,
                batch_size=args.batch_size or config.export.batch_size,
                where_clause=args.where,
                exclude_schema=args.no_schema,
                exclude_indexes=args.no_indexes,
                exclude_constraints=args.no_constraints,
                compress=args.compress or config.export.compression,
                parallel_workers=args.parallel_workers or config.export.parallel_workers
            )
            
            # Perform export
            service.export()
            
    except ConfigError as e:
        logger.error(f"Configuration error: {str(e)}")
        sys.exit(1)
    except ExportError as e:
        logger.error(f"Export error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()