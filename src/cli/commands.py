"""Command-line interface commands."""
import os
import sys
import json
from pathlib import Path
from datetime import datetime

from ..core.logging import get_logger
from ..infrastructure.mariadb import MariaDB
from ..services.export import ExportService
from ..services.import_ import ImportService
from ..ui.ascii import ASCIIInterface

def import_command(args, config):
    """Import data from SQL files."""
    logger = get_logger()
    
    # Create UI interface
    ascii_ui = ASCIIInterface(quiet=args.quiet, verbose=args.verbose, logger=logger)
    
    try:
        # Collect SQL files to import
        if args.file:
            files = [args.file]
        elif args.directory:
            directory = Path(args.directory)
            if not directory.exists():
                logger.error(f"Directory not found: {directory}")
                return 1
                
            files = list(directory.glob('*.sql'))
            if not files:
                logger.error(f"No SQL files found in directory: {directory}")
                return 1
                
            logger.info(f"Found {len(files)} SQL files to import")
        else:
            logger.error("No file or directory specified for import")
            return 1
            
        # Determine import mode
        import_mode = args.mode
        if not import_mode:
            # Default to CANCEL mode
            import_mode = "CANCEL"
            
        logger.info(f"Import mode: {import_mode}")
        
        # Create connection to MariaDB
        mariadb = MariaDB(config.database_settings)
        
        # Create ImportService
        import_service = ImportService(mariadb, ascii_ui, logger)
        
        # Import data
        import_service.import_data([str(f) for f in files], import_mode)
        
        return 0
        
    except Exception as e:
        logger.error(f"Error importing data: {str(e)}")
        return 1 