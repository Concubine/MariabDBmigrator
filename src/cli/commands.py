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
    
    try:
        # Collect SQL files to import
        files = []
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
            
        # Convert Path objects to strings
        files = [str(f) for f in files]
            
        # Determine import mode
        import_mode = args.mode
        if not import_mode:
            # Default to CANCEL mode
            import_mode = "CANCEL"
            
        logger.info(f"Import mode: {import_mode}")
        
        # Create connection to MariaDB
        mariadb = MariaDB(config.database_settings)
        
        # Create storage service
        from ..infrastructure.storage import SQLStorage
        storage_service = SQLStorage()
        
        # Configure import settings
        from ..config import ImportConfig
        import_config = ImportConfig(
            mode=import_mode,
            continue_on_error=args.continue_on_error if hasattr(args, 'continue_on_error') else False,
            import_triggers=args.import_triggers if hasattr(args, 'import_triggers') else True,
            import_procedures=args.import_procedures if hasattr(args, 'import_procedures') else True,
            import_views=args.import_views if hasattr(args, 'import_views') else True,
            import_events=args.import_events if hasattr(args, 'import_events') else True,
            import_functions=args.import_functions if hasattr(args, 'import_functions') else True,
            import_user_types=args.import_user_types if hasattr(args, 'import_user_types') else True
        )
        
        # Create ImportService
        import_service = ImportService(
            mariadb=mariadb,
            storage_service=storage_service,
            files=files,
            config=import_config
        )
        
        # Import data
        results = import_service.import_data()
        
        # Count successes and failures
        success_count = sum(1 for r in results if r.status == "success")
        warning_count = sum(1 for r in results if r.status == "warning")
        error_count = sum(1 for r in results if r.status == "error")
        
        # Print summary
        print(f"\nImport Summary:")
        print(f"Files processed: {len(results)}")
        print(f"Successful: {success_count}")
        print(f"Warnings: {warning_count}")
        print(f"Errors: {error_count}")
        
        # Return success if at least one file was imported
        return 0 if success_count > 0 else 1
        
    except Exception as e:
        logger.error(f"Error importing data: {str(e)}")
        return 1 