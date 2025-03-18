"""Import service implementation."""
from pathlib import Path
from typing import List, Dict, Any, Optional
import time

from ..core.exceptions import ImportError
from ..domain.interfaces import DatabaseInterface, StorageInterface, ProgressInterface
from ..domain.models import ImportOptions, ImportResult

class ImportService:
    """Service for importing data into MariaDB."""
    
    def __init__(
        self,
        database: DatabaseInterface,
        storage: StorageInterface,
        progress: ProgressInterface
    ):
        """Initialize the import service."""
        self.database = database
        self.storage = storage
        self.progress = progress
    
    def import_file(
        self,
        file_path: Path,
        table_name: Optional[str] = None,
        options: Optional[ImportOptions] = None
    ) -> ImportResult:
        """Import data from a SQL file into a table."""
        try:
            # Verify file extension
            if not file_path.suffix.lower() == '.sql':
                raise ImportError(f"Unsupported file format: {file_path.suffix}. Only SQL files are supported.")
            
            # If table name not provided, use file name without extension
            if not table_name:
                table_name = file_path.stem
            
            # Load data from file
            data = self.storage.load_data(
                file_path=file_path,
                format='sql',
                compression=file_path.suffix.endswith('.gz')
            )
            
            if not data:
                return ImportResult(
                    table_name=table_name,
                    rows_imported=0,
                    start_time=time.time(),
                    end_time=time.time()
                )
            
            # Initialize progress
            self.progress.initialize(len(data))
            
            # Start import
            start_time = time.time()
            rows_imported = 0
            
            # Process in batches
            batch_size = options.batch_size if options else 1000
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                
                # Insert batch
                self.database.insert_batch(table_name, batch)
                
                # Update progress
                rows_imported += len(batch)
                self.progress.update(rows_imported)
            
            end_time = time.time()
            
            return ImportResult(
                table_name=table_name,
                rows_imported=rows_imported,
                start_time=start_time,
                end_time=end_time
            )
            
        except Exception as e:
            raise ImportError(f"Failed to import file {file_path}: {str(e)}")
    
    def import_directory(
        self,
        directory: Path,
        options: Optional[ImportOptions] = None
    ) -> List[ImportResult]:
        """Import all SQL files in a directory."""
        results = []
        
        # Get all SQL files (excluding schema files)
        data_files = [
            f for f in directory.glob("*.sql")
            if f.is_file() and not f.name.endswith('.schema.sql')
        ]
        
        for file_path in data_files:
            try:
                # Determine table name from file name
                table_name = file_path.stem
                
                # Import file
                result = self.import_file(
                    file_path=file_path,
                    table_name=table_name,
                    options=options
                )
                results.append(result)
                
            except ImportError as e:
                # Log error but continue with other files
                print(f"Error importing file {file_path}: {str(e)}")
                continue
        
        return results
    
    def import_schema(
        self,
        schema_file: Path
    ) -> None:
        """Import table schema from a SQL file."""
        try:
            # Read schema SQL
            with open(schema_file, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            # Execute schema SQL
            self.database.execute_query(schema_sql)
            
        except Exception as e:
            raise ImportError(f"Failed to import schema from {schema_file}: {str(e)}")
    
    def import_with_schema(
        self,
        data_file: Path,
        schema_file: Optional[Path] = None,
        table_name: Optional[str] = None,
        options: Optional[ImportOptions] = None
    ) -> ImportResult:
        """Import data with optional schema."""
        try:
            # Import schema if provided
            if schema_file and schema_file.exists():
                self.import_schema(schema_file)
            
            # Import data
            return self.import_file(
                file_path=data_file,
                table_name=table_name,
                options=options
            )
            
        except Exception as e:
            raise ImportError(f"Failed to import with schema: {str(e)}") 