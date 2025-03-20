"""File storage operations implementation."""
import gzip
from pathlib import Path
from typing import List, Dict, Any, Optional, Iterator
import logging
import sqlparse
from sqlparse.sql import TokenList, Token
import os
import shutil

from ..core.exceptions import StorageError
from ..domain.interfaces import StorageInterface
from ..domain.models import TableMetadata, ColumnMetadata

logger = logging.getLogger(__name__)

class SQLStorage(StorageInterface):
    """SQL file storage implementation."""
    
    # Define expected metadata fields with defaults
    EXPECTED_TABLE_METADATA = {
        'name': '',
        'columns': [],
        'primary_key': [],
        'foreign_keys': [],
        'indexes': [],
        'constraints': [],
        'schema': None,  # Only need schema field as it's used everywhere
    }
    
    def __init__(self):
        """Initialize the storage."""
        # SUGGESTION: Add option for file rotation/rollover for very large exports
        pass
    
    def save_data(
        self,
        data: List[Dict[str, Any]],
        file_path: Path,
        compression: bool = False
    ) -> None:
        """Save data to a SQL file."""
        try:
            # Create directory if it doesn't exist
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Generate SQL statements
            sql_statements = []
            for row in data:
                # Convert values to SQL format
                values = []
                for value in row.values():
                    if value is None:
                        values.append('NULL')
                    elif isinstance(value, (int, float)):
                        values.append(str(value))
                    else:
                        # Escape single quotes and wrap in quotes
                        value_str = str(value).replace("'", "''")
                        values.append(f"'{value_str}'")
                
                # Create INSERT statement
                columns = ', '.join(row.keys())
                values_str = ', '.join(values)
                sql = f"INSERT INTO {file_path.stem} ({columns}) VALUES ({values_str});"
                sql_statements.append(sql)
            
            # Write to file
            if compression:
                file_path = file_path.with_suffix(file_path.suffix + '.gz')
                with gzip.open(file_path, 'wt', encoding='utf-8') as f:
                    f.write('\n'.join(sql_statements))
            else:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(sql_statements))
                    
        except Exception as e:
            raise StorageError(f"Failed to save data to {file_path}: {str(e)}")
    
    def load_data(
        self,
        file_path: Path,
        compression: bool = False
    ) -> List[Dict[str, Any]]:
        """Load data from a SQL file."""
        try:
            # Read file content
            if compression:
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    content = f.read()
            else:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            
            # Parse SQL statements
            statements = sqlparse.parse(content)
            data = []
            
            for statement in statements:
                if not statement.is_valid:
                    continue
                
                # Extract table name and values
                tokens = statement.flatten()
                if not tokens:
                    continue
                
                # Find INSERT statement
                if tokens[0].ttype is sqlparse.tokens.DML and tokens[0].value.upper() == 'INSERT':
                    # Extract column names
                    columns_start = tokens.index(sqlparse.tokens.Punctuation('(')) + 1
                    columns_end = tokens.index(sqlparse.tokens.Punctuation(')'))
                    columns = [t.value.strip() for t in tokens[columns_start:columns_end]]
                    
                    # Extract values
                    values_start = tokens.index(sqlparse.tokens.Punctuation('('), columns_end) + 1
                    values_end = tokens.index(sqlparse.tokens.Punctuation(')'), values_start)
                    values = [t.value.strip() for t in tokens[values_start:values_end]]
                    
                    # Convert values to Python types
                    row = {}
                    for col, val in zip(columns, values):
                        if val.upper() == 'NULL':
                            row[col] = None
                        elif val.startswith("'") and val.endswith("'"):
                            row[col] = val[1:-1]
                        else:
                            try:
                                row[col] = int(val)
                            except ValueError:
                                try:
                                    row[col] = float(val)
                                except ValueError:
                                    row[col] = val
                    
                    data.append(row)
            
            return data
            
        except Exception as e:
            raise StorageError(f"Failed to load data from {file_path}: {str(e)}")
    
    def save_schema(
        self,
        schema: str,
        file_path: Path,
        compression: bool = False
    ) -> None:
        """Save table schema to a SQL file."""
        try:
            # Create directory if it doesn't exist
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write schema to file
            if compression:
                file_path = file_path.with_suffix(file_path.suffix + '.gz')
                with gzip.open(file_path, 'wt', encoding='utf-8') as f:
                    f.write(schema)
            else:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(schema)
                    
        except Exception as e:
            raise StorageError(f"Failed to save schema to {file_path}: {str(e)}")
    
    def load_schema(
        self,
        file_path: Path,
        compression: bool = False
    ) -> str:
        """Load table schema from a SQL file."""
        try:
            # Read schema from file
            if compression:
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    schema = f.read()
            else:
                with open(file_path, 'r', encoding='utf-8') as f:
                    schema = f.read()
            
            return schema
            
        except Exception as e:
            raise StorageError(f"Failed to load schema from {file_path}: {str(e)}")
    
    def write_schema(
        self,
        file_path: Path,
        metadata: TableMetadata
    ) -> None:
        """Write table schema to a SQL file.
        
        This is an alias for save_schema that accepts TableMetadata.
        
        Args:
            file_path: Path to save schema to
            metadata: Table metadata
        """
        try:
            # Create a complete metadata dictionary with all expected fields
            metadata_dict = {}
            
            # First populate from the metadata object attributes
            for field in self.EXPECTED_TABLE_METADATA:
                if hasattr(metadata, field):
                    metadata_dict[field] = getattr(metadata, field)
            
            # Handle missing fields
            missing_fields = [field for field in self.EXPECTED_TABLE_METADATA.keys() 
                            if field not in metadata_dict]
            if missing_fields:
                logger.debug(f"Missing expected metadata fields for table '{metadata_dict.get('name', 'unknown')}': {', '.join(missing_fields)}")
            
            # Add defaults for missing fields
            for field in missing_fields:
                metadata_dict[field] = self.EXPECTED_TABLE_METADATA[field]
                
            # For backward compatibility, handle definition field
            if hasattr(metadata, 'definition') and metadata.definition and not metadata_dict.get('schema'):
                metadata_dict['schema'] = metadata.definition
            
            # Use schema if available
            if metadata_dict.get('schema'):
                schema = metadata_dict['schema']
                self.save_schema(schema, file_path, compression=False)
                return
                
            # Otherwise, generate CREATE TABLE statement from metadata
            schema = f"CREATE TABLE {metadata_dict['name']} (\n"
            
            # Add columns
            column_defs = []
            for column in metadata_dict['columns']:
                column_defs.append(f"  {column}")
            
            # Add primary key if present
            if metadata_dict['primary_key']:
                pk_cols = ', '.join(metadata_dict['primary_key'])
                column_defs.append(f"  PRIMARY KEY ({pk_cols})")
                
            # Add foreign keys
            for fk in metadata_dict['foreign_keys']:
                fk_def = f"  FOREIGN KEY ({fk['column']}) REFERENCES {fk['ref_table']}({fk['ref_column']})"
                if 'on_delete' in fk:
                    fk_def += f" ON DELETE {fk['on_delete']}"
                if 'on_update' in fk:
                    fk_def += f" ON UPDATE {fk['on_update']}"
                column_defs.append(fk_def)
                
            # Add indexes
            for idx in metadata_dict['indexes']:
                idx_def = f"  INDEX {idx['name']} ({', '.join(idx['columns'])})"
                column_defs.append(idx_def)
                
            # Add constraints
            for constraint in metadata_dict['constraints']:
                constraint_def = f"  CONSTRAINT {constraint['name']} {constraint.get('definition', '')}"
                column_defs.append(constraint_def)
                
            schema += ',\n'.join(column_defs)
            schema += "\n);"
            
            # Save schema
            self.save_schema(schema, file_path, compression=False)
                
        except Exception as e:
            logger.error(f"Failed to write schema for {metadata.name}: {str(e)}", exc_info=True)
            raise StorageError(f"Failed to write schema: {str(e)}")
    
    def write_data(self, file_path: Path, data: str, append: bool = False) -> None:
        """Write data to a file.
        
        Args:
            file_path: Path to the file
            data: Data to write
            append: Whether to append to existing file
            
        Raises:
            StorageError: If writing fails
        """
        # SUGGESTION: Add option to include checksums for data validation
        try:
            mode = 'a' if append else 'w'
            file_path = Path(file_path)
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Write the data
            with open(file_path, mode, encoding='utf-8') as f:
                f.write(data)
                # Write a newline to ensure the file ends with one
                if data and not data.endswith('\n'):
                    f.write('\n')
        except Exception as e:
            raise StorageError(f"Failed to write data to {file_path}: {str(e)}")
    
    def read_file(self, file_path: Path, compression: bool = False) -> Iterator[str]:
        """Read a file line by line.
        
        Args:
            file_path: Path to the file
            compression: Whether the file is compressed
            
        Yields:
            Lines from the file
            
        Raises:
            StorageError: If reading fails
        """
        # SUGGESTION: Add support for resumable reading in case of interruption
        try:
            file_path = Path(file_path)
            
            if compression:
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    for line in f:
                        yield line
            else:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        yield line
        except Exception as e:
            raise StorageError(f"Failed to read file {file_path}: {str(e)}")
            
    def compress_file(self, file_path: Path, delete_original: bool = True) -> Path:
        """Compress a file using gzip.
        
        Args:
            file_path: Path to the file
            delete_original: Whether to delete the original file
            
        Returns:
            Path to the compressed file
            
        Raises:
            StorageError: If compression fails
        """
        # SUGGESTION: Add support for different compression algorithms (zstd, lz4, etc.)
        try:
            file_path = Path(file_path)
            compressed_path = file_path.with_suffix(file_path.suffix + '.gz')
            
            with open(file_path, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    
            if delete_original:
                os.remove(file_path)
                
            return compressed_path
        except Exception as e:
            raise StorageError(f"Failed to compress file {file_path}: {str(e)}")
            
    def decompress_file(self, file_path: Path, delete_original: bool = True) -> Path:
        """Decompress a gzip file.
        
        Args:
            file_path: Path to the file
            delete_original: Whether to delete the original file
            
        Returns:
            Path to the decompressed file
            
        Raises:
            StorageError: If decompression fails
        """
        # SUGGESTION: Add support for different compression algorithms (zstd, lz4, etc.)
        try:
            file_path = Path(file_path)
            
            if not file_path.name.endswith('.gz'):
                raise ValueError(f"File {file_path} is not a gzip file")
                
            decompressed_path = file_path.with_suffix('')
            
            with gzip.open(file_path, 'rb') as f_in:
                with open(decompressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    
            if delete_original:
                os.remove(file_path)
                
            return decompressed_path
        except Exception as e:
            raise StorageError(f"Failed to decompress file {file_path}: {str(e)}")
            
    def file_exists(self, file_path: Path) -> bool:
        """Check if a file exists.
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if the file exists, False otherwise
        """
        # SUGGESTION: Add file locking mechanism to prevent concurrent access
        return os.path.exists(file_path)
        
    def delete_file(self, file_path: Path) -> None:
        """Delete a file.
        
        Args:
            file_path: Path to the file
            
        Raises:
            StorageError: If deletion fails
        """
        # SUGGESTION: Add support for secure deletion with file shredding for sensitive data
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            raise StorageError(f"Failed to delete file {file_path}: {str(e)}")
            
    def list_files(self, directory: Path, pattern: str = "*") -> List[Path]:
        """List files in a directory matching a pattern.
        
        Args:
            directory: Directory to list files in
            pattern: Glob pattern for filtering files
            
        Returns:
            List of file paths
            
        Raises:
            StorageError: If listing fails
        """
        # SUGGESTION: Add support for recursive file listing with depth control
        try:
            return list(directory.glob(pattern))
        except Exception as e:
            raise StorageError(f"Failed to list files in {directory}: {str(e)}")
            
    def get_file_size(self, file_path: Path) -> int:
        """Get the size of a file in bytes.
        
        Args:
            file_path: Path to the file
            
        Returns:
            File size in bytes
            
        Raises:
            StorageError: If getting file size fails
        """
        # SUGGESTION: Add human-readable file size formatting utility
        try:
            return os.path.getsize(file_path)
        except Exception as e:
            raise StorageError(f"Failed to get size of file {file_path}: {str(e)}")
            
    def create_directory(self, directory: Path) -> None:
        """Create a directory if it doesn't exist.
        
        Args:
            directory: Directory to create
            
        Raises:
            StorageError: If creation fails
        """
        # SUGGESTION: Add support for setting directory permissions
        try:
            os.makedirs(directory, exist_ok=True)
        except Exception as e:
            raise StorageError(f"Failed to create directory {directory}: {str(e)}")
            
    def delete_directory(self, directory: Path, recursive: bool = False) -> None:
        """Delete a directory.
        
        Args:
            directory: Directory to delete
            recursive: Whether to delete recursively
            
        Raises:
            StorageError: If deletion fails
        """
        # SUGGESTION: Add dry-run option to see what would be deleted without actually deleting
        try:
            if recursive:
                shutil.rmtree(directory)
            else:
                os.rmdir(directory)
        except Exception as e:
            raise StorageError(f"Failed to delete directory {directory}: {str(e)}")
            
    def get_file_extension(self, file_path: Path) -> str:
        """Get the extension of a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            File extension
        """
        # SUGGESTION: Add support for handling multiple extensions (e.g., .tar.gz)
        return file_path.suffix
    
    def get_file_name(self, file_path: Path) -> str:
        """Get the name of a file without extension.
        
        Args:
            file_path: Path to the file
            
        Returns:
            File name without extension
        """
        # SUGGESTION: Add support for handling multiple extensions (e.g., .tar.gz)
        return file_path.stem
    
    def append_to_path(self, base_path: Path, *parts: str) -> Path:
        """Append parts to a base path.
        
        Args:
            base_path: Base path
            parts: Parts to append
            
        Returns:
            Resulting path
        """
        # SUGGESTION: Add validation for path traversal vulnerabilities
        return base_path.joinpath(*parts) 