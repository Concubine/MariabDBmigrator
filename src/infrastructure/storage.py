"""File storage operations implementation."""
import gzip
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging
import sqlparse
from sqlparse.sql import TokenList, Token

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
        'schema': None,
        'definition': '',  # This is the field that was missing
    }
    
    def save_data(
        self,
        data: List[Dict[str, Any]],
        file_path: Path,
        compression: bool = False
    ) -> None:
        """Save data to a SQL file.
        
        Args:
            data: List of data rows as dictionaries
            file_path: Path to save the file
            compression: Whether to compress the file
        """
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
        """Load data from a SQL file.
        
        Args:
            file_path: Path to the file
            compression: Whether the file is compressed
            
        Returns:
            List of data rows as dictionaries
        """
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
        """Save table schema to a SQL file.
        
        Args:
            schema: SQL schema definition
            file_path: Path to save the file
            compression: Whether to compress the file
        """
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
        """Load table schema from a SQL file.
        
        Args:
            file_path: Path to the file
            compression: Whether the file is compressed
            
        Returns:
            SQL schema definition
        """
        try:
            # Read file content
            if compression:
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    content = f.read()
            else:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            
            return content
            
        except Exception as e:
            raise StorageError(f"Failed to load schema from {file_path}: {str(e)}")
    
    def write_schema(
        self,
        file_path: Path,
        metadata: TableMetadata
    ) -> None:
        """Write table metadata to a schema file.
        
        Args:
            file_path: Path to save the file
            metadata: Table metadata
        """
        try:
            # Create directory if it doesn't exist
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Start with table creation statement
            schema = []
            if metadata.schema:
                schema.append(f"-- Table structure for table `{metadata.name}`")
                schema.append(metadata.schema + ";")
                schema.append("")
            
            # Add indexes if not included in schema
            schema.append(f"-- Indexes for table `{metadata.name}`")
            for index in metadata.indexes:
                # Format index definition
                index_name = index.get('name', f"idx_{metadata.name}_{len(schema)}")
                columns = ', '.join([f"`{col}`" for col in index.get('columns', [])])
                schema.append(f"CREATE INDEX `{index_name}` ON `{metadata.name}` ({columns});")
            schema.append("")
            
            # Add constraints if not included in schema
            schema.append(f"-- Constraints for table `{metadata.name}`")
            for constraint in metadata.constraints:
                if constraint.get('type') == 'UNIQUE':
                    # Format unique constraint
                    constraint_name = constraint.get('name', f"unique_{metadata.name}_{len(schema)}")
                    columns = ', '.join([f"`{col}`" for col in constraint.get('columns', [])])
                    schema.append(f"ALTER TABLE `{metadata.name}` ADD CONSTRAINT `{constraint_name}` UNIQUE ({columns});")
                    
            for fk in metadata.foreign_keys:
                # Format foreign key constraint
                fk_name = fk.get('name', f"fk_{metadata.name}_{len(schema)}")
                column = fk.get('column', '')
                ref_table = fk.get('ref_table', '')
                ref_column = fk.get('ref_column', '')
                if column and ref_table and ref_column:
                    schema.append(f"ALTER TABLE `{metadata.name}` ADD CONSTRAINT `{fk_name}` " +
                                f"FOREIGN KEY (`{column}`) REFERENCES `{ref_table}` (`{ref_column}`);")
            
            # Write to file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(schema))
                
        except Exception as e:
            raise StorageError(f"Failed to write schema to {file_path}: {str(e)}")
    
    def load_metadata(
        self,
        file_path: Path
    ) -> TableMetadata:
        """Load table metadata from a schema file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Table metadata
        """
        # Implementation: Parse schema file to extract table metadata
        # ... existing code ... 