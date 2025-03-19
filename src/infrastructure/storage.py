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
                
            # If we have schema but no definition, use schema as definition
            if metadata_dict.get('schema') and not metadata_dict.get('definition'):
                metadata_dict['definition'] = metadata_dict['schema']
                
            # If we have definition but no schema, use definition as schema
            if metadata_dict.get('definition') and not metadata_dict.get('schema'):
                metadata_dict['schema'] = metadata_dict['definition']
            
            # Use direct schema or definition if available
            if metadata_dict.get('schema') or metadata_dict.get('definition'):
                schema = metadata_dict.get('schema') or metadata_dict.get('definition')
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