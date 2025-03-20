"""Validation services for database metadata."""
import logging
from typing import Any, Dict, List, Optional

from ..domain.models import TableMetadata
from ..domain.interfaces import DatabaseInterface

logger = logging.getLogger(__name__)

class MetadataValidator:
    """Validates and completes metadata structures for database objects."""
    
    def __init__(self, db_connection: DatabaseInterface):
        """Initialize the validator with a database connection.
        
        Args:
            db_connection: Database interface for querying metadata
        """
        # Implementation: Store database connection for metadata retrieval
        self.db_connection = db_connection
    
    # Expected structure of TableMetadata with default values
    EXPECTED_TABLE_METADATA = {
        'name': '',
        'columns': [],
        'primary_key': [],
        'foreign_keys': [],
        'indexes': [],
        'constraints': [],
        'schema': None,
        'definition': '',  # This is the missing key that caused the original error
    }
    
    def validate_table_metadata(self, metadata: Dict[str, Any], table_name: Optional[str] = None) -> Dict[str, Any]:
        """Ensure metadata is complete and valid for a table.
        
        Args:
            metadata: The table metadata to validate
            table_name: The table name (used if not present in metadata)
            
        Returns:
            Dict[str, Any]: Complete and valid table metadata
        """
        # Implementation: Validate and complete table metadata with appropriate defaults
        if not metadata:
            metadata = {}
        
        # Ensure table name
        metadata['name'] = metadata.get('name', table_name)
        if not metadata['name']:
            raise ValueError("Table metadata must include a table name")
        
        # Log any missing expected fields for debugging
        missing_fields = [field for field in self.EXPECTED_TABLE_METADATA.keys() 
                         if field not in metadata]
        if missing_fields:
            logger.debug(f"Missing expected metadata fields for table '{metadata['name']}': {', '.join(missing_fields)}")
        
        # Create a copy with all expected fields
        complete_metadata = self.EXPECTED_TABLE_METADATA.copy()
        
        # Update with provided values
        complete_metadata.update(metadata)
        
        # Ensure definition exists
        # Implementation: If schema exists but definition doesn't, use schema as definition
        if not complete_metadata['definition'] and complete_metadata.get('schema'):
            complete_metadata['definition'] = complete_metadata['schema']
        
        # Enhance metadata with any missing but required elements
        # Implementation: Query database to fill in missing metadata details
        self._enhance_table_metadata(complete_metadata)
        
        return complete_metadata
    
    def _enhance_table_metadata(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure all required metadata fields are present and populated.
        
        Args:
            metadata: Partially complete metadata dictionary
            
        Returns:
            Enhanced metadata dictionary
        """
        # Implementation: Auto-discovery of missing metadata by querying database
        table_name = metadata['name']
        
        # Ensure definition exists
        # Implementation: Retrieve CREATE TABLE statement from database if missing
        if not metadata['definition'] or not metadata['schema']:
            try:
                create_statement = self.db_connection.execute_query(
                    f"SHOW CREATE TABLE `{table_name}`")
                if create_statement and len(create_statement) > 0:
                    # SHOW CREATE TABLE typically returns a tuple with (table_name, create_statement)
                    create_sql = create_statement[0].get('Create Table') if isinstance(create_statement[0], dict) else create_statement[0][1]
                    
                    # Update both fields
                    metadata['definition'] = create_sql
                    metadata['schema'] = create_sql
            except Exception as e:
                logger.warning(f"Could not retrieve CREATE statement for {table_name}: {e}")
        
        # Ensure columns exist
        # Implementation: Query column information if missing
        if not metadata['columns']:
            try:
                columns = self.db_connection.get_column_metadata(table_name)
                if columns:
                    # Extract column names from column metadata
                    metadata['columns'] = [col.name if hasattr(col, 'name') else col for col in columns]
            except Exception as e:
                logger.warning(f"Could not retrieve columns for {table_name}: {e}")
        
        # Ensure primary key exists
        # Implementation: Query primary key information if missing
        if not metadata['primary_key']:
            try:
                result = self.db_connection.execute_query(
                    f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = '{table_name}'
                    AND CONSTRAINT_NAME = 'PRIMARY'
                    """)
                if result:
                    metadata['primary_key'] = [row.get('COLUMN_NAME', row[0]) if isinstance(row, dict) else row[0] for row in result]
            except Exception as e:
                logger.warning(f"Could not retrieve primary key for {table_name}: {e}")
        
        # If we still have no definition but have columns, attempt to create a simple CREATE TABLE statement
        # Implementation: Generate synthetic CREATE TABLE statement from column information
        if not metadata['definition'] and metadata['columns']:
            try:
                column_info = self.db_connection.execute_query(
                    f"SHOW COLUMNS FROM `{table_name}`")
                if column_info:
                    # Create basic definition
                    col_defs = []
                    for col in column_info:
                        col_name = col.get('Field', '') if isinstance(col, dict) else col[0]
                        col_type = col.get('Type', '') if isinstance(col, dict) else col[1]
                        nullable = col.get('Null', 'YES') if isinstance(col, dict) else col[2]
                        null_str = "" if nullable == 'YES' else " NOT NULL"
                        default = col.get('Default', None) if isinstance(col, dict) else col[3]
                        default_str = f" DEFAULT {default}" if default is not None else ""
                        extra = col.get('Extra', '') if isinstance(col, dict) else col[5]
                        extra_str = f" {extra}" if extra else ""
                        
                        col_defs.append(f"`{col_name}` {col_type}{null_str}{default_str}{extra_str}")
                    
                    # Add primary key if we have it
                    if metadata['primary_key']:
                        pk_cols = ', '.join([f"`{col}`" for col in metadata['primary_key']])
                        col_defs.append(f"PRIMARY KEY ({pk_cols})")
                    
                    # Construct CREATE TABLE statement
                    create_sql = f"CREATE TABLE `{table_name}` (\n  " + ",\n  ".join(col_defs) + "\n);"
                    metadata['definition'] = create_sql
                    metadata['schema'] = create_sql
            except Exception as e:
                logger.warning(f"Could not construct CREATE TABLE statement for {table_name}: {e}")
        
        return metadata
    
    def convert_to_table_metadata(self, metadata: Dict[str, Any]) -> TableMetadata:
        """Convert dictionary metadata to TableMetadata object.
        
        Args:
            metadata: Dictionary containing table metadata
            
        Returns:
            TableMetadata object
        """
        # Implementation: Convert dictionary representation to domain model object
        validated_metadata = self.validate_table_metadata(metadata)
        
        # Construct TableMetadata object
        return TableMetadata(
            name=validated_metadata['name'],
            columns=validated_metadata['columns'],
            primary_key=validated_metadata['primary_key'],
            foreign_keys=validated_metadata['foreign_keys'],
            indexes=validated_metadata['indexes'],
            constraints=validated_metadata['constraints'],
            schema=validated_metadata['schema'],
            definition=validated_metadata['definition']
        ) 