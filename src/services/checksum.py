import json
import hashlib
import os
from typing import List, Dict, Any
import logging

# Configure logger
logger = logging.getLogger(__name__)

def calculate_table_checksum(rows: List[Dict[str, Any]]) -> str:
    """
    Calculate a checksum for a table's data.
    
    Args:
        rows: List of dictionaries representing rows of data
        
    Returns:
        String checksum
    """
    # Convert rows to a consistent string representation
    data_str = json.dumps(rows, sort_keys=True)
    
    # Calculate MD5 hash
    return hashlib.md5(data_str.encode()).hexdigest()

def save_checksum(checksum: str, table_name: str, database_name: str, file_path: str) -> None:
    """
    Save a checksum to a file.
    
    Args:
        checksum: The calculated checksum
        table_name: Table name
        database_name: Database name
        file_path: Path to save the checksum file
    """
    checksum_data = {
        "database": database_name,
        "table": table_name,
        "checksum": checksum
    }
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Write to file
    with open(file_path, 'w') as f:
        json.dump(checksum_data, f, indent=2)
    
    logger.info(f"Checksum saved for {database_name}.{table_name}: {checksum}")

def verify_checksum(source_checksum_path: str, target_checksum_path: str) -> bool:
    """
    Verify that two checksum files match.
    
    Args:
        source_checksum_path: Path to source checksum file
        target_checksum_path: Path to target checksum file
        
    Returns:
        True if checksums match, False otherwise
    """
    try:
        # Load source checksum
        with open(source_checksum_path, 'r') as f:
            source_data = json.load(f)
            
        # Load target checksum
        with open(target_checksum_path, 'r') as f:
            target_data = json.load(f)
            
        # Compare checksums
        source_checksum = source_data.get('checksum')
        target_checksum = target_data.get('checksum')
        
        table_name = source_data.get('table', 'unknown')
        
        if source_checksum == target_checksum:
            logger.info(f"Checksum verification successful for table {table_name}")
            return True
        else:
            logger.warning(f"Checksum verification failed for table {table_name}. Source: {source_checksum}, Target: {target_checksum}")
            return False
    except Exception as e:
        logger.error(f"Error verifying checksums: {str(e)}")
        return False 