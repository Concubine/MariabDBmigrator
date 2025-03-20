"""Checksum utilities for data integrity verification."""
import hashlib
import json
from typing import List, Dict, Any, Optional, Union


def calculate_row_checksum(row: Dict[str, Any]) -> str:
    """Calculate checksum for a single row of data.
    
    Args:
        row: A dictionary representing a row of data
        
    Returns:
        String representation of the checksum
    """
    # Sort keys for consistent serialization
    ordered_row = {k: row[k] for k in sorted(row.keys())}
    
    # Serialize to JSON string for consistent representation
    row_string = json.dumps(ordered_row, sort_keys=True, default=str)
    
    # Generate SHA-256 hash
    return hashlib.sha256(row_string.encode('utf-8')).hexdigest()


def calculate_table_checksum(rows: List[Dict[str, Any]]) -> str:
    """Calculate checksum for all rows in a table.
    
    Args:
        rows: List of dictionaries representing table data
        
    Returns:
        String representation of the checksum
    """
    # Calculate checksum for each row
    row_checksums = [calculate_row_checksum(row) for row in rows]
    
    # Concatenate all row checksums and hash the result
    combined = ''.join(row_checksums)
    return hashlib.sha256(combined.encode('utf-8')).hexdigest()


def save_checksum(checksum: str, table_name: str, database_name: str, file_path: str) -> None:
    """Save a checksum to a file.
    
    Args:
        checksum: The calculated checksum
        table_name: Name of the table
        database_name: Name of the database
        file_path: Path to save the checksum
    """
    checksum_data = {
        "database": database_name,
        "table": table_name,
        "checksum": checksum,
        "version": "1.0"  # For future compatibility
    }
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(checksum_data, f, indent=2)


def load_checksum(file_path: str) -> Optional[Dict[str, str]]:
    """Load a checksum from a file.
    
    Args:
        file_path: Path to the checksum file
        
    Returns:
        Dictionary containing checksum data or None if file doesn't exist
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def verify_checksum(
    calculated_checksum: str, 
    expected_checksum: Union[str, Dict[str, str]]
) -> bool:
    """Verify that a calculated checksum matches an expected checksum.
    
    Args:
        calculated_checksum: The checksum calculated from current data
        expected_checksum: The expected checksum or checksum data dictionary
        
    Returns:
        True if checksums match, False otherwise
    """
    if isinstance(expected_checksum, dict):
        expected = expected_checksum.get("checksum")
    else:
        expected = expected_checksum
        
    return calculated_checksum == expected 