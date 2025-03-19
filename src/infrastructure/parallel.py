"""Parallel worker implementation for concurrent operations."""
import multiprocessing
import logging
import concurrent.futures
import re
from typing import Callable, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from src.core.logging import get_logger
from src.infrastructure.mariadb import MariaDB

logger = get_logger(__name__)

@dataclass
class WorkerConfig:
    """Configuration for parallel workers."""
    max_workers: int
    use_processes: bool = False
    batch_size: int = 1000

class ParallelWorker:
    """Manages parallel execution of tasks using either threads or processes."""
    
    def __init__(self, config: WorkerConfig):
        """Initialize the parallel worker.
        
        Args:
            config: Worker configuration
        """
        self.config = config
        self._executor = None
    
    def __enter__(self):
        """Context manager entry."""
        if self.config.use_processes:
            self._executor = ProcessPoolExecutor(max_workers=self.config.max_workers)
        else:
            self._executor = ThreadPoolExecutor(max_workers=self.config.max_workers)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self._executor:
            self._executor.shutdown(wait=True)
    
    def map(self, func: Callable, items: List[Any]) -> List[Any]:
        """Map a function over items in parallel.
        
        Args:
            func: Function to apply to each item
            items: List of items to process
            
        Returns:
            List of results
        """
        if not self._executor:
            raise RuntimeError("ParallelWorker must be used as a context manager")
            
        logger.debug(f"Starting parallel processing with {self.config.max_workers} workers")
        results = list(self._executor.map(
            func,
            items,
            chunksize=self.config.batch_size
        ))
        logger.debug("Parallel processing completed")
        return results
        
    def process_table(self, table: str, database: Any, input_file: Path, disable_foreign_keys: bool = False) -> int:
        """Process a table by importing data from a SQL file.
        
        Args:
            table: Name of the table to import
            database: Database configuration 
            input_file: Path to the SQL file
            disable_foreign_keys: Whether to disable foreign key checks
            
        Returns:
            Number of rows imported
        """
        logger.info(f"Processing table {table} from {input_file}")
        
        if not input_file.exists():
            logger.error(f"Input file not found: {input_file}")
            return 0
            
        # Create a new database connection for this process
        db = MariaDB(database)
        
        try:
            db.connect()
            
            if disable_foreign_keys:
                db.execute("SET FOREIGN_KEY_CHECKS=0")
                
            # Import data from file
            total_rows = db.import_table_data(table, str(input_file), self.config.batch_size)
            
            logger.info(f"Imported {total_rows} rows into table {table}")
            return total_rows
            
        except Exception as e:
            logger.error(f"Error processing table {table}: {str(e)}")
            raise
            
        finally:
            if disable_foreign_keys:
                db.execute("SET FOREIGN_KEY_CHECKS=1")
            db.disconnect()

def get_optimal_worker_count(use_processes: bool = False) -> int:
    """Calculate optimal number of workers based on system resources.
    
    Args:
        use_processes: Whether to use processes instead of threads
        
    Returns:
        Number of workers to use
    """
    cpu_count = multiprocessing.cpu_count()
    
    if use_processes:
        # For CPU-bound tasks, use number of CPU cores
        return cpu_count
    else:
        # For I/O-bound tasks, use more workers than CPU cores
        return min(cpu_count * 2, 32)  # Cap at 32 threads
    
def parse_worker_count(value: str) -> int:
    """Parse worker count from configuration string.
    
    Args:
        value: Configuration value (e.g., "4", "$MAX_CPU_THREADS/2")
        
    Returns:
        Number of workers
    """
    if not value:
        return get_optimal_worker_count()
        
    if value.startswith("$MAX_CPU_THREADS"):
        # Handle expressions like $MAX_CPU_THREADS/2
        try:
            expr = value.replace("$MAX_CPU_THREADS", str(multiprocessing.cpu_count()))
            return max(1, int(eval(expr)))
        except (SyntaxError, ValueError):
            logger.warning(f"Invalid worker count expression: {value}")
            return get_optimal_worker_count()
    
    try:
        return max(1, int(value))
    except ValueError:
        logger.warning(f"Invalid worker count: {value}")
        return get_optimal_worker_count() 