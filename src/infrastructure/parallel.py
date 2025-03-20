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
    # Implementation: Parameters for controlling parallel execution
    max_workers: int  # Maximum number of workers (threads/processes)
    use_processes: bool = False  # Whether to use processes instead of threads
    batch_size: int = 1000  # Number of items per batch for chunking work

class ParallelWorker:
    """Manages parallel execution of tasks using either threads or processes."""
    
    def __init__(self, config: WorkerConfig):
        """Initialize the parallel worker.
        
        Args:
            config: Worker configuration
        """
        # Implementation: Initialize worker with configurable thread/process pool options
        self.config = config
        self._executor = None
    
    def __enter__(self):
        """Context manager entry."""
        # Implementation: Choose appropriate executor based on configuration
        if self.config.use_processes:
            # Use process-based parallelism for CPU-bound tasks
            self._executor = ProcessPoolExecutor(max_workers=self.config.max_workers)
        else:
            # Use thread-based parallelism for I/O-bound tasks (default)
            self._executor = ThreadPoolExecutor(max_workers=self.config.max_workers)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        # Implementation: Clean up executor resources
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
        # Implementation: Apply function to all items in parallel with optimized chunking
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
        # Implementation: Database-specific parallel task processing
        logger.info(f"Processing table {table} from {input_file}")
        
        if not input_file.exists():
            logger.error(f"Input file not found: {input_file}")
            return 0
            
        # Create a new database connection for this process
        # Implementation: Each worker gets its own database connection
        db = MariaDB(database)
        
        try:
            db.connect()
            
            if disable_foreign_keys:
                # Implementation: Temporarily disable foreign keys for faster imports
                db.execute("SET FOREIGN_KEY_CHECKS=0")
                
            # Import data from file
            total_rows = db.import_table_data(table, str(input_file), self.config.batch_size)
            
            logger.info(f"Imported {total_rows} rows into table {table}")
            return total_rows
            
        except Exception as e:
            logger.error(f"Error processing table {table}: {str(e)}")
            raise
            
        finally:
            # Implementation: Ensure database state is restored properly
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
    # Implementation: Auto-detect optimal parallelism level based on system resources
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
    # Implementation: Support variable expressions for worker count configuration
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