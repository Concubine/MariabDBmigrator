"""Parallel worker implementation for concurrent operations."""
import multiprocessing
import logging
import concurrent.futures
import re
from typing import Callable, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from dataclasses import dataclass

from src.core.logging import get_logger

logger = get_logger(__name__)

@dataclass
class WorkerConfig:
    """Configuration for parallel workers."""
    max_workers: int
    use_processes: bool = False
    chunk_size: int = 1

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
            chunksize=self.config.chunk_size
        ))
        logger.debug("Parallel processing completed")
        return results

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