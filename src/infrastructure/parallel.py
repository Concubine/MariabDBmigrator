"""Parallel processing utilities for MariaDB export tool."""
import logging
import time
import os
import threading
import queue
import traceback
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Callable, Union, Tuple
from dataclasses import dataclass

from src.core.logging import get_logger

# SUGGESTION: Add support for process-based parallelism for CPU-bound tasks
# SUGGESTION: Implement an async version using asyncio for IO-bound operations
# SUGGESTION: Add resource monitoring to dynamically adjust worker count based on system load

logger = get_logger(__name__)

def parse_worker_count(worker_count: Union[str, int]) -> int:
    """Parse worker count setting from configuration.
    
    Supports:
    - Integer value (e.g., "4")
    - Percentage of available CPUs (e.g., "50%")
    - "auto" for default of min(4, cpu_count)
    
    Args:
        worker_count: Worker count value to parse
        
    Returns:
        Number of workers to use
    """
    cpu_count = multiprocessing.cpu_count()
    default_workers = min(4, cpu_count)
    
    if not worker_count:
        return default_workers
        
    if isinstance(worker_count, int):
        return max(1, worker_count)
        
    worker_str = str(worker_count).strip().lower()
    
    # Check for percentage
    if worker_str.endswith('%'):
        try:
            percentage = float(worker_str[:-1])
            return max(1, int(cpu_count * percentage / 100))
        except ValueError:
            logger.warning(f"Invalid worker count percentage: {worker_str}, using default: {default_workers}")
            return default_workers
            
    # Check for 'auto'
    if worker_str == 'auto':
        return default_workers
        
    # Try integer value
    try:
        return max(1, int(worker_str))
    except ValueError:
        logger.warning(f"Invalid worker count: {worker_str}, using default: {default_workers}")
        return default_workers

@dataclass
class WorkerConfig:
    """Configuration for parallel workers."""
    num_workers: int = 4
    max_queue_size: int = 1000
    batch_size: int = 100
    timeout: int = 30
    retry_count: int = 3
    retry_delay: int = 5
    # SUGGESTION: Add support for priority queues to process critical tasks first
    # SUGGESTION: Add graceful shutdown mechanism with timeout

class ParallelWorker:
    """Worker for parallel processing of tasks."""
    
    def __init__(self, config: WorkerConfig):
        """Initialize the worker.
        
        Args:
            config: Worker configuration
        """
        self.config = config
        self.queue = queue.Queue(maxsize=config.max_queue_size)
        self.results = []
        self.errors = []
        self.workers = []
        self.stop_event = threading.Event()
        # SUGGESTION: Add progress tracking and reporting capability
        # SUGGESTION: Implement work stealing for better load balancing
        
    def __enter__(self):
        """Enter the context manager."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager."""
        self.stop()
        return False  # Don't suppress exceptions
        
    def map(self, func: Callable[[Any], Any], items: List[Any]) -> List[Any]:
        """Map a function over a list of items.
        
        Args:
            func: Function to apply to each item
            items: Items to process
            
        Returns:
            List of results
        """
        self.add_tasks(items)
        return self.process(func)
    
    def add_task(self, task: Any) -> None:
        """Add a task to the queue.
        
        Args:
            task: Task to add
        """
        try:
            self.queue.put(task, block=True, timeout=self.config.timeout)
        except queue.Full:
            logger.warning(f"Task queue is full, task will be skipped: {task}")
            # SUGGESTION: Implement backpressure mechanism or buffering for full queues
            
    def add_tasks(self, tasks: List[Any]) -> None:
        """Add multiple tasks to the queue.
        
        Args:
            tasks: Tasks to add
        """
        for task in tasks:
            self.add_task(task)
            
    def process(self, worker_func: Callable[[Any], Any]) -> List[Any]:
        """Process tasks in parallel.
        
        Args:
            worker_func: Function to process each task
            
        Returns:
            List of results
        """
        # SUGGESTION: Add support for worker initialization function
        # Create worker threads
        for i in range(self.config.num_workers):
            worker = threading.Thread(
                target=self._worker_thread,
                args=(worker_func, i),
                daemon=True,
                name=f"Worker-{i}"
            )
            self.workers.append(worker)
            worker.start()
            
        # Wait for all tasks to be processed
        try:
            self.queue.join()
            return self.results
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, stopping workers")
            self.stop()
            return self.results
        finally:
            self.stop()
            
    def stop(self) -> None:
        """Stop all workers."""
        self.stop_event.set()
        
        # Clear the queue to prevent workers from blocking on get()
        try:
            while True:
                self.queue.get_nowait()
                self.queue.task_done()
        except queue.Empty:
            pass
            
        # Wait for workers to finish
        for worker in self.workers:
            if worker.is_alive():
                worker.join(timeout=1)
                
        if self.errors:
            logger.warning(f"Encountered {len(self.errors)} errors during parallel processing")
            # SUGGESTION: Implement more detailed error reporting and aggregation
            
    def _worker_thread(self, worker_func: Callable[[Any], Any], worker_id: int) -> None:
        """Worker thread function.
        
        Args:
            worker_func: Function to process each task
            worker_id: Worker ID
        """
        logger.debug(f"Worker {worker_id} started")
        
        while not self.stop_event.is_set():
            try:
                # Get a task from the queue
                task = self.queue.get(block=True, timeout=1)
                
                # Process the task
                try:
                    for attempt in range(self.config.retry_count):
                        try:
                            result = worker_func(task)
                            self.results.append(result)
                            break
                        except Exception as e:
                            if attempt < self.config.retry_count - 1:
                                logger.warning(f"Worker {worker_id} failed to process task (attempt {attempt+1}/{self.config.retry_count}): {str(e)}")
                                time.sleep(self.config.retry_delay)
                            else:
                                logger.error(f"Worker {worker_id} failed to process task after {self.config.retry_count} attempts: {str(e)}")
                                self.errors.append((task, str(e), traceback.format_exc()))
                                raise
                except Exception as e:
                    # Log the error but continue processing other tasks
                    logger.error(f"Worker {worker_id} encountered an error: {str(e)}")
                    # Don't re-raise the exception to allow the worker to continue
                    # SUGGESTION: Add option to fail fast on first error
                
                # Mark the task as done
                self.queue.task_done()
                
            except queue.Empty:
                # No tasks available, just continue
                continue
            except Exception as e:
                # Log any other unexpected errors
                logger.error(f"Worker {worker_id} encountered an unexpected error: {str(e)}")
                # SUGGESTION: Add circuit breaker pattern to pause workers on persistent failures
        
        logger.debug(f"Worker {worker_id} stopped")
        
def process_in_parallel(
    items: List[Any],
    processor_func: Callable[[Any], Any],
    num_workers: int = 4,
    batch_size: int = 100,
    timeout: int = 30,
    retry_count: int = 3,
    retry_delay: int = 5
) -> List[Any]:
    """Process items in parallel.
    
    Args:
        items: Items to process
        processor_func: Function to process each item
        num_workers: Number of worker threads
        batch_size: Size of each batch
        timeout: Timeout for queue operations
        retry_count: Number of retries for failed tasks
        retry_delay: Delay between retries in seconds
        
    Returns:
        List of results
    """
    # SUGGESTION: Add option to return a generator of results instead of collecting all
    # SUGGESTION: Add progress reporting callback
    
    config = WorkerConfig(
        num_workers=num_workers,
        max_queue_size=num_workers * 2,  # Allow a small buffer
        batch_size=batch_size,
        timeout=timeout,
        retry_count=retry_count,
        retry_delay=retry_delay
    )
    
    worker = ParallelWorker(config)
    worker.add_tasks(items)
    return worker.process(processor_func)
    
def process_with_threadpool(
    items: List[Any],
    processor_func: Callable[[Any], Any],
    num_workers: int = 4
) -> List[Any]:
    """Process items using a thread pool executor.
    
    Args:
        items: Items to process
        processor_func: Function to process each item
        num_workers: Number of worker threads
        
    Returns:
        List of results
    """
    # SUGGESTION: Add progress reporting capability and timeout options
    results = []
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit all tasks
        future_to_item = {executor.submit(processor_func, item): item for item in items}
        
        # Process results as they complete
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Error processing {item}: {str(e)}")
                # Don't re-raise the exception to continue processing other items
                # SUGGESTION: Add option to fail fast on first error
                
    return results 