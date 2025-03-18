"""Progress tracking UI implementation."""
import time
from typing import Optional, Callable
from dataclasses import dataclass
from datetime import datetime

@dataclass
class ProgressStats:
    """Statistics about the progress of an operation."""
    total_items: int
    processed_items: int
    start_time: datetime
    current_speed: float  # items per second
    estimated_time_remaining: float  # seconds
    percentage_complete: float

class ProgressTracker:
    """Tracks and displays progress of operations."""
    
    def __init__(
        self,
        total_items: int,
        update_callback: Optional[Callable[[ProgressStats], None]] = None,
        update_interval: float = 1.0
    ):
        self.total_items = total_items
        self.processed_items = 0
        self.start_time = datetime.now()
        self.last_update_time = self.start_time
        self.last_processed_items = 0
        self.update_callback = update_callback
        self.update_interval = update_interval
    
    def update(self, processed_items: int) -> None:
        """Update the progress with the number of processed items."""
        self.processed_items = processed_items
        current_time = datetime.now()
        
        # Check if we should update the display
        if (current_time - self.last_update_time).total_seconds() >= self.update_interval:
            self._calculate_and_notify(current_time)
            self.last_update_time = current_time
            self.last_processed_items = processed_items
    
    def _calculate_and_notify(self, current_time: datetime) -> None:
        """Calculate progress statistics and notify callback if set."""
        elapsed_time = (current_time - self.start_time).total_seconds()
        time_since_last_update = (current_time - self.last_update_time).total_seconds()
        
        # Calculate current speed
        items_processed_since_last_update = self.processed_items - self.last_processed_items
        current_speed = items_processed_since_last_update / time_since_last_update if time_since_last_update > 0 else 0
        
        # Calculate estimated time remaining
        remaining_items = self.total_items - self.processed_items
        estimated_time_remaining = remaining_items / current_speed if current_speed > 0 else 0
        
        # Calculate percentage complete
        percentage_complete = (self.processed_items / self.total_items) * 100 if self.total_items > 0 else 0
        
        stats = ProgressStats(
            total_items=self.total_items,
            processed_items=self.processed_items,
            start_time=self.start_time,
            current_speed=current_speed,
            estimated_time_remaining=estimated_time_remaining,
            percentage_complete=percentage_complete
        )
        
        if self.update_callback:
            self.update_callback(stats)
    
    def complete(self) -> None:
        """Mark the operation as complete."""
        self.update(self.total_items)
    
    def reset(self) -> None:
        """Reset the progress tracker."""
        self.processed_items = 0
        self.start_time = datetime.now()
        self.last_update_time = self.start_time
        self.last_processed_items = 0 