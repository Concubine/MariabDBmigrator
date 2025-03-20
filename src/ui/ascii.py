"""ASCII interface implementation."""
import sys
import time
import os
import shutil
from typing import Optional, List, Tuple
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass

# Fix import error: Remove ExportFormat and use proper import path
try:
    from ..domain.models import ExportResult
except ImportError:
    # Define a fallback for compiled version
    @dataclass
    class ExportResult:
        """Result of an export operation."""
        table_name: str
        success: bool
        rows_exported: int = 0
        file_path: str = ""
        file_size: int = 0
        duration: float = 0.0
        error_message: str = ""

from ..ui.progress import ProgressStats
from ..core.logging import get_logger

logger = get_logger(__name__)

class Box:
    """ASCII box drawing characters."""
    # Basic characters
    horizontal = "─"
    vertical = "│"
    corner_top_left = "┌"
    corner_top_right = "┐"
    corner_bottom_left = "└"
    corner_bottom_right = "┘"
    T_right = "├"
    T_left = "┤"
    T_down = "┬"
    T_up = "┴"
    cross = "┼"
    
    # Double line characters
    double_horizontal = "═"
    double_vertical = "║"
    double_corner_top_left = "╔"
    double_corner_top_right = "╗"
    double_corner_bottom_left = "╚"
    double_corner_bottom_right = "╝"
    double_T_right = "╠"
    double_T_left = "╣"
    double_T_down = "╦"
    double_T_up = "╩"
    double_cross = "╬"
    
    # Block characters
    block_full = "█"
    block_light = "░"
    block_medium = "▒"
    block_dark = "▓"

def get_term_size() -> Tuple[int, int]:
    """Get terminal size.
    
    Returns:
        Tuple of (width, height)
    """
    try:
        columns, lines = shutil.get_terminal_size()
        return columns, lines
    except (AttributeError, OSError):
        # Fallback to environment variables or default
        return (
            int(os.environ.get("COLUMNS", 80)),
            int(os.environ.get("LINES", 24))
        )

class ProgressBar:
    """ASCII progress bar."""
    
    def __init__(self, width: int = 50, fill_char: str = "█", empty_char: str = "░"):
        """Initialize progress bar.
        
        Args:
            width: Width of the progress bar
            fill_char: Character used for filled portion
            empty_char: Character used for empty portion
        """
        self.width = width
        self.fill_char = fill_char
        self.empty_char = empty_char
    
    def update(self, progress: float) -> str:
        """Update progress bar.
        
        Args:
            progress: Progress value between 0.0 and 1.0
            
        Returns:
            Formatted progress bar
        """
        filled_width = min(int(self.width * progress), self.width)
        empty_width = self.width - filled_width
        
        return self.fill_char * filled_width + self.empty_char * empty_width

class ASCIIInterface:
    """ASCII-based command line interface."""
    
    def __init__(self):
        self.logger = logger
    
    def display_progress(self, stats: ProgressStats) -> None:
        """Display progress information in ASCII format."""
        # Clear the current line
        sys.stdout.write('\r' + ' ' * 80 + '\r')
        
        # Create progress bar
        bar_length = 50
        filled_length = int(bar_length * stats.percentage_complete / 100)
        bar = '=' * filled_length + '-' * (bar_length - filled_length)
        
        # Format time remaining
        if stats.estimated_time_remaining > 0:
            time_remaining = self._format_time(stats.estimated_time_remaining)
        else:
            time_remaining = "calculating..."
        
        # Format speed
        speed = f"{stats.current_speed:.2f} items/s"
        
        # Create status line
        status = (
            f"Progress: [{bar}] {stats.percentage_complete:.1f}% | "
            f"Processed: {stats.processed_items}/{stats.total_items} | "
            f"Speed: {speed} | "
            f"ETA: {time_remaining}"
        )
        
        sys.stdout.write(status)
        sys.stdout.flush()
    
    def display_export_result(self, result: ExportResult) -> None:
        """Display the result of an export operation."""
        if result.success:
            self.logger.info(
                f"Successfully exported {result.table_name}:\n"
                f"  Rows exported: {result.rows_exported}\n"
                f"  File path: {result.file_path}\n"
                f"  File size: {self._format_size(result.file_size)}\n"
                f"  Duration: {self._format_time(result.duration)}"
            )
        else:
            self.logger.error(
                f"Failed to export {result.table_name}:\n"
                f"  Error: {result.error_message}"
            )
    
    def display_export_summary(self, results: List[ExportResult]) -> None:
        """Display a summary of multiple export operations."""
        total_rows = sum(r.rows_exported for r in results)
        total_size = sum(r.file_size for r in results)
        total_duration = sum(r.duration for r in results)
        success_count = sum(1 for r in results if r.success)
        
        self.logger.info(
            f"Export Summary:\n"
            f"  Total tables: {len(results)}\n"
            f"  Successful exports: {success_count}\n"
            f"  Total rows exported: {total_rows}\n"
            f"  Total size: {self._format_size(total_size)}\n"
            f"  Total duration: {self._format_time(total_duration)}"
        )
    
    def _format_time(self, seconds: float) -> str:
        """Format time in seconds to a human-readable string."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"
    
    def _format_size(self, size_bytes: int) -> str:
        """Format size in bytes to a human-readable string."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f}PB" 