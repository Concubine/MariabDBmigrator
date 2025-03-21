"""ASCII interface implementation."""
import sys
import time
from typing import Optional, List
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass

from ..domain.models import ExportResult, ImportResult
from ..ui.progress import ProgressStats
from ..core.logging import get_logger

logger = get_logger(__name__)

class ASCIIInterface:
    """ASCII interface for input and output."""
    
    def __init__(self, quiet=False, verbose=False, logger=None):
        """Initialize the interface."""
        self.quiet = quiet
        self.verbose = verbose
        self.progress_tracker = None
        self.logger = logger or get_logger()
    
    def display_progress(self, stats: ProgressStats) -> None:
        """Display progress information in ASCII format."""
        try:
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
            
            # Create status line - make it shorter so it fits better on narrower terminals
            status = (
                f"Progress: [{bar}] {stats.percentage_complete:.1f}% | "
                f"{stats.processed_items}/{stats.total_items} | "
                f"ETA: {time_remaining}"
            )
            
            # Only log to console at certain intervals to avoid overwhelming logs
            if (stats.percentage_complete < 1 or
                stats.percentage_complete > 99 or
                stats.percentage_complete % 10 < 0.5):  # Log at 0%, 10%, 20%... and >99%
                self.logger.info(
                    f"Progress: {stats.percentage_complete:.1f}% | "
                    f"Processed: {stats.processed_items}/{stats.total_items} | "
                    f"Speed: {speed} | "
                    f"ETA: {time_remaining}"
                )
            
            # Always write to stdout
            sys.stdout.write(status)
            sys.stdout.flush()
            
        except Exception as e:
            # If display fails, log the error and continue
            self.logger.error(f"Failed to display progress: {str(e)}")
            # Try a simpler approach
            try:
                sys.stdout.write(f"\rProgress: {stats.percentage_complete:.1f}%")
                sys.stdout.flush()
            except:
                pass
    
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
    
    def display_import_result(self, result: ImportResult) -> None:
        """Display a single import result in ASCII format."""
        # Define status symbols
        symbols = {
            "success": "✓",
            "warning": "⚠",
            "error": "✗"
        }
        symbol = symbols.get(result.status, "?")
        
        # Create basic output
        if result.status == "success":
            output = f"{symbol} {result.table_name}: Imported {result.rows_imported} rows"
            if result.expected_rows > 0:
                output += f" (expected: {result.expected_rows})"
            if result.duration > 0:
                output += f" in {self._format_time(result.duration)}"
        elif result.status == "warning":
            output = f"{symbol} {result.table_name}: Warning - {result.error_message}"
            if result.rows_imported > 0:
                output += f" (imported: {result.rows_imported} rows)"
        else:
            output = f"{symbol} {result.table_name}: Error - {result.error_message}"
        
        # Print or log the result
        if result.status == "error":
            self.logger.error(output)
        elif result.status == "warning":
            self.logger.warning(output)
        else:
            self.logger.info(output)

    def display_import_summary(self, results: List[ImportResult]) -> None:
        """Display a summary of multiple import operations."""
        # Count statistics
        total_tables = len(results)
        success_count = sum(1 for r in results if r.status == "success")
        warning_count = sum(1 for r in results if r.status == "warning")
        error_count = sum(1 for r in results if r.status == "error")
        total_rows_imported = sum(r.rows_imported for r in results)
        empty_tables = sum(1 for r in results if r.status == "success" and r.rows_imported == 0)
        
        # Calculate total duration
        total_duration = sum(r.duration for r in results if r.duration > 0)
        
        # Calculate percentages
        success_percentage = (success_count / total_tables * 100) if total_tables > 0 else 0
        warning_percentage = (warning_count / total_tables * 100) if total_tables > 0 else 0
        error_percentage = (error_count / total_tables * 100) if total_tables > 0 else 0
        
        # Create summary
        summary = (
            f"\n===== Import Summary =====\n"
            f"Total tables processed: {total_tables}\n"
            f"Successful imports: {success_count} ({success_percentage:.1f}%)\n"
            f"Imports with warnings: {warning_count} ({warning_percentage:.1f}%)\n"
            f"Failed imports: {error_count} ({error_percentage:.1f}%)\n"
            f"Total rows imported: {total_rows_imported}\n"
        )
        
        if empty_tables > 0:
            summary += f"Tables with zero rows: {empty_tables} ({(empty_tables/total_tables*100):.1f}%)\n"
        
        if total_duration > 0:
            summary += f"Total duration: {self._format_time(total_duration)}\n"
        
        # Add warnings about problematic tables
        if warning_count > 0 or error_count > 0:
            summary += "\nProblematic tables:\n"
            
            # List tables with warnings
            if warning_count > 0:
                warning_tables = [r for r in results if r.status == "warning"]
                for i, result in enumerate(warning_tables[:5]):  # Show max 5 warnings
                    summary += f"⚠ {result.table_name}: {result.error_message}\n"
                if len(warning_tables) > 5:
                    summary += f"...and {len(warning_tables) - 5} more tables with warnings\n"
            
            # List tables with errors
            if error_count > 0:
                error_tables = [r for r in results if r.status == "error"]
                for i, result in enumerate(error_tables[:5]):  # Show max 5 errors
                    summary += f"✗ {result.table_name}: {result.error_message}\n"
                if len(error_tables) > 5:
                    summary += f"...and {len(error_tables) - 5} more tables with errors\n"
        
        # Add recommendations if there are issues
        if warning_count > 0 or error_count > 0 or empty_tables > 0:
            summary += "\nRecommendations:\n"
            if error_count > 0:
                summary += "- Fix errors before retrying the import\n"
            if warning_count > 0:
                summary += "- Check tables with warnings for potential data integrity issues\n"
            if empty_tables > 0:
                summary += "- Verify if empty tables are expected or indicate import problems\n"
        
        # Display the summary
        self.logger.info(summary) 