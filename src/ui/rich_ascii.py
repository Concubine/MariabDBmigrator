"""Rich ASCII interface implementation using the Rich library."""
import sys
import time
import os
import shutil
import logging
import ctypes
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime

try:
    # Import Rich components for enhanced visualization
    from rich.console import Console
    from rich.progress import (
        Progress, 
        BarColumn, 
        TextColumn, 
        TimeRemainingColumn, 
        SpinnerColumn,
        TaskProgressColumn,
        TimeElapsedColumn
    )
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from rich.layout import Layout
    from rich.live import Live
    from rich import box
except ImportError:
    print("Rich library not found. Please install with: pip install rich")
    sys.exit(1)

from ..domain.models import ExportResult, ImportResult
from ..ui.progress import ProgressStats
from ..ui.log_viewer import LogViewer
from ..core.logging import get_logger

logger = get_logger(__name__)

class ConsoleAreaHandler(logging.Handler):
    """Logging handler that redirects logs to the console area at the bottom of the screen."""
    
    def __init__(self, rich_interface):
        """Initialize the handler.
        
        Args:
            rich_interface: The RichASCIIInterface instance
        """
        super().__init__()
        self.rich_interface = rich_interface
        self.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    
    def emit(self, record):
        """Emit a log record."""
        try:
            message = self.format(record)
            self.rich_interface._log_to_console(message)
            # Also add to the rich log viewer if it's enabled
            self.rich_interface._add_to_log_viewer(record)
        except Exception:
            self.handleError(record)

class RichASCIIInterface:
    """Rich ASCII-based command line interface with enhanced visualization."""
    
    def __init__(self, **kwargs):
        self.logger = logger
        self.console = Console()
        self.active_progress = None
        self.task_id = None
        self.current_operation = None
        self.progress_layout = None
        self.live_display = None
        self.log_viewer = LogViewer()
        self.show_logs = kwargs.get('show_logs', True)
        self.log_console_lines = 8  # Number of lines to reserve for console logs at bottom
        self.terminal_height, self.terminal_width = self._get_terminal_size()
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(complete_style="green", finished_style="bright_green"),
            TaskProgressColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            TextColumn("•"),
            TextColumn("[cyan]{task.fields[speed]}/sec")
        )
        
        # Track the last update time to prevent too frequent updates causing flicker
        self.last_update_time = time.time()
        self.update_interval = 0.2  # Minimum seconds between updates
        
        # Store the last stats update to avoid unnecessary redraws
        self.last_stats = {}
        
        # Print newlines to leave space for console logs at the bottom
        self._reserve_console_space()
        
        # Set up console area logging handler
        self._setup_console_logger()
        
        # Bring window to foreground if we're on Windows
        self._bring_to_foreground()
    
    def _setup_console_logger(self):
        """Set up a logging handler to redirect logs to the console area."""
        # Create a handler that redirects logs to our console area
        handler = ConsoleAreaHandler(self)
        
        # Set level to INFO - we don't want to show debug logs in the console area
        handler.setLevel(logging.INFO)
        
        # Add the handler to the root logger
        logging.getLogger().addHandler(handler)
    
    def _add_to_log_viewer(self, record):
        """Add a log record to the rich log viewer."""
        if self.show_logs and hasattr(self, 'log_viewer'):
            try:
                # Format the time
                log_time = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
                
                # Create an entry similar to how LogViewer processes log lines
                entry = {
                    'timestamp': log_time,
                    'module': record.name,
                    'level': record.levelname,
                    'message': record.getMessage(),
                    'raw': f"{log_time} - {record.name} - {record.levelname} - {record.getMessage()}"
                }
                
                # Add entry directly to LogViewer's entries list with its lock
                if hasattr(self.log_viewer, 'log_lock') and hasattr(self.log_viewer, 'log_entries'):
                    with self.log_viewer.log_lock:
                        self.log_viewer.log_entries.append(entry)
                        # Trim the log entries if needed
                        if len(self.log_viewer.log_entries) > self.log_viewer.max_entries:
                            self.log_viewer.log_entries = self.log_viewer.log_entries[-self.log_viewer.max_entries:]
                
                # Update the logs panel if we have an active display
                if self.progress_layout and "main" in self.progress_layout and "logs" in self.progress_layout["main"]:
                    self.progress_layout["main"]["logs"].update(self.log_viewer.render())
            except Exception as e:
                # Don't crash if log update fails
                self.logger.debug(f"Failed to add log record to viewer: {str(e)}")
                pass
    
    def _get_terminal_size(self):
        """Get the terminal size."""
        try:
            return shutil.get_terminal_size()
        except:
            return (80, 24)  # Default fallback
    
    def _reserve_console_space(self):
        """Reserve space at the bottom of the terminal for console logs."""
        # Print newlines to create space at the bottom
        for _ in range(self.log_console_lines):
            print("")
        
        # Move cursor back up to start displaying the UI
        sys.stdout.write(f"\033[{self.log_console_lines}A")
        sys.stdout.flush()
    
    def _create_dashboard(self) -> Layout:
        """Create the rich dashboard layout."""
        # Calculate available height for the dashboard (total - reserved for console)
        available_height = self.terminal_height - self.log_console_lines - 1  # -1 for safety
        
        layout = Layout(name="root")
        
        # Split the layout into sections
        layout.split(
            Layout(name="header", size=3),
            Layout(name="main"),
            Layout(name="footer", size=3)
        )
        
        # Create header with title
        header_text = Text("MariaDB Export/Import Tool", style="bold white on blue")
        header_text.justify = "center"
        layout["header"].update(Panel(header_text, border_style="blue"))
        
        # Create main layout
        main_layout = layout["main"]
        
        # Add rich logs section if enabled (separate from console logs)
        if self.show_logs:
            # Split the main section to add logs at the bottom
            main_layout.split(
                Layout(name="upper_main", ratio=3),
                Layout(name="logs", ratio=2)
            )
            
            # Split upper_main into progress and stats
            main_layout["upper_main"].split_row(
                Layout(name="progress", ratio=2),
                Layout(name="stats", ratio=1)
            )
            
            # Add progress and stats panels
            main_layout["upper_main"]["progress"].update(
                Panel(self.progress, title="Operation Progress", border_style="green")
            )
            
            # Create empty stats panel - will be populated later
            stats_table = Table(box=box.ROUNDED, show_header=False, expand=True, highlight=True)
            stats_table.add_column("Key", style="cyan")
            stats_table.add_column("Value", style="yellow")
            stats_table.add_row("Status", "Waiting for operation to start...")
            
            main_layout["upper_main"]["stats"].update(
                Panel(stats_table, title="Operation Statistics", border_style="cyan")
            )
            
            # Create log viewer panel
            log_panel = Panel(
                self.log_viewer.render(),
                title="Recent Logs", 
                border_style="blue"
            )
            main_layout["logs"].update(log_panel)
            
            # Start monitoring logs
            self.log_viewer.start_monitoring()
        else:
            # If no logs, just split main into progress and stats directly
            main_layout.split_row(
                Layout(name="progress", ratio=2),
                Layout(name="stats", ratio=1)
            )
            
            # Add progress and stats panels
            main_layout["progress"].update(
                Panel(self.progress, title="Operation Progress", border_style="green")
            )
            
            # Create empty stats panel with initial content
            stats_table = Table(box=box.ROUNDED, show_header=False, expand=True, highlight=True)
            stats_table.add_column("Key", style="cyan")
            stats_table.add_column("Value", style="yellow")
            stats_table.add_row("Status", "Waiting for operation to start...")
            
            main_layout["stats"].update(
                Panel(stats_table, title="Operation Statistics", border_style="cyan")
            )
        
        # Footer with info
        footer_text = Text("Press Ctrl+C to cancel | Console logs shown below", style="italic")
        footer_text.justify = "center"
        layout["footer"].update(Panel(footer_text))
        
        return layout
    
    def _update_stats_panel(self, stats: Dict[str, Any]) -> None:
        """Update the stats panel with current operation statistics."""
        if not self.progress_layout:
            return
        
        # Check if the stats have changed significantly to avoid unnecessary updates
        if self._are_stats_similar(stats, self.last_stats):
            return
            
        # Store these stats for comparison next time
        self.last_stats = stats.copy()
        
        try:
            # Create stats table
            stats_table = Table(box=box.ROUNDED, show_header=False, expand=True, highlight=True)
            stats_table.add_column("Key", style="cyan")
            stats_table.add_column("Value", style="yellow")
            
            for key, value in stats.items():
                # Format the value based on type
                if isinstance(value, (int, float)) and key.endswith("_size"):
                    # Format size values
                    value = self._format_size(value)
                elif isinstance(value, float) and key.endswith("_time"):
                    # Format time values
                    value = self._format_time(value)
                    
                # Special case for progress percentage
                if key == "progress":
                    value = f"[bold green]{value}[/]"
                
                # Special case for status
                if key == "status" and value == "Completed":
                    value = f"[bold green]{value}[/]"
                    
                stats_table.add_row(key.replace("_", " ").title(), str(value))
            
            # Directly use a more specific path to update the stats panel
            panel = Panel(stats_table, title="Operation Statistics", border_style="cyan")
            
            if self.show_logs and "main" in self.progress_layout:
                if "upper_main" in self.progress_layout["main"]:
                    if "stats" in self.progress_layout["main"]["upper_main"]:
                        self.progress_layout["main"]["upper_main"]["stats"].update(panel)
            elif "main" in self.progress_layout:
                if "stats" in self.progress_layout["main"]:
                    self.progress_layout["main"]["stats"].update(panel)
                
        except Exception as e:
            # If we can't update the stats panel, log the error but don't crash
            self.logger.warning(f"Error updating stats panel: {str(e)}")
    
    def _are_stats_similar(self, stats1: Dict[str, Any], stats2: Dict[str, Any]) -> bool:
        """Check if two sets of stats are similar enough to skip updating the display."""
        if not stats2:  # If no previous stats, always update
            return False
            
        # Check if keys match
        if set(stats1.keys()) != set(stats2.keys()):
            return False
            
        # Check progress percentage - update if it's changed by more than 1%
        if "progress" in stats1 and "progress" in stats2:
            progress1 = float(stats1["progress"].rstrip("%"))
            progress2 = float(stats2["progress"].rstrip("%"))
            if abs(progress1 - progress2) > 1.0:
                return False
                
        # Check processed items - update if changed by more than 5%
        if "processed_items" in stats1 and "processed_items" in stats2 and "total_items" in stats1:
            if stats1["total_items"] > 0:
                items1 = stats1["processed_items"]
                items2 = stats2["processed_items"]
                total = stats1["total_items"]
                if abs(items1 - items2) / total > 0.05:
                    return False
        
        # If we got here, stats are similar enough to skip update
        return True
    
    def _bring_to_foreground(self):
        """Bring the console window to the foreground."""
        try:
            if sys.platform == "win32":
                # Get the console window handle
                hwnd = ctypes.windll.kernel32.GetConsoleWindow()
                if hwnd:
                    # Bring window to foreground and set as active
                    ctypes.windll.user32.SetForegroundWindow(hwnd)
                    # Ensure window is not minimized
                    ctypes.windll.user32.ShowWindow(hwnd, 9)  # SW_RESTORE = 9
            else:
                # For Linux/Mac, we could potentially use other methods
                pass
        except Exception as e:
            logger.warning(f"Failed to bring window to foreground: {e}")
    
    def display_progress(self, stats: ProgressStats) -> None:
        """Display progress information in Rich ASCII format."""
        try:
            # Throttle updates to reduce flicker
            current_time = time.time()
            if current_time - self.last_update_time < self.update_interval:
                return
            self.last_update_time = current_time
            
            # Check if terminal size has changed and update if needed
            new_height, new_width = self._get_terminal_size()
            if new_height != self.terminal_height or new_width != self.terminal_width:
                self.terminal_height, self.terminal_width = new_height, new_width
                # If we already have an active display, recreate it
                if self.active_progress and self.live_display:
                    self.live_display.stop()
                    self._reserve_console_space()
                    self.progress_layout = self._create_dashboard()
                    self.live_display = Live(
                        self.progress_layout, 
                        refresh_per_second=4,
                        vertical_overflow="crop",
                        auto_refresh=True
                    )
                    self.live_display.start()
            
            # Initialize the progress display if this is the first update
            if self.active_progress is None and self.live_display is None:
                self.progress_layout = self._create_dashboard()
                self.live_display = Live(
                    self.progress_layout, 
                    refresh_per_second=4,
                    vertical_overflow="crop",
                    auto_refresh=True
                )
                self.live_display.start()
                
                # Create a new task in the progress bar
                self.task_id = self.progress.add_task(
                    "[green]Processing", 
                    total=stats.total_items,
                    speed="0"
                )
                self.active_progress = True
            
            # Ensure we have a valid task_id before updating progress
            if self.task_id is not None:
                # Format speed as an integer for cleaner display
                speed_str = f"{int(stats.current_speed)}"
                
                # Update progress bar
                self.progress.update(
                    self.task_id, 
                    completed=stats.processed_items,
                    speed=speed_str
                )
                
                # Update stats panel with more information
                if self.progress_layout:
                    elapsed_time = (datetime.now() - stats.start_time).total_seconds()
                    self._update_stats_panel({
                        "status": "Active",
                        "total_items": stats.total_items,
                        "processed_items": stats.processed_items,
                        "progress": f"{stats.percentage_complete:.1f}%",
                        "current_speed": f"{int(stats.current_speed)} items/sec", 
                        "elapsed_time": elapsed_time,
                        "estimated_remaining": stats.estimated_time_remaining,
                        "estimated_completion": time.strftime('%H:%M:%S', time.localtime(time.time() + stats.estimated_time_remaining))
                    })
                
                # Force a refresh of the live display
                if self.live_display:
                    self.live_display.refresh()
            
            # Only log to console at certain intervals to avoid overwhelming logs
            if (stats.percentage_complete < 1 or
                stats.percentage_complete > 99 or
                stats.percentage_complete % 10 < 0.5):  # Log at 0%, 10%, 20%... and >99%
                # Position cursor at console log area before logging
                message = (
                    f"Progress: {stats.percentage_complete:.1f}% | "
                    f"Processed: {stats.processed_items}/{stats.total_items} | "
                    f"Speed: {int(stats.current_speed)} items/s | "
                    f"ETA: {self._format_time(stats.estimated_time_remaining)}"
                )
                self._log_to_console(message)
                
        except Exception as e:
            # If display fails, log the error and continue
            self.logger.error(f"Failed to display rich progress: {str(e)}")
            # Fall back to basic display approach
            try:
                sys.stdout.write(f"\rProgress: {stats.percentage_complete:.1f}%")
                sys.stdout.flush()
            except:
                pass
    
    def _log_to_console(self, message):
        """Log a message to the console area at the bottom of the screen."""
        if self.live_display:
            # Stop the live display temporarily
            self.live_display.stop()
            
            # Move cursor to the console area
            sys.stdout.write(f"\033[{self.terminal_height}B")
            # Clear the last line
            sys.stdout.write("\033[K")
            # Print the message
            print(message)
            # Move cursor back to the top
            sys.stdout.write(f"\033[{self.terminal_height}A")
            sys.stdout.flush()
            
            # Restart the live display
            self.live_display.start()
        else:
            # Just log normally if no live display
            print(message)
    
    def _finish_progress(self) -> None:
        """Clean up the progress display."""
        if self.active_progress and self.task_id is not None:
            try:
                # Mark the task as complete
                task = self.progress.tasks.get(self.task_id)
                if task:
                    self.progress.update(self.task_id, completed=task.total)
                    
                    # Update stats panel to show completion if possible
                    if self.progress_layout:
                        self._update_stats_panel({
                            "status": "Completed",
                            "total_items": task.total,
                            "processed_items": task.total,
                            "progress": "100.0%",
                            "current_speed": "0 items/sec", 
                            "elapsed_time": task.elapsed,
                            "estimated_remaining": 0.0,
                            "completion_time": time.strftime('%H:%M:%S', time.localtime())
                        })
                
                # Force a final refresh 
                if self.live_display:
                    self.live_display.refresh()
                
                # Sleep briefly to ensure the UI updates before proceeding
                time.sleep(0.5)
            except Exception as e:
                self.logger.warning(f"Error completing progress: {str(e)}")
            finally:
                # Stop the live display
                if self.live_display:
                    self.live_display.stop()
                    self.live_display = None
                
                # Stop log monitoring if it was active
                if self.show_logs:
                    self.log_viewer.stop_monitoring()
                
                # Move cursor to a position after the UI
                sys.stdout.write(f"\033[{self.log_console_lines}B")
                sys.stdout.flush()
                
                self.active_progress = None
                self.task_id = None
                
                # Clear the last stats to ensure fresh display for next operation
                self.last_stats = {}
    
    def display_export_result(self, result: ExportResult) -> None:
        """Display the result of an export operation."""
        # Finish any progress display that might be running
        self._finish_progress()
        
        # Create a table for the export result
        table = Table(title=f"Export Result: {result.table_name}", box=box.ROUNDED)
        table.add_column("Attribute", style="cyan")
        table.add_column("Value", style="yellow")
        
        table.add_row("Status", "[green]Success" if result.success else f"[red]Failed: {result.error_message}")
        table.add_row("Rows Exported", str(result.rows_exported))
        
        if result.file_path:
            table.add_row("File Path", result.file_path)
            
        table.add_row("File Size", self._format_size(result.file_size))
        table.add_row("Duration", self._format_time(result.duration))
        
        # Output the table
        self.console.print(table)
        
        # Also log the result
        if result.success:
            self.logger.info(
                f"Successfully exported {result.table_name}: "
                f"{result.rows_exported} rows, "
                f"{self._format_size(result.file_size)}, "
                f"{self._format_time(result.duration)}"
            )
        else:
            self.logger.error(
                f"Failed to export {result.table_name}: {result.error_message}"
            )
    
    def display_export_summary(self, results: List[ExportResult]) -> None:
        """Display a summary of multiple export operations."""
        # Make sure to properly clean up any active progress display
        self._finish_progress()
        
        # Calculate totals
        total_rows = sum(r.rows_exported for r in results)
        total_size = sum(r.file_size for r in results)
        total_duration = sum(r.duration for r in results)
        success_count = sum(1 for r in results if r.success)
        
        # Create summary table
        summary_table = Table(title="Export Summary", box=box.ROUNDED)
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="yellow")
        
        summary_table.add_row("Total Tables", str(len(results)))
        summary_table.add_row("Successful Exports", f"{success_count}/{len(results)}")
        summary_table.add_row("Total Rows Exported", str(total_rows))
        summary_table.add_row("Total Size", self._format_size(total_size))
        summary_table.add_row("Total Duration", self._format_time(total_duration))
        
        # Create detailed results table
        results_table = Table(title="Detailed Results", box=box.ROUNDED)
        results_table.add_column("Table", style="blue")
        results_table.add_column("Status", style="cyan")
        results_table.add_column("Rows", style="magenta")
        results_table.add_column("Size", style="yellow")
        results_table.add_column("Duration", style="green")
        
        for result in results:
            status = "[green]Success" if result.success else f"[red]Failed"
            results_table.add_row(
                result.table_name,
                status,
                str(result.rows_exported),
                self._format_size(result.file_size),
                self._format_time(result.duration)
            )
        
        # Print both tables
        self.console.print(summary_table)
        self.console.print(results_table)
        
        # Also log the summary
        self.logger.info(
            f"Export Summary: {success_count}/{len(results)} tables, "
            f"{total_rows} rows, {self._format_size(total_size)}, "
            f"{self._format_time(total_duration)}"
        )
    
    def display_import_result(self, result: ImportResult) -> None:
        """Display the result of an import operation."""
        # Finish any progress display that might be running
        self._finish_progress()
        
        # Create a table for the import result
        table = Table(title=f"Import Result: {result.table_name}", box=box.ROUNDED)
        table.add_column("Attribute", style="cyan")
        table.add_column("Value", style="yellow")
        
        table.add_row("Status", f"[green]{result.status}" if result.success else f"[red]Failed: {result.error_message}")
        table.add_row("Rows Imported", str(result.rows_imported))
        
        if result.file_path:
            table.add_row("File Path", result.file_path)
            
        table.add_row("Duration", self._format_time(result.duration))
        
        # Output the table
        self.console.print(table)
        
        # Also log the result
        if result.success:
            self.logger.info(
                f"Successfully imported {result.table_name}: "
                f"{result.rows_imported} rows, "
                f"Status: {result.status}, "
                f"Duration: {self._format_time(result.duration)}"
            )
        else:
            self.logger.error(
                f"Failed to import {result.table_name}: {result.error_message}"
            )
    
    def display_import_summary(self, results: List[ImportResult]) -> None:
        """Display a summary of multiple import operations."""
        # Calculate totals
        total_rows = sum(r.rows_imported for r in results)
        total_duration = sum(r.duration for r in results)
        success_count = sum(1 for r in results if r.success)
        
        # Create summary table
        summary_table = Table(title="Import Summary", box=box.ROUNDED)
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="yellow")
        
        summary_table.add_row("Total Tables", str(len(results)))
        summary_table.add_row("Successful Imports", f"{success_count}/{len(results)}")
        summary_table.add_row("Total Rows Imported", str(total_rows))
        summary_table.add_row("Total Duration", self._format_time(total_duration))
        
        # Create detailed results table
        results_table = Table(title="Detailed Results", box=box.ROUNDED)
        results_table.add_column("Table", style="blue")
        results_table.add_column("Status", style="cyan")
        results_table.add_column("Rows", style="magenta")
        results_table.add_column("Duration", style="green")
        
        for result in results:
            status = f"[green]{result.status}" if result.success else f"[red]Failed"
            results_table.add_row(
                result.table_name,
                status,
                str(result.rows_imported),
                self._format_time(result.duration)
            )
        
        # Print both tables
        self.console.print(summary_table)
        self.console.print(results_table)
        
        # Also log the summary
        self.logger.info(
            f"Import Summary: {success_count}/{len(results)} tables, "
            f"{total_rows} rows, "
            f"{self._format_time(total_duration)}"
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