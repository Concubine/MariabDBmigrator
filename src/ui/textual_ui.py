"""Textual-based TUI interface implementation."""
import sys
import time
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path

try:
    # Import Textual components
    from textual.app import App, ComposeResult
    from textual.containers import Container, Horizontal, Vertical
    from textual.widgets import Static, Header, Footer, ProgressBar, DataTable, Label, Log
    from textual.reactive import reactive
    from textual import events
    from textual.binding import Binding
except ImportError as e:
    print(f"Textual library import error: {e}")
    print("Python path:", sys.path)
    print("Current working directory:", Path.cwd())
    print("Please install with: pip install textual")
    sys.exit(1)

from ..domain.models import ExportResult, ImportResult
from ..ui.progress import ProgressStats
from ..core.logging import get_logger

logger = get_logger(__name__)

class StatusPanel(Static):
    """Panel showing operation status information."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stats = {}
    
    def compose(self) -> ComposeResult:
        """Compose the status panel."""
        yield Static("Status: Waiting for operation to start...", id="status-msg")
        # Add more detailed status elements
        yield Static("Speed: 0 items/s", id="speed-msg")
        yield Static("Elapsed: 0s", id="elapsed-msg") 
        yield Static("ETA: calculating...", id="eta-msg")
        yield Static("Items: 0/0", id="items-msg")
    
    def update_stats(self, stats: Dict[str, Any]) -> None:
        """Update the status panel with new statistics."""
        self.stats = stats
        self.query_one("#status-msg").update(f"Status: {stats.get('status', 'Running...')}")
        
        # Update additional status information if available
        if 'speed' in stats:
            self.query_one("#speed-msg").update(f"Speed: {stats['speed']}")
        if 'elapsed' in stats:
            self.query_one("#elapsed-msg").update(f"Elapsed: {stats['elapsed']}")
        if 'eta' in stats:
            self.query_one("#eta-msg").update(f"ETA: {stats['eta']}")
        if 'items' in stats:
            self.query_one("#items-msg").update(f"Items: {stats['items']}")
        
        self.refresh()

class MariaDBExportApp(App):
    """Textual TUI application for MariaDB Export/Import Tool."""
    
    CSS = """
    #header {
        dock: top;
        background: $primary-darken-2;
        height: 3;
        padding: 0 1;
        content-align: center middle;
        color: $text;
    }
    
    #footer {
        dock: bottom;
        background: $primary-darken-1;
        height: 1;
        padding: 0 1;
    }
    
    .panel {
        height: 1fr;
        border: tall $primary;
        padding: 1;
        margin: 1;
    }
    
    #status-panel {
        width: 30%;
        border: thick $accent;
    }
    
    #progress-panel {
        width: 70%;
        border: thick $success;
    }
    
    #log-area {
        height: 12;
        border: thick $primary;
    }
    
    #progress-title, #stats-title, #logs-title {
        background: $surface;
        color: $text;
        text-align: center;
        padding: 1;
        text-style: bold;
    }
    
    #progress-bar {
        height: 1;
        width: 100%;
    }
    
    #progress-details {
        margin-top: 1;
        text-align: center;
    }
    
    #status-msg, #speed-msg, #elapsed-msg, #eta-msg, #items-msg {
        margin: 1 0;
    }
    
    #status-msg {
        color: $success;
    }
    
    #speed-msg {
        color: $warning;
    }
    
    #elapsed-msg {
        color: $accent;
    }
    
    #eta-msg {
        color: $secondary;
    }
    
    #items-msg {
        color: $primary;
    }
    """
    
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("ctrl+c", "quit", "Quit"),
    ]
    
    def __init__(self, **kwargs):
        super().__init__()
        self.log_entries = []
        self.active_progress_id = None
        self.last_update_time = time.time()
        self.update_interval = 0.2  # Minimum seconds between updates
        self.show_logs = kwargs.get('show_logs', True)
        self.progress_value = reactive(0.0)
        self.stats = {}
    
    def compose(self) -> ComposeResult:
        """Compose the UI layout."""
        yield Header(show_clock=True, id="header", name="MariaDB Export/Import Tool")
        
        with Vertical():
            with Horizontal():
                with Container(classes="panel", id="progress-panel"):
                    yield Static("MariaDB Export/Import Progress", id="progress-title")
                    yield ProgressBar(id="progress-bar", total=100)
                    yield Label("0/0 items processed | 0.0%", id="progress-details")
                
                with Container(classes="panel", id="status-panel"):
                    yield Static("Operation Statistics", id="stats-title")
                    yield StatusPanel(id="status-content")
            
            if self.show_logs:
                with Container(classes="panel", id="log-area"):
                    yield Static("Recent Logs", id="logs-title")
                    yield Log(id="log", highlight=True)
        
        # Add a basic Footer with no highlight/markup parameters
        yield Footer()
    
    def on_mount(self) -> None:
        """Handle the mount event to set up initial state."""
        self.update_progress(0, 0, 0, "Waiting to start")
    
    def update_progress(self, completed: float, current: int, total: int, status: str) -> None:
        """Update the progress bar and details."""
        # Only update if enough time has passed since the last update
        now = time.time()
        if now - self.last_update_time < self.update_interval:
            return
        
        self.last_update_time = now
        try:
            progress_bar = self.query_one(ProgressBar)
            progress_details = self.query_one("#progress-details")
            
            # Update the progress bar
            progress_bar.progress = completed
            
            # Update the details label
            progress_details.update(f"{current}/{total} items processed | {completed:.1f}%")
            
            # Update status
            self.stats["status"] = status
            status_panel = self.query_one(StatusPanel)
            status_panel.update_stats(self.stats)
        except Exception as e:
            logger.error(f"Failed to update progress UI: {e}")
    
    def add_log_entry(self, message: str, level: str = "info") -> None:
        """Add an entry to the log area."""
        if not self.show_logs:
            return
        
        try:
            log_widget = self.query_one(Log)
            
            # Get timestamp
            timestamp = datetime.now().strftime("%H:%M:%S")
            
            # Format log level (use simpler formatting)
            level_formatted = level.upper()
            
            # Add formatted log entry (as plain text)
            log_entry = f"{timestamp} {level_formatted}: {message}"
            log_widget.write(log_entry)
            
            # Also log to system logger for persistence
            if level.lower() == "error":
                logger.error(message)
            elif level.lower() == "warning":
                logger.warning(message)
            else:
                logger.info(message)
        except Exception as e:
            # If we can't update the log, just log to the system logger
            logger.error(f"Failed to update log widget: {e}")
            logger.info(message)


class TextualInterface:
    """Textual-based TUI interface that manages the Textual application."""
    
    def __init__(self, **kwargs):
        self.logger = logger
        self.app = MariaDBExportApp(**kwargs)
        self.app_started = False
        self.stats = {}
        # We'll use a simpler approach without threading
    
    def _start_app_if_needed(self):
        """Start the app if it hasn't been started yet."""
        if not self.app_started:
            try:
                # This will start the app in a non-blocking way using Textual's mechanisms
                self.app_started = True
                self.app.run()
            except Exception as e:
                self.logger.error(f"Error starting Textual app: {str(e)}")
                self.app_started = False
    
    def display_progress(self, stats: ProgressStats) -> None:
        """Display progress information with Textual."""
        try:
            # Ensure app is started
            if not self.app_started:
                self._start_app_if_needed()
                
            # Format speed
            speed = f"{stats.current_speed:.2f} items/s"
            
            # Update statistics
            self.stats = {
                "status": "Running export/import",
                "speed": speed,
                "elapsed": self._format_time((datetime.now() - stats.start_time).total_seconds()),
                "eta": self._format_time(stats.estimated_time_remaining) if stats.estimated_time_remaining > 0 else "calculating...",
                "items": f"{stats.processed_items}/{stats.total_items}"
            }
            
            # Update app UI - only if app is started
            if self.app_started:
                self.app.update_progress(
                    stats.percentage_complete, 
                    stats.processed_items, 
                    stats.total_items,
                    f"Processing at {speed}"
                )
                
                # Log progress at intervals
                if (stats.percentage_complete < 1 or
                    stats.percentage_complete > 99 or
                    stats.percentage_complete % 10 < 0.5):  # Log at 0%, 10%, 20%... and >99%
                    
                    message = (
                        f"Progress: {stats.percentage_complete:.1f}% | "
                        f"Processed: {stats.processed_items}/{stats.total_items} | "
                        f"Speed: {speed} | "
                        f"ETA: {self.stats['eta']}"
                    )
                    try:
                        self.app.add_log_entry(message, "info")
                    except:
                        pass
            else:
                # If app not started, use fallback console output
                sys.stdout.write(f"\rProgress: {stats.percentage_complete:.1f}% | {stats.processed_items}/{stats.total_items}")
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
            message = (
                f"Successfully exported {result.table_name}:\n"
                f"  Rows exported: {result.rows_exported}\n"
                f"  File path: {result.file_path}\n"
                f"  File size: {self._format_size(result.file_size)}\n"
                f"  Duration: {self._format_time(result.duration)}"
            )
            level = "info"
        else:
            message = (
                f"Failed to export {result.table_name}:\n"
                f"  Error: {result.error_message}"
            )
            level = "error"
        
        # Add to log
        if self.app_started:
            try:
                self.app.add_log_entry(message, level)
            except Exception as e:
                self.logger.error(f"Failed to add log entry: {e}")
        
        # Always log to system logger
        if level == "info":
            self.logger.info(message)
        else:
            self.logger.error(message)
    
    def display_export_summary(self, results: List[ExportResult]) -> None:
        """Display a summary of multiple export operations."""
        total_rows = sum(r.rows_exported for r in results)
        total_size = sum(r.file_size for r in results)
        total_duration = sum(r.duration for r in results)
        success_count = sum(1 for r in results if r.success)
        
        message = (
            f"Export Summary:\n"
            f"  Total tables: {len(results)}\n"
            f"  Successful exports: {success_count}\n"
            f"  Total rows exported: {total_rows}\n"
            f"  Total size: {self._format_size(total_size)}\n"
            f"  Total duration: {self._format_time(total_duration)}"
        )
        
        # Add to log
        if self.app_started:
            try:
                self.app.add_log_entry(message, "info")
            except Exception as e:
                self.logger.error(f"Failed to add log entry: {e}")
        
        # Always log to system logger
        self.logger.info(message)
    
    def display_import_result(self, result: ImportResult) -> None:
        """Display the result of an import operation."""
        if result.status == "completed":
            message = (
                f"Successfully imported {result.table_name}:\n"
                f"  Rows imported: {result.rows_imported}\n"
                f"  Duration: {self._format_time(result.duration)}\n"
                f"  Checksum status: {result.checksum_status or 'Not verified'}"
            )
            level = "info"
        else:
            message = (
                f"Failed to import {result.table_name}:\n"
                f"  Error: {result.error_message}"
            )
            level = "error"
        
        # Add to log
        if self.app_started:
            try:
                self.app.add_log_entry(message, level)
            except Exception as e:
                self.logger.error(f"Failed to add log entry: {e}")
        
        # Always log to system logger
        if level == "info":
            self.logger.info(message)
        else:
            self.logger.error(message)
    
    def display_import_summary(self, results: List[ImportResult]) -> None:
        """Display a summary of multiple import operations."""
        total_rows = sum(r.rows_imported for r in results)
        completed_count = sum(1 for r in results if r.status == "completed")
        failed_count = len(results) - completed_count
        total_duration = sum(r.duration for r in results)
        
        message = (
            f"Import Summary:\n"
            f"  Total tables: {len(results)}\n"
            f"  Successful imports: {completed_count}\n"
            f"  Failed imports: {failed_count}\n"
            f"  Total rows imported: {total_rows}\n"
            f"  Total duration: {self._format_time(total_duration)}"
        )
        
        # Add to log
        if self.app_started:
            try:
                self.app.add_log_entry(message, "info")
            except Exception as e:
                self.logger.error(f"Failed to add log entry: {e}")
        
        # Always log to system logger
        self.logger.info(message)
    
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