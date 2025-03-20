#!/usr/bin/env python
"""Simple example of a Textual-like interface without using the Textual framework."""
import sys
import time
import random
import os
from pathlib import Path
from datetime import datetime

# Add parent directory to path to import from src
sys.path.append(str(Path(__file__).parent.parent))

from src.domain.models import ExportResult
from src.ui.progress import ProgressTracker, ProgressStats
from src.core.logging import get_logger

logger = get_logger(__name__)

class SimpleTextualInterface:
    """Simplified Textual-like interface."""
    
    def __init__(self):
        self.logger = logger
        self.terminal_width = self._get_terminal_width()
        self.stats = {}
        self._clear_screen()
        self._draw_header()
    
    def _get_terminal_width(self):
        """Get terminal width."""
        try:
            return os.get_terminal_size().columns
        except:
            return 80
    
    def _clear_screen(self):
        """Clear the screen."""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def _draw_header(self):
        """Draw the header section."""
        header = "MariaDB Export/Import Tool"
        padding = (self.terminal_width - len(header)) // 2
        print("\033[1;37;44m" + " " * self.terminal_width + "\033[0m")
        print("\033[1;37;44m" + " " * padding + header + " " * (self.terminal_width - len(header) - padding) + "\033[0m")
        print("\033[1;37;44m" + " " * self.terminal_width + "\033[0m")
        print()
    
    def _draw_progress_bar(self, percentage, current, total):
        """Draw a progress bar."""
        bar_width = self.terminal_width - 20
        filled_width = int(bar_width * percentage / 100)
        bar = "█" * filled_width + "░" * (bar_width - filled_width)
        print(f"\033[1;32m┌{'─' * (self.terminal_width - 2)}┐\033[0m")
        print(f"\033[1;32m│\033[0m \033[1;36mProgress:\033[0m {bar} \033[1;32m│\033[0m")
        print(f"\033[1;32m│\033[0m \033[1;36mStatus:\033[0m {current}/{total} items processed ({percentage:.1f}%) \033[1;32m│\033[0m")
        
        if 'speed' in self.stats:
            print(f"\033[1;32m│\033[0m \033[1;33mSpeed:\033[0m {self.stats.get('speed', '0 items/s')} \033[1;32m│\033[0m")
        
        if 'eta' in self.stats:
            print(f"\033[1;32m│\033[0m \033[1;35mETA:\033[0m {self.stats.get('eta', 'calculating...')} \033[1;32m│\033[0m")
            
        print(f"\033[1;32m└{'─' * (self.terminal_width - 2)}┘\033[0m")
        print()
    
    def _draw_logs(self, logs):
        """Draw the log section."""
        print(f"\033[1;34m┌{'─' * (self.terminal_width - 2)}┐\033[0m")
        print(f"\033[1;34m│\033[0m \033[1mRecent Logs:\033[0m{' ' * (self.terminal_width - 14)}\033[1;34m│\033[0m")
        print(f"\033[1;34m├{'─' * (self.terminal_width - 2)}┤\033[0m")
        
        for log in logs[-10:]:  # Show last 10 logs
            timestamp, level, message = log
            if level.lower() == "error":
                level_color = "\033[1;31m"  # Red
            elif level.lower() == "warning":
                level_color = "\033[1;33m"  # Yellow
            else:
                level_color = "\033[1;36m"  # Cyan
                
            log_line = f"{timestamp} {level_color}{level.upper()}\033[0m: {message}"
            if len(log_line) > self.terminal_width - 4:
                log_line = log_line[:self.terminal_width - 7] + "..."
                
            print(f"\033[1;34m│\033[0m {log_line}{' ' * (self.terminal_width - len(log_line) - 4)}\033[1;34m│\033[0m")
            
        print(f"\033[1;34m└{'─' * (self.terminal_width - 2)}┘\033[0m")
        print()
    
    def _draw_footer(self):
        """Draw the footer section."""
        footer = "Press Ctrl+C to quit | MariaDB Export/Import Tool"
        print("\033[1;37;44m" + footer + " " * (self.terminal_width - len(footer)) + "\033[0m")
    
    def display_progress(self, stats: ProgressStats):
        """Display progress information."""
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
        
        # Only update display every 0.5 seconds to reduce flicker
        current_time = time.time()
        if not hasattr(self, 'last_update_time') or current_time - self.last_update_time > 0.5:
            self.last_update_time = current_time
            
            # Store message in logs
            if (stats.percentage_complete < 1 or
                stats.percentage_complete > 99 or
                stats.percentage_complete % 10 < 0.1):  # Log at 0%, 10%, 20%... and >99%
                
                message = (
                    f"Progress: {stats.percentage_complete:.1f}% | "
                    f"Processed: {stats.processed_items}/{stats.total_items} | "
                    f"Speed: {speed} | "
                    f"ETA: {self.stats['eta']}"
                )
                timestamp = datetime.now().strftime("%H:%M:%S")
                if not hasattr(self, 'logs'):
                    self.logs = []
                self.logs.append((timestamp, "info", message))
                
            self._update_display(stats.percentage_complete, stats.processed_items, stats.total_items)
    
    def _update_display(self, percentage, current, total):
        """Update the entire display."""
        self._clear_screen()
        self._draw_header()
        self._draw_progress_bar(percentage, current, total)
        if hasattr(self, 'logs'):
            self._draw_logs(self.logs)
        self._draw_footer()
    
    def add_log(self, message, level="info"):
        """Add a log entry."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        if not hasattr(self, 'logs'):
            self.logs = []
        self.logs.append((timestamp, level, message))
        
        # Log to system logger too
        if level.lower() == "error":
            self.logger.error(message)
        elif level.lower() == "warning":
            self.logger.warning(message)
        else:
            self.logger.info(message)
    
    def display_export_result(self, result: ExportResult):
        """Display the result of an export operation."""
        if result.success:
            message = (
                f"Successfully exported {result.table_name}: "
                f"{result.rows_exported} rows, "
                f"size: {self._format_size(result.file_size)}, "
                f"duration: {self._format_time(result.duration)}"
            )
            level = "info"
        else:
            message = f"Failed to export {result.table_name}: {result.error_message}"
            level = "error"
        
        self.add_log(message, level)
    
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


def simulate_export():
    """Simulate an export operation with multiple tables."""
    # Create simple Textual-like interface
    ui = SimpleTextualInterface()
    
    # Define tables with varying row counts
    tables = [
        {"name": "users", "rows": 25000},
        {"name": "orders", "rows": 50000},
        {"name": "products", "rows": 10000}
    ]
    
    for table in tables:
        # Simulate table size
        table_name = table["name"]
        total_rows = table["rows"]
        
        # Log table export starting
        ui.add_log(f"Starting export of table '{table_name}' with {total_rows} rows...")
        
        # Set up progress tracking
        start_time = datetime.now()
        
        # Process in batches
        batch_size = 1000
        for i in range(0, total_rows, batch_size):
            batch_end = min(i + batch_size, total_rows)
            
            # Simulate processing the batch
            time.sleep(0.01)  # Fast processing for demo
            
            # Calculate progress stats
            elapsed = (datetime.now() - start_time).total_seconds()
            current_speed = batch_end / elapsed if elapsed > 0 else 0
            percentage = (batch_end / total_rows) * 100
            
            # Calculate ETA
            items_left = total_rows - batch_end
            eta = items_left / current_speed if current_speed > 0 else 0
            
            # Create progress stats
            stats = ProgressStats(
                total_items=total_rows,
                processed_items=batch_end,
                start_time=start_time,
                current_speed=current_speed,
                estimated_time_remaining=eta,
                percentage_complete=percentage
            )
            
            # Update progress display
            ui.display_progress(stats)
            
            # Sometimes log interesting events
            if random.random() < 0.05:
                event_type = random.choice(["warning", "info"])
                if event_type == "warning":
                    ui.add_log(f"Potential performance issue detected in table '{table_name}', continuing...", "warning")
                else:
                    ui.add_log(f"Processing batch {i//batch_size + 1}/{total_rows//batch_size + 1} of table '{table_name}'", "info")
        
        # Create a result
        success = random.random() > 0.1  # 90% chance of success
        
        result = ExportResult(
            table_name=table_name,
            success=success,
            rows_exported=total_rows if success else random.randint(0, total_rows - 1),
            schema_exported=success,
            data_exported=success,
            file_path=f"./export/{table_name}.sql" if success else None,
            file_size=total_rows * random.randint(100, 500) if success else 0,
            duration=elapsed,
            error_message="Simulated error: Connection reset by peer" if not success else None
        )
        
        # Display result
        ui.display_export_result(result)
        
        # Small delay between tables
        time.sleep(1)
    
    # Final log
    ui.add_log("Export simulation completed!")
    
    # Wait a moment to show the final screen
    time.sleep(5)

if __name__ == "__main__":
    try:
        simulate_export()
    except KeyboardInterrupt:
        print("\nExport simulation interrupted by user.")
    except Exception as e:
        print(f"\nError: {str(e)}")
    finally:
        # Restore terminal
        print("\033[0m") 