"""Log viewer for Rich ASCII interface."""
import os
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import re
import threading

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich import box
except ImportError:
    pass  # Will be caught by the interface that uses this

class LogViewer:
    """Log viewer component for Rich ASCII interface."""
    
    def __init__(self, log_file: Optional[str] = None, max_entries: int = 100):
        """Initialize log viewer.
        
        Args:
            log_file: Path to log file (if None, will try to find default)
            max_entries: Maximum number of log entries to keep in memory
        """
        self.log_file = log_file
        self.max_entries = max_entries
        self.log_entries = []
        self.log_pattern = re.compile(
            r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - '
            r'(?P<module>[\w\._]+) - '
            r'(?P<level>\w+) - '
            r'(?P<message>.*)'
        )
        self.file_position = 0
        self.console = Console()
        self.log_lock = threading.Lock()
        self.running = False
        self.log_thread = None
        
        # Try to determine log file if not provided
        if self.log_file is None or not os.path.exists(self.log_file):
            potential_logs = ['LoggerOutput.log', 'logs/LoggerOutput.log']
            for log in potential_logs:
                if os.path.exists(log):
                    self.log_file = log
                    break
    
    def start_monitoring(self) -> None:
        """Start monitoring the log file in a separate thread."""
        if self.log_file and os.path.exists(self.log_file):
            self.file_position = os.path.getsize(self.log_file)
            self.running = True
            self.log_thread = threading.Thread(target=self._monitor_log, daemon=True)
            self.log_thread.start()
    
    def stop_monitoring(self) -> None:
        """Stop monitoring the log file."""
        self.running = False
        if self.log_thread:
            self.log_thread.join(1)  # Wait for 1 second max
            self.log_thread = None
    
    def _monitor_log(self) -> None:
        """Monitor log file for new entries."""
        while self.running:
            try:
                if self.log_file and os.path.exists(self.log_file):
                    file_size = os.path.getsize(self.log_file)
                    
                    if file_size > self.file_position:
                        with open(self.log_file, 'r') as f:
                            f.seek(self.file_position)
                            new_content = f.read()
                            self.file_position = file_size
                            
                            # Process new log entries
                            for line in new_content.splitlines():
                                if line.strip():
                                    self._process_log_line(line)
            except Exception as e:
                print(f"Error monitoring log: {str(e)}")
            
            time.sleep(0.5)  # Check every half second
    
    def _process_log_line(self, line: str) -> None:
        """Process a log line and add it to the entries.
        
        Args:
            line: Log line to process
        """
        match = self.log_pattern.match(line)
        if match:
            entry = {
                'timestamp': match.group('timestamp'),
                'module': match.group('module'),
                'level': match.group('level'),
                'message': match.group('message'),
                'raw': line
            }
            
            with self.log_lock:
                self.log_entries.append(entry)
                # Trim the log entries if needed
                if len(self.log_entries) > self.max_entries:
                    self.log_entries = self.log_entries[-self.max_entries:]
        else:
            # If it doesn't match, just add the raw line
            with self.log_lock:
                if self.log_entries:
                    # Append to the last entry's message if it exists
                    self.log_entries[-1]['message'] += f"\n{line}"
                    self.log_entries[-1]['raw'] += f"\n{line}"
                else:
                    # Create a new entry with just the raw line
                    self.log_entries.append({
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3],
                        'module': 'unknown',
                        'level': 'INFO',
                        'message': line,
                        'raw': line
                    })
    
    def render(self, level_filter: Optional[str] = None, max_rows: int = 10) -> Panel:
        """Render the log viewer as a rich panel.
        
        Args:
            level_filter: Optional level to filter logs by (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            max_rows: Maximum number of rows to display
            
        Returns:
            Rich Panel containing the log table
        """
        # Create table for logs
        log_table = Table(box=box.SIMPLE, expand=True, highlight=True)
        log_table.add_column("Time", style="dim", width=8)
        log_table.add_column("Level", style="cyan", width=8)
        log_table.add_column("Message", style="green")
        
        # Get the log entries (with lock to avoid race conditions)
        with self.log_lock:
            entries = list(self.log_entries)
        
        # Filter by level if needed
        if level_filter:
            entries = [e for e in entries if e['level'] == level_filter.upper()]
        
        # Display the most recent entries (limited by max_rows)
        for entry in entries[-max_rows:]:
            level = entry['level']
            # Color code the level
            if level == 'ERROR' or level == 'CRITICAL':
                level_style = "[bold red]"
            elif level == 'WARNING':
                level_style = "[bold yellow]"
            elif level == 'INFO':
                level_style = "[bold green]"
            else:  # DEBUG
                level_style = "[bold blue]"
                
            # Format the timestamp to be shorter (just time, not date)
            short_time = entry['timestamp'].split()[1].split(',')[0]
            
            log_table.add_row(
                short_time,
                f"{level_style}{level}",
                entry['message']
            )
        
        if not entries:
            log_table.add_row("", "", "[italic]No log entries[/italic]")
        
        return Panel(log_table, title="Recent Logs", border_style="blue")
    
    def get_log_file_info(self) -> Dict[str, Any]:
        """Get information about the log file.
        
        Returns:
            Dictionary with log file information
        """
        if not self.log_file or not os.path.exists(self.log_file):
            return {
                'exists': False,
                'path': str(self.log_file) if self.log_file else 'Not configured',
                'size': 0,
                'modified': None,
                'entries': len(self.log_entries)
            }
            
        file_path = Path(self.log_file)
        stats = file_path.stat()
        
        return {
            'exists': True,
            'path': str(file_path),
            'size': stats.st_size,
            'modified': datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
            'entries': len(self.log_entries)
        } 