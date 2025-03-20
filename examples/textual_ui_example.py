#!/usr/bin/env python
"""Example of using the Textual UI interface."""
import sys
import time
import random
from pathlib import Path
from datetime import datetime

# Add parent directory to path to import from src
sys.path.append(str(Path(__file__).parent.parent))

from src.domain.models import ExportResult, ImportResult
from src.ui.factory import create_interface
from src.ui.progress import ProgressTracker, ProgressStats

def simulate_export():
    """Simulate an export operation with multiple tables."""
    # Create Textual UI interface
    ui = create_interface(interface_type="textual")
    
    # Initial welcome message
    print("MariaDB Export Tool - Textual UI Example")
    print("=======================================")
    print("This example demonstrates the Textual UI interface.")
    print("The interface will appear in a moment...\n")
    
    # Small delay to let the print messages show before Textual takes over
    time.sleep(2)
    
    # Define tables with varying row counts for more interesting demo
    tables = [
        {"name": "users", "rows": random.randint(100000, 500000)},
        {"name": "orders", "rows": random.randint(200000, 800000)},
        {"name": "products", "rows": random.randint(5000, 20000)},
        {"name": "customers", "rows": random.randint(50000, 150000)},
        {"name": "invoices", "rows": random.randint(300000, 900000)}
    ]
    
    results = []
    
    for table in tables:
        # Simulate table size
        table_name = table["name"]
        total_rows = table["rows"]
        
        # Create progress tracker with UI callback
        tracker = ProgressTracker(
            total_items=total_rows,
            update_callback=ui.display_progress,
            update_interval=0.2  # Update more frequently for this demo
        )
        
        # Log table export starting
        start_message = f"Starting export of table '{table_name}' with {total_rows} rows..."
        ui.app.add_log_entry(start_message, "info")
        
        # Simulation variables for more realistic progress
        batch_size = 5000
        processing_speeds = [
            0.000001,  # Fast
            0.000005,  # Medium
            0.00002    # Slow
        ]
        
        # Process in batches for more interesting progress updates
        for i in range(0, total_rows, batch_size):
            batch_end = min(i + batch_size, total_rows)
            batch_size_actual = batch_end - i
            
            # Randomly vary the processing speed to simulate real-world conditions
            processing_speed = random.choice(processing_speeds)
            
            # Simulate processing the batch
            time.sleep(processing_speed * batch_size_actual)
            
            # Update progress
            tracker.update(batch_end)
            
            # Sometimes log interesting events
            if random.random() < 0.1:
                event_type = random.choice(["note", "warning", "info"])
                if event_type == "warning":
                    ui.app.add_log_entry(f"Potential performance issue detected in table '{table_name}', continuing...", "warning")
                elif event_type == "note":
                    ui.app.add_log_entry(f"Processing batch {i//batch_size + 1} of table '{table_name}'", "info")
        
        # Create a result with more interesting data
        export_file = Path(f"./export/{table_name}.sql")
        success = random.random() > 0.1  # 90% chance of success
        
        # Sometimes add a small delay to simulate file I/O
        time.sleep(random.uniform(0.2, 1.0))
        
        rows_exported = total_rows if success else random.randint(0, total_rows - 1)
        file_size = rows_exported * random.randint(100, 500)  # More realistic file size based on rows
        duration = random.uniform(1.0, 10.0)
        
        result = ExportResult(
            table_name=table_name,
            success=success,
            rows_exported=rows_exported,
            schema_exported=success,
            data_exported=success,
            file_path=str(export_file) if success else None,
            file_size=file_size,
            duration=duration,
            error_message="Simulated error: Connection reset by peer" if not success else None,
            checksum="abc123def456" if success else None
        )
        
        # Display result
        ui.display_export_result(result)
        results.append(result)
        
        # Small delay between tables
        time.sleep(1)
    
    # Display summary
    ui.display_export_summary(results)
    
    # Simulate an import operation for demonstration
    ui.app.add_log_entry("Starting import demonstration...", "info")
    time.sleep(1)
    
    import_results = []
    for i, table in enumerate(tables):
        if i < 3:  # Only import first 3 tables
            import_result = ImportResult(
                table_name=table["name"],
                status="completed" if random.random() > 0.2 else "failed",
                rows_imported=table["rows"] if random.random() > 0.2 else random.randint(0, table["rows"]),
                duration=random.uniform(1.0, 8.0),
                error_message="Simulated import error: Duplicate key" if random.random() < 0.2 else None,
                database="test_db",
                checksum_verified=random.random() > 0.1,
                checksum_status="verified" if random.random() > 0.1 else "mismatch"
            )
            ui.display_import_result(import_result)
            import_results.append(import_result)
            time.sleep(1)
    
    # Wait for user to view results before exiting
    time.sleep(10)

if __name__ == "__main__":
    simulate_export() 