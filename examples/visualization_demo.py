#!/usr/bin/env python
"""Demonstration of the enhanced visualization capabilities."""
import time
import random
import argparse
import os
from pathlib import Path
import sys

# Add parent directory to path to make imports work
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from src.ui.rich_ascii import RichASCIIInterface
from src.ui.ascii import ASCIIInterface
from src.domain.models import ExportResult, ImportResult
from src.ui.progress import ProgressTracker, ProgressStats

def simulate_table_export(ui_interface, table_name, total_rows):
    """Simulate exporting a table with progress updates."""
    # Create a progress tracker
    tracker = ProgressTracker(
        total_items=total_rows,
        update_callback=ui_interface.display_progress
    )
    
    # Simulate processing rows
    processed = 0
    while processed < total_rows:
        # Process a random number of rows (between 1% and 5% of total)
        batch_size = random.randint(int(total_rows * 0.01), int(total_rows * 0.05))
        batch_size = min(batch_size, total_rows - processed)
        processed += batch_size
        
        # Update progress
        tracker.update(processed)
        
        # Sleep to simulate work
        time.sleep(0.2)
    
    # Complete the progress
    tracker.complete()
    
    # Create a successful result
    success = random.random() > 0.2  # 80% chance of success
    result = ExportResult(
        table_name=table_name,
        success=success,
        rows_exported=total_rows if success else int(total_rows * random.random()),
        file_path=f"exports/{table_name}.sql" if success else None,
        file_size=random.randint(1000, 1000000) if success else 0,
        duration=random.uniform(0.5, 5.0),
        error_message=None if success else "Simulated export error"
    )
    
    # Display the result
    ui_interface.display_export_result(result)
    
    return result

def main():
    """Main demonstration function."""
    parser = argparse.ArgumentParser(description="Demonstrate visualization capabilities")
    parser.add_argument("--ui", choices=["ascii", "rich_ascii"], default="rich_ascii", 
                       help="UI interface to use")
    args = parser.parse_args()
    
    # Create the UI interface
    if args.ui == "ascii":
        ui = ASCIIInterface()
    else:
        ui = RichASCIIInterface()
        ui.start("export")
    
    try:
        # Define some sample tables
        tables = [
            ("customers", 5000),
            ("orders", 10000),
            ("products", 2000),
            ("categories", 100),
            ("users", 1500),
            ("payments", 8000),
            ("reviews", 3000),
            ("inventory", 4000),
            ("shipments", 6000),
            ("returns", 1000)
        ]
        
        # Simulate exporting each table
        results = []
        for table_name, row_count in tables:
            result = simulate_table_export(ui, table_name, row_count)
            results.append(result)
            
            # Sleep between tables
            time.sleep(0.5)
        
        # Display the summary
        ui.display_export_summary(results)
        
    finally:
        # Stop the UI if it's a rich interface
        if args.ui == "rich_ascii":
            ui.stop()

if __name__ == "__main__":
    main() 