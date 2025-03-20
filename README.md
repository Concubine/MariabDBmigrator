# MariaDB Export/Import Tool

A high-performance parallel tool for exporting and importing MariaDB databases.

## Status
- Export: ✅ Working with parallel processing
- Import: ⚠️ Needs validation

## Features
- Parallel data processing for improved performance
- Configurable batch size and number of workers
- Support for selective table export/import
- Compression support
- Configurable export/import modes
- Verbose logging options
- Enhanced visualization with Rich terminal interface
- Data integrity verification with checksums

## Requirements
- Python 3.12+
- MariaDB/MySQL Server
- Required Python packages (see requirements.txt)

## Installation

1. Clone the repository:
```bash
git clone [repository-url]
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Visualization Features

The MariaDB Export/Import Tool now includes enhanced terminal visualization through the Rich library:

- Interactive dashboard-style terminal interface
- Real-time progress bars with estimated completion time
- Detailed export/import statistics
- Color-coded success/error display
- Comprehensive table results view
- Consolidated log display

To select the visualization mode, use the `--ui` parameter:

```bash
mariadbexport.exe export --ui rich_ascii [options]  # Enhanced visualization (default)
mariadbexport.exe export --ui ascii [options]       # Basic ASCII visualization
```

## Data Integrity with Checksums

The MariaDB Export/Import Tool now includes built-in data integrity verification through checksums:

- During export, a SHA-256 checksum is calculated for each table's data
- Checksums are stored alongside the exported data in JSON files
- During import, data integrity is automatically verified against the stored checksums
- Import behavior on checksum mismatch depends on the selected import mode:
  - In `cancel` mode, the import will abort if a checksum mismatch is detected
  - In other modes, a warning is logged and the import continues
  
This feature ensures database consistency by guaranteeing that the data hasn't been corrupted or tampered with between export and import operations.

## Usage

### Export
```bash
mariadbexport.exe export [options]

Options:
  --verbose             Enable verbose logging
  --batch-size         Number of rows to process in each batch (default: 1000)
  --parallel-workers   Number of parallel workers (default: 16)
  --compress           Enable compression for output files
  --tables            Specific tables to export (comma-separated)
  --exclude-tables    Tables to exclude from export (comma-separated)
  --where             WHERE clause for filtering data
  --ui                UI interface to use: ascii or rich_ascii (default: rich_ascii)
```

### Import
```bash
mariadbexport.exe import [options]

Options:
  --verbose             Enable verbose logging
  --batch-size         Number of rows to process in each batch (default: 1000)
  --parallel-workers   Number of parallel workers (default: 16)
  --mode              Import mode (skip/replace/truncate)
  --disable-fk        Disable foreign key checks during import
  --continue-on-error Continue importing on error
```

## Configuration
The tool uses environment variables for database connection:
- `DB_HOST` - Database host (default: localhost)
- `DB_PORT` - Database port (default: 3306)
- `DB_USER` - Database username
- `DB_PASSWORD` - Database password
- `DB_NAME` - Database name

## Building from Source
```bash
python -m PyInstaller mariadbexport.spec --clean
```

## License
[License information] 