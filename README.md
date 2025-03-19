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