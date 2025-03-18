# MariaDB Export Tool

A command-line tool for exporting data from MariaDB databases to various file formats.

## Features

- Export entire databases or specific tables
- Support for custom SQL queries
- Multiple export formats (CSV, JSON, SQL)
- Optional compression
- Progress tracking with ETA
- Configurable batch size
- Data filtering with WHERE clauses

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/mariadbexport.git
cd mariadbexport
```

2. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements/prod.txt
```

## Configuration

1. Copy the example configuration file:
```bash
cp config/config.yaml.example config/config.yaml
```

2. Edit `config/config.yaml` with your database settings:
```yaml
database:
  host: localhost
  port: 3306
  user: your_username
  password: your_password
  database: your_database
```

## Usage

### Export all tables

```bash
python main.py --format csv --output-dir exports
```

### Export a specific table

```bash
python main.py --table users --format json --compression
```

### Export with custom query

```bash
python main.py --query "SELECT * FROM users WHERE age > 18" --format csv
```

### Export with WHERE clause

```bash
python main.py --table orders --where "status = 'completed'" --format json
```

## Command Line Arguments

- `--config`: Path to configuration file (default: config/config.yaml)
- `--table`: Specific table to export
- `--query`: Custom SQL query to export
- `--format`: Export format (csv, json, sql) (default: csv)
- `--output-dir`: Output directory for exported files (default: exports)
- `--compression`: Enable compression for exported files
- `--where`: WHERE clause for filtering data

## Development

### Setup Development Environment

```bash
pip install -r requirements/dev.txt
```

### Running Tests

```bash
pytest
```

### Code Style

```bash
black .
isort .
flake8
mypy .
```

## License

This project is licensed under the MIT License - see the LICENSE file for details. 