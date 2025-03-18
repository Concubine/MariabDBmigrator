"""Logging configuration for the MariaDB export tool."""
import logging
import sys
from pathlib import Path
from typing import Optional

def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[Path] = None,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
) -> None:
    """Configure logging for the application.
    
    Args:
        log_level: The logging level to use
        log_file: Optional path to log file
        log_format: The format string for log messages
    """
    # Set up basic configuration
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Add file handler if log file is specified
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(log_format))
        logging.getLogger().addHandler(file_handler)

    # Create logger for this module
    logger = logging.getLogger(__name__)
    logger.info("Logging configured successfully")

def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the specified name.
    
    Args:
        name: The name for the logger
        
    Returns:
        A configured logger instance
    """
    return logging.getLogger(name)
