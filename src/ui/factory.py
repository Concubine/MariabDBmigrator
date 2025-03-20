"""Factory for creating UI interfaces."""
from typing import Optional, Dict, Any

from ..core.logging import get_logger
from ..ui.ascii import ASCIIInterface

logger = get_logger(__name__)

def create_interface(interface_type: str = "ascii", **kwargs) -> Any:
    """Create an appropriate UI interface.
    
    Args:
        interface_type: Type of interface to create (only 'ascii' is supported)
        **kwargs: Additional arguments to pass to the interface constructor
        
    Returns:
        UI interface instance
    """
    logger.info("Using ASCII interface for all operations")
    return ASCIIInterface() 