"""Factory for creating UI interfaces."""
from typing import Optional, Dict, Any

from ..core.logging import get_logger
from ..ui.ascii import ASCIIInterface
from ..ui.rich_ascii import RichASCIIInterface
from ..ui.textual_ui import TextualInterface

logger = get_logger(__name__)

def create_interface(interface_type: str = "rich_ascii", ui_config=None, **kwargs) -> Any:
    """Create an appropriate UI interface.
    
    Args:
        interface_type: Type of interface to create ('ascii', 'rich_ascii', or 'textual')
        ui_config: UI configuration options (from config.ui)
        **kwargs: Additional arguments to pass to the interface constructor
        
    Returns:
        UI interface instance
    """
    # Add UI configuration options to kwargs if provided
    if ui_config:
        kwargs.update({
            'show_summary': getattr(ui_config, 'show_summary', True),
            'show_detailed_results': getattr(ui_config, 'show_detailed_results', True),
            'show_progress': getattr(ui_config, 'show_progress', True),
            'show_logs': getattr(ui_config, 'show_logs', True)
        })
    
    if interface_type.lower() == "textual":
        logger.info("Using Textual TUI interface for enhanced visualization")
        return TextualInterface(**kwargs)
    elif interface_type.lower() == "rich_ascii":
        logger.info("Using Rich ASCII interface for enhanced visualization")
        return RichASCIIInterface(**kwargs)
    else:
        logger.info("Using basic ASCII interface for all operations")
        return ASCIIInterface(**kwargs) 