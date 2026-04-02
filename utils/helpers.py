"""
Helper Utilities
Common utility functions used across the application.
"""

import logging
from pathlib import Path
from typing import Optional


def setup_logging(verbose: bool = False) -> logging.Logger:
    """
    Setup application logging.
    
    Args:
        verbose: Enable verbose (DEBUG) logging
        
    Returns:
        Configured logger instance
    """
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    return logging.getLogger('FinanceTracker')


def validate_file(file_path: Path) -> bool:
    """
    Validate that input file exists and has correct extension.
    
    Args:
        file_path: Path to file
        
    Returns:
        True if valid, False otherwise
    """
    if not file_path.exists():
        return False
    
    valid_extensions = ['.csv', '.xlsx', '.xls', '.xlsm']
    return file_path.suffix.lower() in valid_extensions


def format_currency(amount: float, currency: str = '€') -> str:
    """
    Format amount as currency string.
    
    Args:
        amount: Numeric amount
        currency: Currency symbol
        
    Returns:
        Formatted currency string
    """
    return f"{currency}{amount:,.2f}"


def truncate_string(text: str, max_length: int = 50) -> str:
    """
    Truncate string to maximum length.
    
    Args:
        text: Input string
        max_length: Maximum length
        
    Returns:
        Truncated string with ellipsis if needed
    """
    if len(text) <= max_length:
        return text
    return text[:max_length-3] + '...'