"""
Transaction Normalizer Module
Normalizes raw transaction data into a consistent format.
"""

import re
from datetime import datetime
from typing import List, Dict, Any, Optional
import pandas as pd

from modules.parser import detect_column_mapping


def normalize_transactions(raw_transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Normalize raw transaction data into a consistent format.
    Auto-detects date format (DD-MM vs MM-DD) to prevent errors.
    
    Args:
        raw_transactions: List of raw transaction dictionaries
        
    Returns:
        List of normalized transaction dictionaries
    """
    if not raw_transactions:
        return []
    
    # Detect column mapping
    columns = list(raw_transactions[0].keys())
    mapping = detect_column_mapping(columns)
    
    # AUTO-DETECT DATE FORMAT by analyzing date samples
    date_samples = []
    date_col = mapping.get('date')
    if date_col:
        for tx in raw_transactions[:100]:  # Sample first 100 transactions
            date_val = tx.get(date_col)
            if date_val:
                date_samples.append(str(date_val))
    
    detected_format = detect_date_format(date_samples) if date_samples else 'DD-MM'
    
    print(f"🔍 Auto-detected date format: {detected_format}")
    print(f"   Sample dates: {date_samples[:3]}")
    
    normalized = []
    
    for raw_tx in raw_transactions:
        try:
            normalized_tx = _normalize_single_transaction(raw_tx, mapping, detected_format)
            if normalized_tx:  # Skip invalid transactions
                normalized.append(normalized_tx)
        except Exception as e:
            # Skip transactions that cannot be normalized
            continue
    
    return normalized


def _normalize_single_transaction(
    raw_tx: Dict[str, Any], 
    mapping: Dict[str, str],
    date_format: str = 'DD-MM'
) -> Optional[Dict[str, Any]]:
    """
    Normalize a single transaction.
    
    Args:
        raw_tx: Raw transaction dictionary
        mapping: Column mapping dictionary
        date_format: 'DD-MM' for Italian or 'MM-DD' for American
        
    Returns:
        Normalized transaction dictionary or None if invalid
    """
    # Extract fields using mapping
    date_raw = raw_tx.get(mapping.get('date'))
    description_raw = raw_tx.get(mapping.get('description'))
    
    # Handle both single amount column and separate debit/credit columns
    amount_raw = raw_tx.get(mapping.get('amount'))
    debit_raw = raw_tx.get(mapping.get('debit'))
    credit_raw = raw_tx.get(mapping.get('credit'))
    
    balance_raw = raw_tx.get(mapping.get('balance'))
    
    # Skip if essential fields are missing
    if not date_raw or not description_raw:
        return None
    
    # Determine amount from either single column or debit/credit columns
    final_amount = None
    
    if amount_raw is not None and not pd.isna(amount_raw):
        # Single amount column - use as is
        final_amount = amount_raw
    elif debit_raw is not None or credit_raw is not None:
        # Separate debit/credit columns (BPER format)
        # Note: BPER already provides negative values for Uscite!
        
        # Check debit (Uscite) - might already be negative
        if pd.notna(debit_raw):
            if isinstance(debit_raw, (int, float)):
                debit_val = float(debit_raw)
            else:
                debit_val = _normalize_amount(debit_raw)
            
            if debit_val is not None and debit_val != 0:
                # Use as-is if already negative, otherwise make it negative
                final_amount = debit_val if debit_val < 0 else -abs(debit_val)
        
        # Check credit (Entrate) - should be positive
        if final_amount is None and pd.notna(credit_raw):
            if isinstance(credit_raw, (int, float)):
                credit_val = float(credit_raw)
            else:
                credit_val = _normalize_amount(credit_raw)
            
            if credit_val is not None and credit_val != 0:
                final_amount = abs(credit_val)  # Ensure positive
    
    # If still no amount, skip this transaction
    if final_amount is None:
        return None
    
    # Normalize date with detected format
    date_normalized = _normalize_date(date_raw, date_format)
    if not date_normalized:
        return None
    
    # Normalize description
    description_normalized = _normalize_description(description_raw)
    
    # Normalize amount (final_amount might already be a float)
    if isinstance(final_amount, (int, float)):
        amount_normalized = float(final_amount)
    else:
        amount_normalized = _normalize_amount(final_amount)
    
    if amount_normalized is None:
        return None
    
    # Normalize balance (optional)
    balance_normalized = _normalize_amount(balance_raw) if balance_raw else None
    
    return {
        'date': date_normalized,
        'description_raw': str(description_raw),
        'description_normalized': description_normalized,
        'amount': amount_normalized,
        'balance': balance_normalized
    }


def detect_date_format(date_samples: List[str]) -> str:
    """
    Auto-detect date format by analyzing a sample of dates.
    
    Strategy:
    1. Look for dates with day > 12 (unambiguous)
    2. Check if dates are in the future (likely wrong format)
    3. Determine if format is DD-MM or MM-DD
    
    Args:
        date_samples: List of date strings to analyze
        
    Returns:
        'DD-MM' for Italian format or 'MM-DD' for American format
    """
    today = datetime.now()
    dd_mm_score = 0
    mm_dd_score = 0
    
    for date_str in date_samples[:50]:  # Analyze first 50 dates
        if not date_str or pd.isna(date_str):
            continue
            
        date_str = str(date_str).strip()
        
        # Try to extract numeric parts (YYYY-XX-YY or XX/YY/YYYY)
        # Match patterns like: 2026-02-11, 11/02/2026, 11-02-2026
        import re
        
        # Pattern 1: YYYY-XX-YY or YYYY/XX/YY
        match = re.match(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})', date_str)
        if match:
            year = int(match.group(1))
            first_num = int(match.group(2))
            second_num = int(match.group(3))
            
            # If first_num > 12, it MUST be day (DD-MM format)
            if first_num > 12:
                dd_mm_score += 10
            # If second_num > 12, it MUST be day (MM-DD format)
            elif second_num > 12:
                mm_dd_score += 10
            else:
                # Ambiguous case: check if date would be in future
                # Try DD-MM interpretation
                try:
                    date_dd_mm = datetime(year, first_num, second_num)
                    if date_dd_mm > today:
                        dd_mm_score -= 5  # Penalize future dates
                    else:
                        dd_mm_score += 1
                except:
                    pass
                
                # Try MM-DD interpretation
                try:
                    date_mm_dd = datetime(year, second_num, first_num)
                    if date_mm_dd > today:
                        mm_dd_score -= 5  # Penalize future dates
                    else:
                        mm_dd_score += 1
                except:
                    pass
        
        # Pattern 2: XX/YY/YYYY or XX-YY-YYYY
        match = re.match(r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})', date_str)
        if match:
            first_num = int(match.group(1))
            second_num = int(match.group(2))
            year = int(match.group(3))
            
            # If first_num > 12, it MUST be day (DD-MM format)
            if first_num > 12:
                dd_mm_score += 10
            # If second_num > 12, it MUST be month in MM-DD format (impossible, so DD-MM)
            elif second_num > 12:
                dd_mm_score += 10
            else:
                # Check future dates
                try:
                    date_dd_mm = datetime(year, second_num, first_num)
                    if date_dd_mm > today:
                        dd_mm_score -= 5
                    else:
                        dd_mm_score += 1
                except:
                    pass
                
                try:
                    date_mm_dd = datetime(year, first_num, second_num)
                    if date_mm_dd > today:
                        mm_dd_score -= 5
                    else:
                        mm_dd_score += 1
                except:
                    pass
    
    # Return format with highest score
    # Default to DD-MM (Italian) if scores are equal
    return 'DD-MM' if dd_mm_score >= mm_dd_score else 'MM-DD'


def _normalize_date(date_value: Any, date_format: str = 'DD-MM') -> Optional[str]:
    """
    Normalize date to ISO format (YYYY-MM-DD).
    Supports Italian date format (e.g., "19 marzo 2026").
    
    Args:
        date_value: Raw date value (string, datetime, or timestamp)
        date_format: 'DD-MM' for Italian or 'MM-DD' for American
        
    Returns:
        ISO formatted date string or None if invalid
    """
    if pd.isna(date_value):
        return None
    
    # If already datetime
    if isinstance(date_value, datetime):
        return date_value.strftime('%Y-%m-%d')
    
    # If pandas Timestamp
    if isinstance(date_value, pd.Timestamp):
        return date_value.strftime('%Y-%m-%d')
    
    # Try parsing string
    date_str = str(date_value).strip()
    
    # FIRST: Try manual Italian month mapping (most reliable for BPER)
    italian_months = {
        'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
        'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
        'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
    }
    
    # Check if it's Italian format: "19 marzo 2026"
    parts = date_str.split()
    if len(parts) == 3:
        try:
            day = parts[0]
            month_name = parts[1].lower()
            year = parts[2]
            
            if month_name in italian_months:
                month = italian_months[month_name]
                return f"{year}-{month}-{day.zfill(2)}"
        except:
            pass
    
    # Handle YYYY-XX-YY format based on detected format
    import re
    match = re.match(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})', date_str)
    if match:
        year = match.group(1)
        first_num = match.group(2).zfill(2)
        second_num = match.group(3).zfill(2)
        
        if date_format == 'DD-MM':
            # YYYY-DD-MM → YYYY-MM-DD (swap middle numbers)
            return f"{year}-{second_num}-{first_num}"
        else:
            # YYYY-MM-DD → keep as is
            return f"{year}-{first_num}-{second_num}"
    
    # Use pandas with appropriate dayfirst setting
    try:
        dayfirst = (date_format == 'DD-MM')
        dt = pd.to_datetime(date_str, dayfirst=dayfirst)
        return dt.strftime('%Y-%m-%d')
    except:
        pass
    
    # FALLBACK: Try specific formats based on detected format
    if date_format == 'DD-MM':
        formats = [
            '%d/%m/%Y',      # Italian: 23/03/2026
            '%d-%m-%Y',      # Italian: 23-03-2026
            '%d.%m.%Y',      # Italian: 23.03.2026
            '%d %b %Y',      # 23 Mar 2026
            '%d %B %Y',      # 23 March 2026
        ]
    else:
        formats = [
            '%m/%d/%Y',      # American: 03/23/2026
            '%m-%d-%Y',      # American: 03-23-2026
        ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    # No valid date found
    return None


def _normalize_description(description: Any) -> str:
    """
    Normalize transaction description.
    
    Args:
        description: Raw description
        
    Returns:
        Normalized description (uppercase, trimmed, simplified)
    """
    if pd.isna(description):
        return "UNKNOWN"
    
    desc = str(description).strip()
    
    # Convert to uppercase
    desc = desc.upper()
    
    # Remove extra whitespace
    desc = re.sub(r'\s+', ' ', desc)
    
    # Remove special characters but keep basic punctuation
    desc = re.sub(r'[^\w\s\-\.\,\&]', '', desc)
    
    # Trim to reasonable length
    if len(desc) > 200:
        desc = desc[:200]
    
    return desc if desc else "UNKNOWN"


def _normalize_amount(amount: Any) -> Optional[float]:
    """
    Normalize amount to float.
    
    Args:
        amount: Raw amount value
        
    Returns:
        Float amount or None if invalid
    """
    if pd.isna(amount):
        return None
    
    # If already numeric
    if isinstance(amount, (int, float)):
        return float(amount)
    
    # Parse string
    amount_str = str(amount).strip()
    
    # Remove currency symbols and spaces
    amount_str = re.sub(r'[€$£¥\s]', '', amount_str)
    
    # Handle different decimal separators
    # If both comma and dot present, assume comma is thousands separator
    if ',' in amount_str and '.' in amount_str:
        amount_str = amount_str.replace(',', '')
    # If only comma, assume it's decimal separator (European format)
    elif ',' in amount_str:
        amount_str = amount_str.replace(',', '.')
    
    # Remove any remaining non-numeric characters except dot and minus
    amount_str = re.sub(r'[^\d\.\-]', '', amount_str)
    
    try:
        return float(amount_str)
    except ValueError:
        return None
