"""
File Parser Module
Handles parsing of CSV and XLSX bank statement files.
"""

import pandas as pd
from pathlib import Path
from typing import List, Dict, Any


def parse_file(file_path: Path) -> List[Dict[str, Any]]:
    """
    Parse a bank statement file (CSV or XLSX) into a list of transaction dictionaries.
    
    Args:
        file_path: Path to the input file
        
    Returns:
        List of raw transaction dictionaries
        
    Raises:
        ValueError: If file format is not supported
        Exception: If file cannot be parsed
    """
    suffix = file_path.suffix.lower()
    
    if suffix == '.csv':
        return _parse_csv(file_path)
    elif suffix in ['.xlsx', '.xls', '.xlsm']:
        return _parse_excel(file_path)
    else:
        raise ValueError(f"Unsupported file format: {suffix}")


def _parse_csv(file_path: Path) -> List[Dict[str, Any]]:
    """
    Parse CSV file with automatic delimiter detection.
    
    Args:
        file_path: Path to CSV file
        
    Returns:
        List of transaction dictionaries
    """
    try:
        # Try common delimiters
        for delimiter in [',', ';', '\t', '|']:
            try:
                df = pd.read_csv(file_path, delimiter=delimiter, encoding='utf-8')
                if len(df.columns) > 1:  # Valid parse should have multiple columns
                    break
            except:
                continue
        else:
            # Fallback to pandas auto-detection
            df = pd.read_csv(file_path, encoding='utf-8')
        
        return df.to_dict('records')
    
    except UnicodeDecodeError:
        # Try different encodings
        for encoding in ['latin-1', 'iso-8859-1', 'cp1252']:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                return df.to_dict('records')
            except:
                continue
        raise Exception(f"Could not decode CSV file: {file_path}")


def _parse_excel(file_path: Path) -> List[Dict[str, Any]]:
    """
    Parse Excel file (first sheet by default).
    Automatically detects and skips header rows.
    
    Args:
        file_path: Path to Excel file
        
    Returns:
        List of transaction dictionaries
    """
    suffix = file_path.suffix.lower()
    
    # For .xls files, try xlrd engine first
    if suffix == '.xls':
        try:
            # Try to find the header row automatically
            df = _read_excel_with_header_detection(file_path, engine='xlrd')
            return df.to_dict('records')
        except ImportError:
            raise Exception(
                "xlrd library not installed. Install it with: pip install xlrd\n"
                "Or convert your .xls file to .xlsx format."
            )
        except Exception as e:
            # If xlrd fails, suggest conversion
            raise Exception(
                f"Could not parse .xls file: {file_path}\n"
                f"Error: {str(e)}\n"
                f"Try converting the file to .xlsx or .csv format in Excel."
            )
    
    # For .xlsx files, use openpyxl
    try:
        df = _read_excel_with_header_detection(file_path, engine='openpyxl')
        return df.to_dict('records')
    except Exception as e:
        raise Exception(f"Could not parse Excel file: {file_path}. Error: {str(e)}")


def _read_excel_with_header_detection(file_path: Path, engine: str) -> pd.DataFrame:
    """
    Read Excel file and automatically detect where the data table starts.
    Skips bank header information.
    
    Args:
        file_path: Path to Excel file
        engine: Pandas engine to use ('xlrd' or 'openpyxl')
        
    Returns:
        DataFrame with transaction data
    """
    # Try reading without header first to analyze structure
    try:
        df_raw = pd.read_excel(file_path, sheet_name=0, header=None, engine=engine)
    except Exception as e:
        raise Exception(f"Could not read Excel file: {e}")
    
    # Find the header row (row with most non-empty cells that looks like column names)
    header_row = _detect_header_row(df_raw)
    
    if header_row is not None:
        # Read again with correct header row
        df = pd.read_excel(file_path, sheet_name=0, header=header_row, engine=engine)
        # Remove completely empty rows
        df = df.dropna(how='all')
        return df
    else:
        # Fallback: assume first row is header
        df = pd.read_excel(file_path, sheet_name=0, header=0, engine=engine)
        df = df.dropna(how='all')
        return df


def _detect_header_row(df: pd.DataFrame) -> int:
    """
    Detect which row contains the column headers.
    
    Args:
        df: Raw DataFrame without headers
        
    Returns:
        Row index of header, or None if not found
    """
    # Keywords that typically appear in bank statement headers
    header_keywords = [
        'data', 'date', 'fecha', 'datum',
        'descrizione', 'description', 'details', 'causale',
        'importo', 'amount', 'value', 'dare', 'avere', 'debit', 'credit',
        'saldo', 'balance', 'solde',
        'valuta', 'currency',
        'entrate', 'uscite', 'entrata', 'uscita',  # Italian bank terms
        'operazione', 'categoria'  # BPER specific
    ]
    
    max_score = 0
    best_row = None
    
    # Check first 30 rows (some banks have long headers)
    for idx in range(min(30, len(df))):
        row = df.iloc[idx]
        
        # Count non-empty cells
        non_empty = row.notna().sum()
        
        # Skip rows with too few cells
        if non_empty < 2:
            continue
        
        # Check if row contains header keywords
        score = 0
        keyword_matches = 0
        for cell in row:
            if pd.notna(cell):
                cell_str = str(cell).lower().strip()
                for keyword in header_keywords:
                    if keyword in cell_str:
                        score += 10
                        keyword_matches += 1
                        break
        
        # Bonus for having multiple non-empty cells
        score += non_empty
        
        # Extra bonus if we found multiple keywords (likely a real header)
        if keyword_matches >= 3:
            score += 20
        
        if score > max_score:
            max_score = score
            best_row = idx
    
    # Return best row if score is high enough
    if max_score >= 15:
        return best_row
    
    return None


def detect_column_mapping(columns: List[str]) -> Dict[str, str]:
    """
    Automatically detect which columns correspond to date, description, amount, balance.
    Also detects separate debit/credit columns (Entrate/Uscite for Italian banks).
    
    Args:
        columns: List of column names from the file
        
    Returns:
        Dictionary mapping standard fields to actual column names
    """
    mapping = {}
    columns_lower = [str(c).lower().strip() for c in columns]
    
    # Date detection - prefer "data operazione" over "data valuta" for Italian banks
    date_keywords = [
        'data operazione', 'transaction date', 'posting date',  # Prefer these
        'date', 'data', 'fecha', 'datum'  # Generic fallback
    ]
    for keyword in date_keywords:
        for i, col in enumerate(columns_lower):
            if keyword in col and 'valuta' not in col:  # Skip "data valuta"
                mapping['date'] = columns[i]
                break
        if 'date' in mapping:
            break
    
    # If no date found, try including "data valuta" as fallback
    if 'date' not in mapping:
        for i, col in enumerate(columns_lower):
            if 'data' in col or 'date' in col:
                mapping['date'] = columns[i]
                break
    
    # Description detection
    desc_keywords = ['description', 'descrizione', 'descripcion', 'details', 'narrative', 'memo']
    for keyword in desc_keywords:
        for i, col in enumerate(columns_lower):
            if keyword in col:
                mapping['description'] = columns[i]
                break
        if 'description' in mapping:
            break
    
    # Amount detection
    amount_keywords = ['amount', 'importo', 'importe', 'value', 'debit', 'credit']
    for keyword in amount_keywords:
        for i, col in enumerate(columns_lower):
            if keyword in col and 'balance' not in col:
                mapping['amount'] = columns[i]
                break
        if 'amount' in mapping:
            break
    
    # Balance detection (optional)
    balance_keywords = ['balance', 'saldo', 'solde']
    for keyword in balance_keywords:
        for i, col in enumerate(columns_lower):
            if keyword in col:
                mapping['balance'] = columns[i]
                break
        if 'balance' in mapping:
            break
    
    # CRITICAL: Check for separate debit/credit columns (BPER format: Entrate/Uscite)
    debit_keywords = ['uscite', 'uscita', 'dare', 'debit', 'addebito']
    credit_keywords = ['entrate', 'entrata', 'avere', 'credit', 'accredito']
    
    # Search for debit column (Uscite)
    for i, col in enumerate(columns_lower):
        for keyword in debit_keywords:
            if keyword == col or keyword in col:
                mapping['debit'] = columns[i]
                break
        if 'debit' in mapping:
            break
    
    # Search for credit column (Entrate)
    for i, col in enumerate(columns_lower):
        for keyword in credit_keywords:
            if keyword == col or keyword in col:
                mapping['credit'] = columns[i]
                break
        if 'credit' in mapping:
            break
    
    return mapping
