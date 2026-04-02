"""
Transaction Categorizer Module
Handles automatic and manual categorization of transactions.
"""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional


def categorize_transactions(
    transactions: List[Dict[str, Any]],
    rules_path: str,
    db
) -> List[Dict[str, Any]]:
    """
    Categorize transactions using rules and manual overrides.
    
    Args:
        transactions: List of transactions to categorize
        rules_path: Path to rules.json file
        db: Database instance
        
    Returns:
        List of categorized transactions
    """
    # Load categorization rules
    rules = load_rules(rules_path)
    
    # Get manual overrides from database
    manual_overrides = db.get_manual_overrides()
    
    categorized = []
    
    for tx in transactions:
        # Check for manual override first
        override = find_manual_override(tx, manual_overrides)
        
        if override:
            tx['category_auto'] = None
            tx['category_manual'] = override['category']
            tx['subcategory'] = override.get('subcategory')
            tx['type'] = override.get('type')
        else:
            # Apply automatic categorization
            category_info = apply_rules(tx, rules)
            tx['category_auto'] = category_info['category']
            tx['category_manual'] = None
            tx['subcategory'] = category_info.get('subcategory')
            tx['type'] = category_info.get('type')
            
            # Flag uncategorized for review
            if tx['category_auto'] == 'UNCATEGORIZED':
                tx['review_flag'] = True
        
        categorized.append(tx)
    
    return categorized


def load_rules(rules_path: str) -> List[Dict[str, Any]]:
    """
    Load categorization rules from JSON file.
    
    Args:
        rules_path: Path to rules.json
        
    Returns:
        List of rule dictionaries
    """
    rules_file = Path(rules_path)
    
    if not rules_file.exists():
        # Return empty rules if file doesn't exist
        return []
    
    try:
        with open(rules_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('rules', [])
    except Exception as e:
        print(f"Warning: Could not load rules from {rules_path}: {e}")
        return []


def apply_rules(
    transaction: Dict[str, Any], 
    rules: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Apply categorization rules to a transaction.
    
    Args:
        transaction: Transaction to categorize
        rules: List of categorization rules
        
    Returns:
        Dictionary with category, subcategory, and type
    """
    description = transaction['description_normalized'].upper()
    
    # First match wins
    for rule in rules:
        keywords = rule.get('keywords', [])
        
        for keyword in keywords:
            if keyword.upper() in description:
                return {
                    'category': rule.get('category', 'UNCATEGORIZED'),
                    'subcategory': rule.get('subcategory'),
                    'type': rule.get('type', 'expense')
                }
    
    # No match found
    return {
        'category': 'UNCATEGORIZED',
        'subcategory': None,
        'type': determine_type_from_amount(transaction['amount'])
    }


def determine_type_from_amount(amount: float) -> str:
    """
    Determine transaction type based on amount sign.
    
    Args:
        amount: Transaction amount
        
    Returns:
        'income' if positive, 'expense' if negative
    """
    return 'income' if amount > 0 else 'expense'


def find_manual_override(
    transaction: Dict[str, Any],
    overrides: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """
    Find manual categorization override for a transaction.
    
    Args:
        transaction: Transaction to check
        overrides: List of manual override dictionaries
        
    Returns:
        Override dictionary or None
    """
    description = transaction['description_normalized']
    
    for override in overrides:
        if override['description_normalized'] == description:
            return override
    
    return None