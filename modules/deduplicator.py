"""
Transaction Deduplicator Module
Handles duplicate detection and fingerprinting.
"""

import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple


def deduplicate_transactions(
    transactions: List[Dict[str, Any]], 
    db
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Remove duplicate transactions and flag near-duplicates.
    
    Args:
        transactions: List of normalized transactions
        db: Database instance
        
    Returns:
        Tuple of (unique_transactions, duplicate_count)
    """
    unique_transactions = []
    duplicate_count = 0
    
    # Get existing fingerprints from database
    existing_fingerprints = db.get_all_fingerprints()
    
    for tx in transactions:
        # Generate fingerprint
        fingerprint = generate_fingerprint(tx)
        tx['fingerprint_hash'] = fingerprint
        
        # Check if already exists
        if fingerprint in existing_fingerprints:
            duplicate_count += 1
            continue
        
        # Check for near-duplicates
        tx['review_flag'] = check_near_duplicate(tx, db)
        
        unique_transactions.append(tx)
        existing_fingerprints.add(fingerprint)
    
    return unique_transactions, duplicate_count


def generate_fingerprint(transaction: Dict[str, Any]) -> str:
    """
    Generate a unique fingerprint for a transaction using SHA256.
    
    Args:
        transaction: Normalized transaction dictionary
        
    Returns:
        SHA256 hash string
    """
    # Combine date, amount, and normalized description
    date = transaction['date']
    amount = f"{transaction['amount']:.2f}"  # Normalize to 2 decimals
    description = transaction['description_normalized']
    
    # Create fingerprint string
    fingerprint_str = f"{date}|{amount}|{description}"
    
    # Generate SHA256 hash
    return hashlib.sha256(fingerprint_str.encode('utf-8')).hexdigest()


def check_near_duplicate(transaction: Dict[str, Any], db) -> bool:
    """
    Check if transaction is a near-duplicate (same amount, similar date ±1 day).
    
    Args:
        transaction: Transaction to check
        db: Database instance
        
    Returns:
        True if near-duplicate detected, False otherwise
    """
    try:
        tx_date = datetime.strptime(transaction['date'], '%Y-%m-%d')
        tx_amount = transaction['amount']
        
        # Check dates ±1 day
        date_range = [
            (tx_date - timedelta(days=1)).strftime('%Y-%m-%d'),
            tx_date.strftime('%Y-%m-%d'),
            (tx_date + timedelta(days=1)).strftime('%Y-%m-%d')
        ]
        
        # Query database for similar transactions
        similar_transactions = db.find_similar_transactions(
            date_range=date_range,
            amount=tx_amount
        )
        
        # If found similar transactions, flag for review
        return len(similar_transactions) > 0
    
    except Exception:
        return False