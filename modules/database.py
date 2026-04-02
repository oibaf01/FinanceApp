"""
Database Module
Handles SQLite database operations for transaction storage.
ORIGINAL CODE + Recurring Payments Support
"""

import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Set
from datetime import datetime


class Database:
    """SQLite database manager for transactions."""
    
    def __init__(self, db_path: str = "data/database.db"):
        """
        Initialize database connection and create tables if needed.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row  # Enable column access by name
        self.cursor = self.conn.cursor()
        
        self._create_tables()
    
    def _create_tables(self):
        """Create database tables if they don't exist."""
        # Transactions table (ORIGINAL - DO NOT MODIFY)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                description_raw TEXT,
                description_normalized TEXT NOT NULL,
                amount REAL NOT NULL,
                balance REAL,
                type TEXT,
                category TEXT,
                category_auto TEXT,
                category_manual TEXT,
                subcategory TEXT,
                review_flag BOOLEAN DEFAULT 0,
                fingerprint_hash TEXT UNIQUE NOT NULL,
                import_batch_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT
            )
        """)
        
        # Migration: Add 'category' column if it doesn't exist (for existing databases)
        try:
            self.cursor.execute("ALTER TABLE transactions ADD COLUMN category TEXT")
            self.conn.commit()
        except sqlite3.OperationalError:
            # Column already exists
            pass
        
        # Migration: Add 'import_batch_id' column if it doesn't exist
        try:
            self.cursor.execute("ALTER TABLE transactions ADD COLUMN import_batch_id TEXT")
            self.conn.commit()
        except sqlite3.OperationalError:
            # Column already exists
            pass
        
        # Index for faster lookups
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_fingerprint 
            ON transactions(fingerprint_hash)
        """)
        
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_date 
            ON transactions(date)
        """)
        
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_amount 
            ON transactions(amount)
        """)
        
        # NEW: Recurring payments table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS recurring_payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                amount REAL NOT NULL,
                frequency TEXT NOT NULL,
                type TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT,
                next_payment_date TEXT,
                is_active INTEGER DEFAULT 1,
                category TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT
            )
        """)
        
        self.conn.commit()
    
    # ========================================================================
    # ORIGINAL TRANSACTIONS METHODS - DO NOT MODIFY
    # ========================================================================
    
    def insert_transactions(self, transactions: List[Dict[str, Any]]) -> int:
        """
        Insert multiple transactions into database.
        
        Args:
            transactions: List of transaction dictionaries
            
        Returns:
            Number of transactions inserted
        """
        inserted_count = 0
        current_time = datetime.now().isoformat()
        
        for tx in transactions:
            try:
                # Determine final category
                final_category = tx.get('category_manual') or tx.get('category_auto') or tx.get('category')
                
                self.cursor.execute("""
                    INSERT INTO transactions (
                        date, description_raw, description_normalized,
                        amount, balance, type, category, category_auto, category_manual,
                        subcategory, review_flag, fingerprint_hash, import_batch_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    tx['date'],
                    tx['description_raw'],
                    tx['description_normalized'],
                    tx['amount'],
                    tx.get('balance'),
                    tx.get('type'),
                    final_category,
                    tx.get('category_auto'),
                    tx.get('category_manual'),
                    tx.get('subcategory'),
                    tx.get('review_flag', False),
                    tx['fingerprint_hash'],
                    tx.get('import_batch_id'),
                    current_time
                ))
                inserted_count += 1
            except sqlite3.IntegrityError:
                # Duplicate fingerprint, skip
                continue
        
        self.conn.commit()
        return inserted_count
    
    def get_all_fingerprints(self) -> Set[str]:
        """
        Get all existing transaction fingerprints.
        
        Returns:
            Set of fingerprint hashes
        """
        self.cursor.execute("SELECT fingerprint_hash FROM transactions")
        return {row['fingerprint_hash'] for row in self.cursor.fetchall()}
    
    def get_all_transactions(self) -> List[Dict[str, Any]]:
        """
        Get all transactions from database.
        
        Returns:
            List of transaction dictionaries
        """
        self.cursor.execute("""
            SELECT 
                id, date, description_raw, description_normalized,
                amount, balance, type, 
                COALESCE(category_manual, category_auto) as category,
                category_auto, category_manual,
                subcategory, review_flag, fingerprint_hash,
                created_at, updated_at
            FROM transactions
            ORDER BY date DESC
        """)
        
        return [dict(row) for row in self.cursor.fetchall()]
    
    def find_similar_transactions(
        self, 
        date_range: List[str], 
        amount: float
    ) -> List[Dict[str, Any]]:
        """
        Find transactions with same amount in date range.
        
        Args:
            date_range: List of dates to check
            amount: Amount to match
            
        Returns:
            List of matching transactions
        """
        placeholders = ','.join('?' * len(date_range))
        query = f"""
            SELECT * FROM transactions
            WHERE date IN ({placeholders})
            AND amount = ?
        """
        
        self.cursor.execute(query, date_range + [amount])
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_manual_overrides(self) -> List[Dict[str, Any]]:
        """
        Get all manually categorized transactions for reuse.
        
        Returns:
            List of manual override dictionaries
        """
        self.cursor.execute("""
            SELECT DISTINCT
                description_normalized,
                category_manual as category,
                subcategory,
                type
            FROM transactions
            WHERE category_manual IS NOT NULL
        """)
        
        return [dict(row) for row in self.cursor.fetchall()]
    
    def update_category(
        self, 
        transaction_id: int, 
        category: str, 
        subcategory: str = None
    ):
        """
        Manually update transaction category.
        
        Args:
            transaction_id: Transaction ID
            category: New category
            subcategory: New subcategory (optional)
        """
        current_time = datetime.now().isoformat()
        
        self.cursor.execute("""
            UPDATE transactions
            SET category = ?,
                category_manual = ?,
                subcategory = ?,
                updated_at = ?
            WHERE id = ?
        """, (category, category, subcategory, current_time, transaction_id))
        
        self.conn.commit()
    
    def delete_transactions(self, transaction_ids: List[int]) -> int:
        """
        Delete multiple transactions by IDs.
        
        Args:
            transaction_ids: List of transaction IDs to delete
            
        Returns:
            Number of transactions deleted
        """
        if not transaction_ids:
            return 0
        
        placeholders = ','.join('?' * len(transaction_ids))
        query = f"DELETE FROM transactions WHERE id IN ({placeholders})"
        
        self.cursor.execute(query, transaction_ids)
        self.conn.commit()
        
        return self.cursor.rowcount
    
    def clear_all_transactions(self):
        """Delete all transactions from database."""
        self.cursor.execute("DELETE FROM transactions")
        self.conn.commit()
    
    # ========================================================================
    # NEW: RECURRING PAYMENTS METHODS
    # ========================================================================
    
    def get_recurring_payments(self) -> List[Dict[str, Any]]:
        """Get all recurring payments"""
        self.cursor.execute("""
            SELECT 
                id, name, description, amount, frequency, type,
                start_date, end_date, next_payment_date, is_active,
                category, created_at, updated_at
            FROM recurring_payments
            ORDER BY name
        """)
        
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_recurring_payment(self, payment_id: int) -> Dict[str, Any]:
        """Get a specific recurring payment"""
        self.cursor.execute("""
            SELECT 
                id, name, description, amount, frequency, type,
                start_date, end_date, next_payment_date, is_active,
                category, created_at, updated_at
            FROM recurring_payments
            WHERE id = ?
        """, (payment_id,))
        
        row = self.cursor.fetchone()
        return dict(row) if row else None
    
    def create_recurring_payment(
        self,
        name: str,
        amount: float,
        frequency: str,
        type: str,
        start_date: str,
        end_date: str = None,
        description: str = None,
        category: str = None
    ) -> int:
        """Create a new recurring payment"""
        current_time = datetime.now().isoformat()
        
        self.cursor.execute("""
            INSERT INTO recurring_payments (
                name, description, amount, frequency, type,
                start_date, end_date, category, is_active,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        """, (name, description, amount, frequency, type, start_date, end_date, category, current_time, current_time))
        
        self.conn.commit()
        return self.cursor.lastrowid
    
    def update_recurring_payment(
        self,
        payment_id: int,
        name: str,
        amount: float,
        frequency: str,
        type: str,
        start_date: str,
        end_date: str = None,
        description: str = None,
        category: str = None
    ):
        """Update a recurring payment"""
        current_time = datetime.now().isoformat()
        
        self.cursor.execute("""
            UPDATE recurring_payments
            SET name = ?, description = ?, amount = ?, frequency = ?,
                type = ?, start_date = ?, end_date = ?, category = ?,
                updated_at = ?
            WHERE id = ?
        """, (name, description, amount, frequency, type, start_date, end_date, category, current_time, payment_id))
        
        self.conn.commit()
    
    def delete_recurring_payment(self, payment_id: int):
        """Delete a recurring payment"""
        self.cursor.execute("DELETE FROM recurring_payments WHERE id = ?", (payment_id,))
        self.conn.commit()
    
    def toggle_recurring_payment(self, payment_id: int, is_active: bool):
        """Toggle active status of a recurring payment"""
        current_time = datetime.now().isoformat()
        
        self.cursor.execute("""
            UPDATE recurring_payments
            SET is_active = ?, updated_at = ?
            WHERE id = ?
        """, (1 if is_active else 0, current_time, payment_id))
        
        self.conn.commit()
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    def close(self):
        """Close database connection."""
        self.conn.close()
