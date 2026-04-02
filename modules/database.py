"""
Database Module for Finance Tracker v3.0
Multi-user support with SQLite
"""

import sqlite3
from typing import List, Dict, Optional, Any
from datetime import datetime
import json


class Database:
    """Database handler for Finance Tracker with multi-user support"""
    
    def __init__(self, db_path: str):
        """Initialize database connection and create tables"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        
        # Enable foreign keys
        self.cursor.execute("PRAGMA foreign_keys = ON")
        
        self._create_tables()
    
    def _create_tables(self):
        """Create all necessary tables"""
        
        # ====================================================================
        # USERS TABLE (NEW)
        # ====================================================================
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                hashed_password TEXT NOT NULL,
                full_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1
            )
        """)
        
        # ====================================================================
        # TRANSACTIONS TABLE (with user_id)
        # ====================================================================
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                description_raw TEXT NOT NULL,
                description_normalized TEXT,
                amount REAL NOT NULL,
                balance REAL,
                type TEXT,
                category TEXT,
                category_auto TEXT,
                category_manual TEXT,
                subcategory TEXT,
                review_flag INTEGER DEFAULT 0,
                import_batch_id TEXT,
                fingerprint_hash TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        """)
        
        # ====================================================================
        # RECURRING PAYMENTS TABLE (with user_id)
        # ====================================================================
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS recurring_payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                amount REAL NOT NULL,
                frequency TEXT NOT NULL,
                type TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT,
                description TEXT,
                category TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        """)
        
        # ====================================================================
        # INDEXES for performance
        # ====================================================================
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_transactions_user_id 
            ON transactions(user_id)
        """)
        
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_transactions_date 
            ON transactions(date)
        """)
        
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_transactions_category 
            ON transactions(category)
        """)
        
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_recurring_payments_user_id 
            ON recurring_payments(user_id)
        """)
        
        self.conn.commit()
    
    # ========================================================================
    # USER MANAGEMENT FUNCTIONS (NEW)
    # ========================================================================
    
    def create_user(self, email: str, hashed_password: str, full_name: str) -> int:
        """
        Create a new user
        
        Args:
            email: User email (unique)
            hashed_password: Bcrypt hashed password
            full_name: User's full name
            
        Returns:
            user_id: ID of created user
        """
        cursor = self.cursor.execute("""
            INSERT INTO users (email, hashed_password, full_name)
            VALUES (?, ?, ?)
        """, (email, hashed_password, full_name))
        
        self.conn.commit()
        return cursor.lastrowid
    
    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Get user by email
        
        Args:
            email: User email
            
        Returns:
            User dict or None if not found
        """
        cursor = self.cursor.execute("""
            SELECT id, email, hashed_password, full_name, created_at, is_active
            FROM users
            WHERE email = ?
        """, (email,))
        
        row = cursor.fetchone()
        if row:
            return {
                'id': row[0],
                'email': row[1],
                'hashed_password': row[2],
                'full_name': row[3],
                'created_at': row[4],
                'is_active': row[5]
            }
        return None
    
    def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Get user by ID
        
        Args:
            user_id: User ID
            
        Returns:
            User dict or None if not found
        """
        cursor = self.cursor.execute("""
            SELECT id, email, hashed_password, full_name, created_at, is_active
            FROM users
            WHERE id = ? AND is_active = 1
        """, (user_id,))
        
        row = cursor.fetchone()
        if row:
            return {
                'id': row[0],
                'email': row[1],
                'hashed_password': row[2],
                'full_name': row[3],
                'created_at': row[4],
                'is_active': row[5]
            }
        return None
    
    def update_user(self, user_id: int, **kwargs) -> bool:
        """
        Update user information
        
        Args:
            user_id: User ID
            **kwargs: Fields to update (email, full_name, hashed_password)
            
        Returns:
            True if updated, False otherwise
        """
        allowed_fields = ['email', 'full_name', 'hashed_password']
        updates = []
        values = []
        
        for key, value in kwargs.items():
            if key in allowed_fields:
                updates.append(f"{key} = ?")
                values.append(value)
        
        if not updates:
            return False
        
        values.append(user_id)
        query = f"UPDATE users SET {', '.join(updates)} WHERE id = ?"
        
        self.cursor.execute(query, values)
        self.conn.commit()
        
        return self.cursor.rowcount > 0
    
    def delete_user(self, user_id: int) -> bool:
        """
        Soft delete user (set is_active = 0)
        
        Args:
            user_id: User ID
            
        Returns:
            True if deleted, False otherwise
        """
        self.cursor.execute("""
            UPDATE users SET is_active = 0 WHERE id = ?
        """, (user_id,))
        
        self.conn.commit()
        return self.cursor.rowcount > 0
    
    # ========================================================================
    # TRANSACTION FUNCTIONS (MODIFIED for multi-user)
    # ========================================================================
    
    def insert_transactions(self, transactions: List[Dict[str, Any]]) -> int:
        """
        Insert multiple transactions
        
        Args:
            transactions: List of transaction dicts (must include user_id)
            
        Returns:
            Number of transactions inserted
        """
        inserted = 0
        
        for tx in transactions:
            try:
                self.cursor.execute("""
                    INSERT INTO transactions 
                    (user_id, date, description_raw, description_normalized, amount, balance,
                     type, category, category_auto, category_manual, subcategory,
                     review_flag, import_batch_id, fingerprint_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    tx.get('user_id'),
                    tx['date'],
                    tx['description_raw'],
                    tx.get('description_normalized'),
                    tx['amount'],
                    tx.get('balance'),
                    tx.get('type'),
                    tx.get('category'),
                    tx.get('category_auto'),
                    tx.get('category_manual'),
                    tx.get('subcategory'),
                    tx.get('review_flag', 0),
                    tx.get('import_batch_id'),
                    tx.get('fingerprint_hash')
                ))
                inserted += 1
            except sqlite3.IntegrityError:
                # Duplicate fingerprint, skip
                continue
        
        self.conn.commit()
        return inserted
    
    def get_all_transactions(self) -> List[Dict[str, Any]]:
        """
        Get ALL transactions (for backward compatibility)
        WARNING: In multi-user mode, use get_user_transactions instead
        
        Returns:
            List of all transactions
        """
        cursor = self.cursor.execute("""
            SELECT id, user_id, date, description_raw, description_normalized,
                   amount, balance, type, category, category_auto, category_manual,
                   subcategory, review_flag, import_batch_id, fingerprint_hash, created_at
            FROM transactions
            ORDER BY date DESC
        """)
        
        return self._parse_transactions(cursor.fetchall())
    
    def get_user_transactions(self, user_id: int) -> List[Dict[str, Any]]:
        """
        Get all transactions for a specific user
        
        Args:
            user_id: User ID
            
        Returns:
            List of user's transactions
        """
        cursor = self.cursor.execute("""
            SELECT id, user_id, date, description_raw, description_normalized,
                   amount, balance, type, category, category_auto, category_manual,
                   subcategory, review_flag, import_batch_id, fingerprint_hash, created_at
            FROM transactions
            WHERE user_id = ?
            ORDER BY date DESC
        """, (user_id,))
        
        return self._parse_transactions(cursor.fetchall())
    
    def _parse_transactions(self, rows: List[tuple]) -> List[Dict[str, Any]]:
        """Parse transaction rows into dicts"""
        transactions = []
        for row in rows:
            transactions.append({
                'id': row[0],
                'user_id': row[1],
                'date': row[2],
                'description_raw': row[3],
                'description_normalized': row[4],
                'amount': row[5],
                'balance': row[6],
                'type': row[7],
                'category': row[8],
                'category_auto': row[9],
                'category_manual': row[10],
                'subcategory': row[11],
                'review_flag': row[12],
                'import_batch_id': row[13],
                'fingerprint_hash': row[14],
                'created_at': row[15]
            })
        return transactions
    
    def get_transaction_by_id(self, transaction_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a specific transaction by ID
        
        Args:
            transaction_id: Transaction ID
            
        Returns:
            Transaction dict or None
        """
        cursor = self.cursor.execute("""
            SELECT id, user_id, date, description_raw, description_normalized,
                   amount, balance, type, category, category_auto, category_manual,
                   subcategory, review_flag, import_batch_id, fingerprint_hash, created_at
            FROM transactions
            WHERE id = ?
        """, (transaction_id,))
        
        row = cursor.fetchone()
        if row:
            return self._parse_transactions([row])[0]
        return None
    
    def update_category(self, transaction_id: int, category: str, 
                       subcategory: Optional[str] = None) -> bool:
        """
        Update transaction category
        
        Args:
            transaction_id: Transaction ID
            category: New category
            subcategory: New subcategory (optional)
            
        Returns:
            True if updated, False otherwise
        """
        self.cursor.execute("""
            UPDATE transactions 
            SET category = ?, category_manual = ?, subcategory = ?
            WHERE id = ?
        """, (category, category, subcategory, transaction_id))
        
        self.conn.commit()
        return self.cursor.rowcount > 0
    
    def delete_transactions(self, transaction_ids: List[int]) -> int:
        """
        Delete multiple transactions
        
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
    
    def check_duplicate(self, fingerprint_hash: str) -> bool:
        """
        Check if transaction with this fingerprint already exists
        
        Args:
            fingerprint_hash: Transaction fingerprint hash
            
        Returns:
            True if duplicate exists, False otherwise
        """
        cursor = self.cursor.execute("""
            SELECT COUNT(*) FROM transactions WHERE fingerprint_hash = ?
        """, (fingerprint_hash,))
        
        count = cursor.fetchone()[0]
        return count > 0
    
    # ========================================================================
    # RECURRING PAYMENTS FUNCTIONS (MODIFIED for multi-user)
    # ========================================================================
    
    def get_recurring_payments(self) -> List[Dict[str, Any]]:
        """
        Get ALL recurring payments (for backward compatibility)
        WARNING: In multi-user mode, use get_user_recurring_payments instead
        
        Returns:
            List of all recurring payments
        """
        cursor = self.cursor.execute("""
            SELECT id, user_id, name, amount, frequency, type, start_date, end_date,
                   description, category, is_active, created_at
            FROM recurring_payments
            ORDER BY created_at DESC
        """)
        
        return self._parse_recurring_payments(cursor.fetchall())
    
    def get_user_recurring_payments(self, user_id: int) -> List[Dict[str, Any]]:
        """
        Get all recurring payments for a specific user
        
        Args:
            user_id: User ID
            
        Returns:
            List of user's recurring payments
        """
        cursor = self.cursor.execute("""
            SELECT id, user_id, name, amount, frequency, type, start_date, end_date,
                   description, category, is_active, created_at
            FROM recurring_payments
            WHERE user_id = ?
            ORDER BY created_at DESC
        """, (user_id,))
        
        return self._parse_recurring_payments(cursor.fetchall())
    
    def _parse_recurring_payments(self, rows: List[tuple]) -> List[Dict[str, Any]]:
        """Parse recurring payment rows into dicts"""
        payments = []
        for row in rows:
            payments.append({
                'id': row[0],
                'user_id': row[1],
                'name': row[2],
                'amount': row[3],
                'frequency': row[4],
                'type': row[5],
                'start_date': row[6],
                'end_date': row[7],
                'description': row[8],
                'category': row[9],
                'is_active': row[10],
                'created_at': row[11]
            })
        return payments
    
    def get_recurring_payment(self, payment_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a specific recurring payment by ID
        
        Args:
            payment_id: Payment ID
            
        Returns:
            Payment dict or None
        """
        cursor = self.cursor.execute("""
            SELECT id, user_id, name, amount, frequency, type, start_date, end_date,
                   description, category, is_active, created_at
            FROM recurring_payments
            WHERE id = ?
        """, (payment_id,))
        
        row = cursor.fetchone()
        if row:
            return self._parse_recurring_payments([row])[0]
        return None
    
    def create_recurring_payment(self, user_id: int, name: str, amount: float, 
                                frequency: str, type: str, start_date: str,
                                end_date: Optional[str] = None,
                                description: Optional[str] = None,
                                category: Optional[str] = None) -> int:
        """
        Create a new recurring payment
        
        Args:
            user_id: User ID
            name: Payment name
            amount: Payment amount
            frequency: Payment frequency (monthly, yearly, weekly)
            type: Payment type (subscription, financing)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (optional)
            description: Payment description (optional)
            category: Payment category (optional)
            
        Returns:
            payment_id: ID of created payment
        """
        cursor = self.cursor.execute("""
            INSERT INTO recurring_payments 
            (user_id, name, amount, frequency, type, start_date, end_date, description, category)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (user_id, name, amount, frequency, type, start_date, end_date, description, category))
        
        self.conn.commit()
        return cursor.lastrowid
    
    def update_recurring_payment(self, payment_id: int, name: str, amount: float,
                                frequency: str, type: str, start_date: str,
                                end_date: Optional[str] = None,
                                description: Optional[str] = None,
                                category: Optional[str] = None) -> bool:
        """
        Update a recurring payment
        
        Args:
            payment_id: Payment ID
            name: Payment name
            amount: Payment amount
            frequency: Payment frequency
            type: Payment type
            start_date: Start date
            end_date: End date (optional)
            description: Description (optional)
            category: Category (optional)
            
        Returns:
            True if updated, False otherwise
        """
        self.cursor.execute("""
            UPDATE recurring_payments
            SET name = ?, amount = ?, frequency = ?, type = ?, 
                start_date = ?, end_date = ?, description = ?, category = ?
            WHERE id = ?
        """, (name, amount, frequency, type, start_date, end_date, description, category, payment_id))
        
        self.conn.commit()
        return self.cursor.rowcount > 0
    
    def delete_recurring_payment(self, payment_id: int) -> bool:
        """
        Delete a recurring payment
        
        Args:
            payment_id: Payment ID
            
        Returns:
            True if deleted, False otherwise
        """
        self.cursor.execute("""
            DELETE FROM recurring_payments WHERE id = ?
        """, (payment_id,))
        
        self.conn.commit()
        return self.cursor.rowcount > 0
    
    def toggle_recurring_payment_status(self, payment_id: int) -> bool:
        """
        Toggle active status of a recurring payment
        
        Args:
            payment_id: Payment ID
            
        Returns:
            True if toggled, False otherwise
        """
        self.cursor.execute("""
            UPDATE recurring_payments
            SET is_active = NOT is_active
            WHERE id = ?
        """, (payment_id,))
        
        self.conn.commit()
        return self.cursor.rowcount > 0
    
    # ========================================================================
    # UTILITY FUNCTIONS
    # ========================================================================
    
    def get_user_stats(self, user_id: int) -> Dict[str, Any]:
        """
        Get statistics for a user
        
        Args:
            user_id: User ID
            
        Returns:
            Dict with user statistics
        """
        cursor = self.cursor.execute("""
            SELECT 
                COUNT(*) as total_transactions,
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as total_income,
                SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) as total_expenses,
                MIN(date) as first_transaction_date,
                MAX(date) as last_transaction_date
            FROM transactions
            WHERE user_id = ?
        """, (user_id,))
        
        row = cursor.fetchone()
        
        return {
            'total_transactions': row[0] or 0,
            'total_income': row[1] or 0,
            'total_expenses': abs(row[2] or 0),
            'net_savings': (row[1] or 0) + (row[2] or 0),
            'first_transaction_date': row[3],
            'last_transaction_date': row[4]
        }
    
    def get_categories_for_user(self, user_id: int) -> List[str]:
        """
        Get unique categories used by a user
        
        Args:
            user_id: User ID
            
        Returns:
            List of unique category names
        """
        cursor = self.cursor.execute("""
            SELECT DISTINCT category
            FROM transactions
            WHERE user_id = ? AND category IS NOT NULL AND category != ''
            ORDER BY category
        """, (user_id,))
        
        return [row[0] for row in cursor.fetchall()]
    
    def vacuum(self):
        """Optimize database (reclaim space)"""
        self.cursor.execute("VACUUM")
        self.conn.commit()
    
    def close(self):
        """Close database connection"""
        self.conn.close()
    
    def __del__(self):
        """Destructor - ensure connection is closed"""
        try:
            self.conn.close()
        except:
            pass


# ============================================================================
# DATABASE MIGRATION UTILITY
# ============================================================================

def migrate_existing_database(db_path: str, admin_email: str = "admin@example.com", 
                              admin_password_hash: str = None):
    """
    Migrate existing database to multi-user schema
    
    Args:
        db_path: Path to database file
        admin_email: Email for default admin user
        admin_password_hash: Hashed password for admin (if None, will use default)
    
    Returns:
        admin_user_id: ID of created admin user
    """
    from passlib.context import CryptContext
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔄 Starting database migration to multi-user schema...")
    
    # 1. Check if users table exists
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='users'
    """)
    
    if cursor.fetchone() is None:
        print("1. Creating users table...")
        cursor.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                hashed_password TEXT NOT NULL,
                full_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1
            )
        """)
    else:
        print("1. Users table already exists")
    
    # 2. Create default admin user
    cursor.execute("SELECT id FROM users WHERE email = ?", (admin_email,))
    existing_admin = cursor.fetchone()
    
    if existing_admin:
        admin_id = existing_admin[0]
        print(f"2. Admin user already exists (ID: {admin_id})")
    else:
        print("2. Creating default admin user...")
        
        if admin_password_hash is None:
            pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
            admin_password_hash = pwd_context.hash("admin123")
        
        cursor.execute("""
            INSERT INTO users (email, hashed_password, full_name)
            VALUES (?, ?, ?)
        """, (admin_email, admin_password_hash, "Admin User"))
        
        admin_id = cursor.lastrowid
        print(f"   ✅ Admin user created (ID: {admin_id})")
        print(f"   📧 Email: {admin_email}")
        print(f"   🔑 Password: admin123 (CHANGE THIS!)")
    
    # 3. Add user_id to transactions table
    cursor.execute("PRAGMA table_info(transactions)")
    columns = [col[1] for col in cursor.fetchall()]
    
    if 'user_id' not in columns:
        print("3. Adding user_id column to transactions table...")
        cursor.execute("ALTER TABLE transactions ADD COLUMN user_id INTEGER DEFAULT 1")
        
        # Update all existing transactions to belong to admin
        cursor.execute("UPDATE transactions SET user_id = ?", (admin_id,))
        
        cursor.execute("SELECT COUNT(*) FROM transactions")
        count = cursor.fetchone()[0]
        print(f"   ✅ {count} existing transactions assigned to admin user")
    else:
        print("3. user_id column already exists in transactions")
    
    # 4. Add user_id to recurring_payments table
    cursor.execute("PRAGMA table_info(recurring_payments)")
    columns = [col[1] for col in cursor.fetchall()]
    
    if 'user_id' not in columns:
        print("4. Adding user_id column to recurring_payments table...")
        cursor.execute("ALTER TABLE recurring_payments ADD COLUMN user_id INTEGER DEFAULT 1")
        
        # Update all existing payments to belong to admin
        cursor.execute("UPDATE recurring_payments SET user_id = ?", (admin_id,))
        
        cursor.execute("SELECT COUNT(*) FROM recurring_payments")
        count = cursor.fetchone()[0]
        print(f"   ✅ {count} existing recurring payments assigned to admin user")
    else:
        print("4. user_id column already exists in recurring_payments")
    
    # 5. Create indexes
    print("5. Creating indexes for performance...")
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_transactions_user_id 
        ON transactions(user_id)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_recurring_payments_user_id 
        ON recurring_payments(user_id)
    """)
    
    conn.commit()
    conn.close()
    
    print("\n✅ Migration completed successfully!")
    print(f"\n📊 Summary:")
    print(f"   • Admin user ID: {admin_id}")
    print(f"   • Admin email: {admin_email}")
    print(f"   • Database: {db_path}")
    print(f"\n⚠️  IMPORTANT: Change admin password after first login!")
    
    return admin_id


# ============================================================================
# STANDALONE MIGRATION SCRIPT
# ============================================================================

if __name__ == "__main__":
    """Run database migration when executed directly"""
    import sys
    
    db_path = "data/database.db"
    
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    
    print("=" * 70)
    print("🗄️  Finance Tracker Database Migration Tool")
    print("=" * 70)
    print(f"Database: {db_path}")
    print("=" * 70)
    
    try:
        admin_id = migrate_existing_database(db_path)
        print("\n" + "=" * 70)
        print("✅ SUCCESS - Database is now multi-user ready!")
        print("=" * 70)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
