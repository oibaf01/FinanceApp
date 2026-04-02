"""
Database Migration Script for Finance Tracker v3.0
Migrates existing single-user database to multi-user schema
"""

import sqlite3
import sys
from pathlib import Path
from passlib.context import CryptContext


def migrate_database(db_path: str = "data/database.db", 
                     admin_email: str = "admin@example.com",
                     admin_password: str = "admin123"):
    """
    Migrate existing database to multi-user schema
    
    Args:
        db_path: Path to database file
        admin_email: Email for default admin user
        admin_password: Password for admin user (will be hashed)
    
    Returns:
        admin_user_id: ID of created admin user
    """
    
    # Ensure database file exists
    db_file = Path(db_path)
    if not db_file.parent.exists():
        db_file.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=" * 70)
    print("🔄 Finance Tracker Database Migration")
    print("=" * 70)
    print(f"Database: {db_path}")
    print("=" * 70)
    print()
    
    # ========================================================================
    # STEP 1: Create users table
    # ========================================================================
    print("STEP 1: Creating users table...")
    
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='users'
    """)
    
    if cursor.fetchone() is None:
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
        print("   ✅ Users table created")
    else:
        print("   ℹ️  Users table already exists")
    
    # ========================================================================
    # STEP 2: Create default admin user
    # ========================================================================
    print("\nSTEP 2: Creating default admin user...")
    
    cursor.execute("SELECT id FROM users WHERE email = ?", (admin_email,))
    existing_admin = cursor.fetchone()
    
    if existing_admin:
        admin_id = existing_admin[0]
        print(f"   ℹ️  Admin user already exists (ID: {admin_id})")
    else:
        # Hash password
        pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        hashed_password = pwd_context.hash(admin_password)
        
        cursor.execute("""
            INSERT INTO users (email, hashed_password, full_name)
            VALUES (?, ?, ?)
        """, (admin_email, hashed_password, "Admin User"))
        
        admin_id = cursor.lastrowid
        print(f"   ✅ Admin user created (ID: {admin_id})")
        print(f"      📧 Email: {admin_email}")
        print(f"      🔑 Password: {admin_password}")
        print(f"      ⚠️  CHANGE PASSWORD AFTER FIRST LOGIN!")
    
    # ========================================================================
    # STEP 3: Migrate transactions table
    # ========================================================================
    print("\nSTEP 3: Migrating transactions table...")
    
    # Check if user_id column exists
    cursor.execute("PRAGMA table_info(transactions)")
    columns = [col[1] for col in cursor.fetchall()]
    
    if 'user_id' not in columns:
        print("   Adding user_id column...")
        cursor.execute("ALTER TABLE transactions ADD COLUMN user_id INTEGER DEFAULT 1")
        
        # Assign all existing transactions to admin
        cursor.execute("UPDATE transactions SET user_id = ?", (admin_id,))
        
        # Get count
        cursor.execute("SELECT COUNT(*) FROM transactions")
        count = cursor.fetchone()[0]
        
        print(f"   ✅ {count} existing transactions assigned to admin user")
    else:
        cursor.execute("SELECT COUNT(*) FROM transactions WHERE user_id IS NULL")
        null_count = cursor.fetchone()[0]
        
        if null_count > 0:
            cursor.execute("UPDATE transactions SET user_id = ? WHERE user_id IS NULL", (admin_id,))
            print(f"   ✅ {null_count} transactions with NULL user_id assigned to admin")
        else:
            print("   ℹ️  user_id column already exists and populated")
    
    # ========================================================================
    # STEP 4: Migrate recurring_payments table
    # ========================================================================
    print("\nSTEP 4: Migrating recurring_payments table...")
    
    # Check if table exists
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='recurring_payments'
    """)
    
    if cursor.fetchone() is None:
        print("   ℹ️  recurring_payments table doesn't exist yet (will be created)")
    else:
        # Check if user_id column exists
        cursor.execute("PRAGMA table_info(recurring_payments)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'user_id' not in columns:
            print("   Adding user_id column...")
            cursor.execute("ALTER TABLE recurring_payments ADD COLUMN user_id INTEGER DEFAULT 1")
            
            # Assign all existing payments to admin
            cursor.execute("UPDATE recurring_payments SET user_id = ?", (admin_id,))
            
            # Get count
            cursor.execute("SELECT COUNT(*) FROM recurring_payments")
            count = cursor.fetchone()[0]
            
            print(f"   ✅ {count} existing recurring payments assigned to admin user")
        else:
            cursor.execute("SELECT COUNT(*) FROM recurring_payments WHERE user_id IS NULL")
            null_count = cursor.fetchone()[0]
            
            if null_count > 0:
                cursor.execute("UPDATE recurring_payments SET user_id = ? WHERE user_id IS NULL", (admin_id,))
                print(f"   ✅ {null_count} payments with NULL user_id assigned to admin")
            else:
                print("   ℹ️  user_id column already exists and populated")
    
    # ========================================================================
    # STEP 5: Create indexes for performance
    # ========================================================================
    print("\nSTEP 5: Creating performance indexes...")
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_transactions_user_id 
        ON transactions(user_id)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_transactions_date 
        ON transactions(date)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_transactions_category 
        ON transactions(category)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_recurring_payments_user_id 
        ON recurring_payments(user_id)
    """)
    
    print("   ✅ Indexes created")
    
    # ========================================================================
    # COMMIT AND CLOSE
    # ========================================================================
    conn.commit()
    
    # Get final statistics
    cursor.execute("SELECT COUNT(*) FROM users")
    user_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM transactions")
    tx_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM recurring_payments")
    rp_count = cursor.fetchone()[0]
    
    conn.close()
    
    # ========================================================================
    # SUMMARY
    # ========================================================================
    print("\n" + "=" * 70)
    print("✅ MIGRATION COMPLETED SUCCESSFULLY!")
    print("=" * 70)
    print(f"\n📊 Database Statistics:")
    print(f"   • Total users: {user_count}")
    print(f"   • Total transactions: {tx_count}")
    print(f"   • Total recurring payments: {rp_count}")
    print(f"\n👤 Admin User:")
    print(f"   • ID: {admin_id}")
    print(f"   • Email: {admin_email}")
    print(f"   • Password: {admin_password}")
    print(f"\n⚠️  IMPORTANT NEXT STEPS:")
    print(f"   1. Login with admin credentials")
    print(f"   2. Change admin password immediately")
    print(f"   3. Create accounts for other users")
    print(f"   4. Test that all data is accessible")
    print("\n" + "=" * 70)
    
    return admin_id


def verify_migration(db_path: str = "data/database.db"):
    """
    Verify that migration was successful
    
    Args:
        db_path: Path to database file
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("\n🔍 Verifying migration...")
    print("-" * 70)
    
    issues = []
    
    # Check users table
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='users'
    """)
    if cursor.fetchone() is None:
        issues.append("❌ Users table not found")
    else:
        print("✅ Users table exists")
    
    # Check user_id in transactions
    cursor.execute("PRAGMA table_info(transactions)")
    columns = [col[1] for col in cursor.fetchall()]
    if 'user_id' not in columns:
        issues.append("❌ user_id column missing in transactions table")
    else:
        print("✅ user_id column exists in transactions")
        
        # Check for NULL values
        cursor.execute("SELECT COUNT(*) FROM transactions WHERE user_id IS NULL")
        null_count = cursor.fetchone()[0]
        if null_count > 0:
            issues.append(f"⚠️  {null_count} transactions have NULL user_id")
        else:
            print("✅ All transactions have user_id")
    
    # Check user_id in recurring_payments
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='recurring_payments'
    """)
    if cursor.fetchone() is not None:
        cursor.execute("PRAGMA table_info(recurring_payments)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'user_id' not in columns:
            issues.append("❌ user_id column missing in recurring_payments table")
        else:
            print("✅ user_id column exists in recurring_payments")
            
            cursor.execute("SELECT COUNT(*) FROM recurring_payments WHERE user_id IS NULL")
            null_count = cursor.fetchone()[0]
            if null_count > 0:
                issues.append(f"⚠️  {null_count} recurring payments have NULL user_id")
            else:
                print("✅ All recurring payments have user_id")
    
    # Check indexes
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='index' AND name='idx_transactions_user_id'
    """)
    if cursor.fetchone() is None:
        issues.append("⚠️  Index idx_transactions_user_id not found")
    else:
        print("✅ Performance indexes created")
    
    conn.close()
    
    print("-" * 70)
    
    if issues:
        print("\n⚠️  Issues found:")
        for issue in issues:
            print(f"   {issue}")
        return False
    else:
        print("\n✅ Migration verification passed!")
        return True


if __name__ == "__main__":
    """
    Run migration when executed directly
    
    Usage:
        python migrate_database.py [db_path] [admin_email] [admin_password]
    
    Examples:
        python migrate_database.py
        python migrate_database.py data/database.db
        python migrate_database.py data/database.db admin@myapp.com mypassword123
    """
    
    # Parse command line arguments
    db_path = "data/database.db"
    admin_email = "admin@example.com"
    admin_password = "admin123"
    
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    
    if len(sys.argv) > 2:
        admin_email = sys.argv[2]
    
    if len(sys.argv) > 3:
        admin_password = sys.argv[3]
    
    try:
        # Run migration
        admin_id = migrate_database(db_path, admin_email, admin_password)
        
        # Verify migration
        verify_migration(db_path)
        
        print("\n✅ All done! You can now start the backend with multi-user support.")
        sys.exit(0)
        
    except Exception as e:
        print(f"\n❌ ERROR during migration: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
