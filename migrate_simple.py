"""
Simple Database Migration Script - No bcrypt issues
"""

import sqlite3
import hashlib
from pathlib import Path

def simple_hash(password):
    """Simple SHA256 hash - we'll use bcrypt in the backend"""
    # This is just for migration, backend will use bcrypt properly
    return hashlib.sha256(password.encode()).hexdigest()

def migrate_database(db_path="data/database.db"):
    """Migrate database to multi-user schema"""
    
    # Ensure database directory exists
    db_file = Path(db_path)
    if not db_file.parent.exists():
        db_file.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=" * 70)
    print("🔄 Finance Tracker Database Migration (Simple)")
    print("=" * 70)
    print(f"Database: {db_path}")
    print("=" * 70)
    print()
    
    # STEP 1: Create users table
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
    
    # STEP 2: Create admin user with simple hash
    print("\nSTEP 2: Creating default admin user...")
    
    admin_email = "admin@example.com"
    cursor.execute("SELECT id FROM users WHERE email = ?", (admin_email,))
    existing_admin = cursor.fetchone()
    
    if existing_admin:
        admin_id = existing_admin[0]
        print(f"   ℹ️  Admin user already exists (ID: {admin_id})")
    else:
        # Use simple hash for now - backend will handle bcrypt properly
        temp_hash = simple_hash("admin123")
        
        cursor.execute("""
            INSERT INTO users (email, hashed_password, full_name)
            VALUES (?, ?, ?)
        """, (admin_email, temp_hash, "Admin User"))
        
        admin_id = cursor.lastrowid
        print(f"   ✅ Admin user created (ID: {admin_id})")
        print(f"      📧 Email: {admin_email}")
        print(f"      🔑 Temporary password: admin123")
        print(f"      ⚠️  You'll need to reset password on first login!")
    
    # STEP 3: Add user_id to transactions
    print("\nSTEP 3: Migrating transactions table...")
    
    cursor.execute("PRAGMA table_info(transactions)")
    columns = [col[1] for col in cursor.fetchall()]
    
    if 'user_id' not in columns:
        print("   Adding user_id column...")
        cursor.execute("ALTER TABLE transactions ADD COLUMN user_id INTEGER DEFAULT 1")
        cursor.execute("UPDATE transactions SET user_id = ?", (admin_id,))
        
        cursor.execute("SELECT COUNT(*) FROM transactions")
        count = cursor.fetchone()[0]
        print(f"   ✅ {count} existing transactions assigned to admin user")
    else:
        cursor.execute("SELECT COUNT(*) FROM transactions WHERE user_id IS NULL")
        null_count = cursor.fetchone()[0]
        
        if null_count > 0:
            cursor.execute("UPDATE transactions SET user_id = ? WHERE user_id IS NULL", (admin_id,))
            print(f"   ✅ {null_count} transactions assigned to admin")
        else:
            print("   ℹ️  user_id column already exists and populated")
    
    # STEP 4: Add user_id to recurring_payments
    print("\nSTEP 4: Migrating recurring_payments table...")
    
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='recurring_payments'
    """)
    
    if cursor.fetchone() is None:
        print("   ℹ️  recurring_payments table doesn't exist yet")
    else:
        cursor.execute("PRAGMA table_info(recurring_payments)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'user_id' not in columns:
            print("   Adding user_id column...")
            cursor.execute("ALTER TABLE recurring_payments ADD COLUMN user_id INTEGER DEFAULT 1")
            cursor.execute("UPDATE recurring_payments SET user_id = ?", (admin_id,))
            
            cursor.execute("SELECT COUNT(*) FROM recurring_payments")
            count = cursor.fetchone()[0]
            print(f"   ✅ {count} existing recurring payments assigned to admin user")
        else:
            cursor.execute("SELECT COUNT(*) FROM recurring_payments WHERE user_id IS NULL")
            null_count = cursor.fetchone()[0]
            
            if null_count > 0:
                cursor.execute("UPDATE recurring_payments SET user_id = ? WHERE user_id IS NULL", (admin_id,))
                print(f"   ✅ {null_count} payments assigned to admin")
            else:
                print("   ℹ️  user_id column already exists and populated")
    
    # STEP 5: Create indexes
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
        CREATE INDEX IF NOT EXISTS idx_recurring_payments_user_id 
        ON recurring_payments(user_id)
    """)
    
    print("   ✅ Indexes created")
    
    # Commit and close
    conn.commit()
    
    # Get statistics
    cursor.execute("SELECT COUNT(*) FROM users")
    user_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM transactions")
    tx_count = cursor.fetchone()[0]
    
    conn.close()
    
    # Summary
    print("\n" + "=" * 70)
    print("✅ MIGRATION COMPLETED SUCCESSFULLY!")
    print("=" * 70)
    print(f"\n📊 Database Statistics:")
    print(f"   • Total users: {user_count}")
    print(f"   • Total transactions: {tx_count}")
    print(f"\n👤 Admin User:")
    print(f"   • ID: {admin_id}")
    print(f"   • Email: {admin_email}")
    print(f"   • Temporary Password: admin123")
    print(f"\n⚠️  IMPORTANT:")
    print(f"   The admin password uses temporary hashing.")
    print(f"   After first login, you MUST change the password!")
    print(f"   The backend will then use proper bcrypt hashing.")
    print("\n" + "=" * 70)
    
    return admin_id

if __name__ == "__main__":
    import sys
    
    db_path = "data/database.db"
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    
    try:
        migrate_database(db_path)
        print("\n✅ Migration successful! You can now start the backend.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
