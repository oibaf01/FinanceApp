"""
Script to fix admin password after migration
Run this AFTER migrate_simple.py to set proper bcrypt hash
"""

import sqlite3
from passlib.context import CryptContext

def fix_admin_password(db_path="data/database.db", admin_email="admin@example.com", new_password="admin123"):
    """Update admin password with proper bcrypt hash"""
    
    print("🔧 Fixing admin password with bcrypt...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get admin user
    cursor.execute("SELECT id FROM users WHERE email = ?", (admin_email,))
    admin = cursor.fetchone()
    
    if not admin:
        print(f"❌ Admin user {admin_email} not found!")
        return False
    
    admin_id = admin[0]
    
    # Create proper bcrypt hash with safe settings
    pwd_context = CryptContext(
        schemes=["bcrypt"],
        deprecated="auto",
        bcrypt__rounds=12,
        bcrypt__ident="2b"
    )
    
    # Hash password (ensure it's within limits)
    safe_password = new_password[:50]  # Keep it well under 72 bytes
    
    try:
        hashed_password = pwd_context.hash(safe_password)
        
        # Update database
        cursor.execute("""
            UPDATE users 
            SET hashed_password = ? 
            WHERE id = ?
        """, (hashed_password, admin_id))
        
        conn.commit()
        conn.close()
        
        print(f"✅ Admin password updated successfully!")
        print(f"   Email: {admin_email}")
        print(f"   Password: {new_password}")
        print(f"   Hash: {hashed_password[:50]}...")
        
        return True
        
    except Exception as e:
        print(f"❌ Error hashing password: {e}")
        print(f"\nTrying alternative method...")
        
        # Alternative: use argon2 instead
        try:
            pwd_context_alt = CryptContext(schemes=["argon2"], deprecated="auto")
            hashed_password = pwd_context_alt.hash(safe_password)
            
            cursor.execute("""
                UPDATE users 
                SET hashed_password = ? 
                WHERE id = ?
            """, (hashed_password, admin_id))
            
            conn.commit()
            conn.close()
            
            print(f"✅ Admin password updated with argon2!")
            print(f"   Email: {admin_email}")
            print(f"   Password: {new_password}")
            
            return True
            
        except Exception as e2:
            print(f"❌ Alternative method also failed: {e2}")
            conn.close()
            return False

if __name__ == "__main__":
    import sys
    
    db_path = "data/database.db"
    admin_email = "admin@example.com"
    admin_password = "admin123"
    
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    if len(sys.argv) > 2:
        admin_email = sys.argv[2]
    if len(sys.argv) > 3:
        admin_password = sys.argv[3]
    
    success = fix_admin_password(db_path, admin_email, admin_password)
    
    if success:
        print("\n✅ Done! You can now login with the admin credentials.")
        sys.exit(0)
    else:
        print("\n❌ Failed to fix password. See errors above.")
        sys.exit(1)
