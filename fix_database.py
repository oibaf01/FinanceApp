"""
Script to fix the recurring_payments table
Run this once to recreate the table with correct schema
"""

import sqlite3
from pathlib import Path

# Database path
db_path = Path("data/database.db")

if not db_path.exists():
    print("❌ Database not found at data/database.db")
    print("Make sure you're running this from the project root directory")
    exit(1)

# Connect to database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("🔧 Fixing recurring_payments table...")

# Drop existing table if it exists
cursor.execute("DROP TABLE IF EXISTS recurring_payments")
print("✓ Dropped old table")

# Create new table with correct schema
cursor.execute("""
    CREATE TABLE recurring_payments (
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
        updated_at TEXT NOT NULL
    )
""")
print("✓ Created new table with correct schema")

conn.commit()
conn.close()

print("✅ Database fixed successfully!")
print("You can now restart the backend and add recurring payments.")
