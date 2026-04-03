"""
Script per creare un utente admin nel database
"""

import sys
sys.path.append('.')

from modules.database import Database
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Inizializza database
db = Database("data/database.db")

# Credenziali admin
ADMIN_EMAIL = "admin@example.com"
ADMIN_PASSWORD = "admin123"
ADMIN_NAME = "Admin User"

# Verifica se l'admin esiste già
existing_user = db.get_user_by_email(ADMIN_EMAIL)

if existing_user:
    print(f"✅ L'utente admin esiste già:")
    print(f"   Email: {existing_user['email']}")
    print(f"   Nome: {existing_user['full_name']}")
    print(f"   ID: {existing_user['id']}")
else:
    # Crea l'admin
    hashed_password = pwd_context.hash(ADMIN_PASSWORD)
    user_id = db.create_user(
        email=ADMIN_EMAIL,
        hashed_password=hashed_password,
        full_name=ADMIN_NAME
    )
    
    print(f"✅ Utente admin creato con successo!")
    print(f"   Email: {ADMIN_EMAIL}")
    print(f"   Password: {ADMIN_PASSWORD}")
    print(f"   Nome: {ADMIN_NAME}")
    print(f"   ID: {user_id}")

print("\n🔐 Usa queste credenziali per fare login:")
print(f"   Email: {ADMIN_EMAIL}")
print(f"   Password: {ADMIN_PASSWORD}")
