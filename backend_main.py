"""
Finance Tracker - FastAPI Backend v3.0
Enhanced with MULTI-USER AUTHENTICATION + all existing features
"""

from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from pydantic import BaseModel, EmailStr
from passlib.context import CryptContext
from jose import JWTError, jwt
import uvicorn
import os
import json
from pathlib import Path
import hashlib
import re

# Import our existing modules
from modules.parser import parse_file
from modules.normalizer import normalize_transactions
from modules.deduplicator import deduplicate_transactions
from modules.categorizer import categorize_transactions
from modules.database import Database
from modules.reporter import generate_report

# ============================================================================
# AUTHENTICATION CONFIGURATION
# ============================================================================

SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-CHANGE-IN-PRODUCTION")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = 7

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()

# ============================================================================
# APP INITIALIZATION
# ============================================================================

app = FastAPI(
    title="Personal Finance Tracker API v3.0",
    description="Multi-user API with authentication for managing personal finances",
    version="3.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production: specify your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database instance
db = Database("data/database.db")

# Ensure directories exist
Path("uploads").mkdir(exist_ok=True)
Path("reports").mkdir(exist_ok=True)
Path("config").mkdir(exist_ok=True)

# ============================================================================
# AUTHENTICATION MODELS
# ============================================================================

class UserCreate(BaseModel):
    email: EmailStr
    password: str
    full_name: str

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str
    user: dict

# ============================================================================
# EXISTING PYDANTIC MODELS
# ============================================================================

class Transaction(BaseModel):
    id: Optional[int] = None
    date: str
    description_raw: str
    description_normalized: str
    amount: float
    balance: Optional[float] = None
    type: Optional[str] = None
    category: Optional[str] = None
    category_auto: Optional[str] = None
    category_manual: Optional[str] = None
    subcategory: Optional[str] = None
    review_flag: bool = False
    import_batch_id: Optional[str] = None

class CategoryRule(BaseModel):
    keywords: List[str]
    category: str
    subcategory: Optional[str] = None
    type: str  # income, expense, transfer
    priority: Optional[int] = 1

class CategoryUpdate(BaseModel):
    transaction_id: int
    category: str
    subcategory: Optional[str] = None
    learn: bool = True

class Budget(BaseModel):
    category: str
    monthly_limit: float
    alert_threshold: float = 0.8

class ImportBatch(BaseModel):
    batch_id: str
    filename: str
    upload_date: str
    transaction_count: int
    date_range: Dict[str, str]

class TransactionCreate(BaseModel):
    date: str
    description: str
    amount: float
    type: str  # "income" or "expense"

class RecurringPaymentCreate(BaseModel):
    name: str
    amount: float
    frequency: str  # monthly, yearly, weekly
    type: str  # subscription, financing
    start_date: str
    end_date: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None

# ============================================================================
# AUTHENTICATION HELPER FUNCTIONS
# ============================================================================

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get current authenticated user from JWT token"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Credenziali non valide",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    payload = decode_token(credentials.credentials)
    if payload is None:
        raise credentials_exception
    
    user_id: int = payload.get("sub")
    if user_id is None:
        raise credentials_exception
    
    # Get user from database
    user = db.get_user_by_id(user_id)
    if user is None:
        raise credentials_exception
    
    return user

# ============================================================================
# EXISTING HELPER FUNCTIONS (UNCHANGED)
# ============================================================================

def load_rules() -> Dict:
    """Load categorization rules from JSON"""
    rules_path = Path("config/rules.json")
    if rules_path.exists():
        with open(rules_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"rules": [], "learned_rules": []}

def save_rules(rules_data: Dict):
    """Save categorization rules to JSON"""
    rules_path = Path("config/rules.json")
    with open(rules_path, 'w', encoding='utf-8') as f:
        json.dump(rules_data, f, indent=2, ensure_ascii=False)

def smart_categorize_with_ai(description: str, user_categories: List[str]) -> Dict[str, Any]:
    """AI-powered categorization (UNCHANGED)"""
    description_clean = description.upper().strip()
    rules_data = load_rules()
    
    # Priority 1: Manual rules
    for rule in rules_data.get('rules', []):
        for keyword in rule.get('keywords', []):
            if keyword.upper() in description_clean:
                return {
                    'category': rule['category'],
                    'confidence': 0.95,
                    'method': 'manual_rule',
                    'matched_keyword': keyword
                }
    
    # Priority 2: Learned rules
    for rule in rules_data.get('learned_rules', []):
        for keyword in rule.get('keywords', []):
            if keyword.upper() in description_clean:
                return {
                    'category': rule['category'],
                    'confidence': 0.90,
                    'method': 'learned_rule',
                    'matched_keyword': keyword
                }
    
    # Priority 3: Generic patterns
    category_patterns = {
        'ALIMENTARI': ['MARKET', 'SUPER', 'CONAD', 'COOP', 'ESSELUNGA', 'CARREFOUR', 'LIDL', 'EUROSPIN'],
        'RISTORANTI': ['RISTORANTE', 'PIZZERIA', 'BAR', 'CAFE', 'MCDONALD', 'BURGER'],
        'TRASPORTI': ['BENZINA', 'DIESEL', 'ENI', 'SHELL', 'TRENITALIA', 'ATM', 'METRO'],
        'SHOPPING': ['ZARA', 'H&M', 'NIKE', 'AMAZON', 'IKEA'],
        'UTENZE': ['ENEL', 'GAS', 'ACQUA', 'VODAFONE', 'TIM'],
    }
    
    for pattern_cat, keywords in category_patterns.items():
        for keyword in keywords:
            if keyword in description_clean:
                matching_user_cat = next((uc for uc in user_categories if uc.upper() == pattern_cat), None)
                if matching_user_cat:
                    return {'category': matching_user_cat, 'confidence': 0.75, 'method': 'generic_pattern'}
    
    return {'category': 'UNCATEGORIZED', 'confidence': 0.0, 'method': 'no_match'}

def learn_from_categorization(description: str, category: str, subcategory: Optional[str] = None):
    """Learn from manual categorization (UNCHANGED)"""
    rules_data = load_rules()
    words = description.upper().split()
    
    common_words = {
        'DEL', 'ITA', 'SRL', 'SPA', 'OPERAZIONE', 'CARTA', 'THE', 'AND', 'DI', 'DA',
        'CON', 'PER', 'SU', 'IN', 'A', 'IL', 'LA', 'LE', 'I', 'GLI', 'UN', 'UNA'
    }
    
    keywords = []
    for word in words:
        clean_word = re.sub(r'[^A-Z]', '', word)
        if len(clean_word) > 3 and clean_word not in common_words:
            keywords.append(clean_word.lower())
    
    keywords = list(set(keywords))[:5]
    
    if not keywords:
        return
    
    if 'learned_rules' not in rules_data:
        rules_data['learned_rules'] = []
    
    existing_rule = None
    for rule in rules_data['learned_rules']:
        if rule['category'] == category:
            for kw in keywords:
                if kw.lower() not in [k.lower() for k in rule['keywords']]:
                    rule['keywords'].append(kw.lower())
            existing_rule = rule
            break
    
    if not existing_rule:
        new_rule = {
            "keywords": [kw.lower() for kw in keywords],
            "category": category,
            "subcategory": subcategory,
            "type": "expense",
            "learned": True,
            "learn_count": 1,
            "created_at": datetime.now().isoformat()
        }
        rules_data['learned_rules'].append(new_rule)
    else:
        existing_rule['learn_count'] = existing_rule.get('learn_count', 1) + 1
        existing_rule['updated_at'] = datetime.now().isoformat()
    
    save_rules(rules_data)

def get_import_batches() -> List[Dict]:
    """Get list of all import batches (UNCHANGED)"""
    try:
        cursor = db.cursor.execute("""
            SELECT 
                import_batch_id,
                COUNT(*) as count,
                MIN(date) as min_date,
                MAX(date) as max_date,
                MIN(created_at) as upload_date
            FROM transactions
            WHERE import_batch_id IS NOT NULL
            GROUP BY import_batch_id
            ORDER BY upload_date DESC
        """)
        
        batches = []
        for row in cursor.fetchall():
            batches.append({
                'batch_id': row[0],
                'transaction_count': row[1],
                'date_range': {'start': row[2], 'end': row[3]},
                'upload_date': row[4]
            })
        
        return batches
    except:
        return []

# ============================================================================
# AUTHENTICATION ENDPOINTS (NEW)
# ============================================================================

@app.post("/api/auth/register", response_model=Token)
async def register(user: UserCreate):
    """Register a new user"""
    # Check if email already exists
    existing_user = db.get_user_by_email(user.email)
    if existing_user:
        raise HTTPException(status_code=400, detail="Email già registrata")
    
    # Create new user
    hashed_password = get_password_hash(user.password)
    user_id = db.create_user(
        email=user.email,
        hashed_password=hashed_password,
        full_name=user.full_name
    )
    
    # Create token
    access_token = create_access_token(data={"sub": user_id})
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": user_id,
            "email": user.email,
            "full_name": user.full_name
        }
    }

@app.post("/api/auth/login", response_model=Token)
async def login(user: UserLogin):
    """Login user"""
    db_user = db.get_user_by_email(user.email)
    if not db_user or not verify_password(user.password, db_user['hashed_password']):
        raise HTTPException(status_code=401, detail="Email o password non corretti")
    
    # Create token
    access_token = create_access_token(data={"sub": db_user['id']})
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": db_user['id'],
            "email": db_user['email'],
            "full_name": db_user['full_name']
        }
    }

@app.get("/api/auth/me")
async def get_me(current_user: dict = Depends(get_current_user)):
    """Get current user info"""
    return {
        "id": current_user['id'],
        "email": current_user['email'],
        "full_name": current_user['full_name']
    }

# ============================================================================
# ROOT & INFO ENDPOINTS
# ============================================================================

@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "message": "Personal Finance Tracker API v3.0 - Multi-User Edition",
        "version": "3.0.0",
        "features": [
            "Multi-user authentication with JWT",
            "User-isolated data",
            "Multi-file import with batch tracking",
            "Smart category learning",
            "Advanced filtering and analytics",
            "Budget tracking",
            "Custom period reports"
        ]
    }

# ============================================================================
# TRANSACTIONS ENDPOINTS (PROTECTED)
# ============================================================================

@app.get("/api/transactions")
async def get_transactions(
    current_user: dict = Depends(get_current_user),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    min_amount: Optional[float] = Query(None),
    max_amount: Optional[float] = Query(None),
    review_only: bool = Query(False),
    batch_id: Optional[str] = Query(None),
    limit: int = Query(1000)
):
    """Get transactions for current user with filters"""
    try:
        # Get only current user's transactions
        transactions = db.get_user_transactions(current_user['id'])
        
        # Apply filters (same as before)
        if start_date:
            transactions = [t for t in transactions if t['date'] >= start_date]
        if end_date:
            transactions = [t for t in transactions if t['date'] <= end_date]
        if category:
            transactions = [t for t in transactions if t.get('category', '').upper() == category.upper()]
        if min_amount is not None:
            transactions = [t for t in transactions if t['amount'] >= min_amount]
        if max_amount is not None:
            transactions = [t for t in transactions if t['amount'] <= max_amount]
        if review_only:
            transactions = [t for t in transactions if t.get('review_flag')]
        if batch_id:
            transactions = [t for t in transactions if t.get('import_batch_id') == batch_id]
        
        transactions.sort(key=lambda x: x['date'], reverse=True)
        return transactions[:limit]
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/transactions")
async def create_transaction(
    transaction: TransactionCreate,
    current_user: dict = Depends(get_current_user)
):
    """Create a new transaction for current user"""
    try:
        if not transaction.description.strip():
            raise HTTPException(status_code=400, detail="Description cannot be empty")
        
        if transaction.type not in ["income", "expense"]:
            raise HTTPException(status_code=400, detail="Type must be 'income' or 'expense'")
        
        final_amount = abs(transaction.amount) if transaction.type == "income" else -abs(transaction.amount)
        
        # Get user categories
        user_categories = await get_user_categories(current_user)
        
        # AI categorization
        ai_result = smart_categorize_with_ai(transaction.description, user_categories)
        suggested_category = ai_result.get('category', 'UNCATEGORIZED')
        
        fingerprint_string = f"{transaction.date}_{transaction.description.upper().strip()}_{final_amount}"
        fingerprint_hash = hashlib.md5(fingerprint_string.encode()).hexdigest()
        
        new_transaction = {
            'user_id': current_user['id'],  # IMPORTANT: Add user_id
            'date': transaction.date,
            'description_raw': transaction.description,
            'description_normalized': transaction.description.upper().strip(),
            'amount': final_amount,
            'balance': None,
            'type': transaction.type,
            'category': suggested_category,
            'category_auto': suggested_category,
            'category_manual': None,
            'subcategory': None,
            'review_flag': False,
            'import_batch_id': f"MANUAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'fingerprint_hash': fingerprint_hash
        }
        
        db.insert_transactions([new_transaction])
        
        return {
            "message": "Transaction created successfully",
            "transaction": new_transaction,
            "ai_suggestion": ai_result
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/transactions/{transaction_id}/category")
async def update_transaction_category(
    transaction_id: int,
    update: CategoryUpdate,
    current_user: dict = Depends(get_current_user)
):
    """Update transaction category (only if belongs to current user)"""
    try:
        # Verify transaction belongs to user
        transactions = db.get_user_transactions(current_user['id'])
        transaction = next((t for t in transactions if t['id'] == transaction_id), None)
        
        if not transaction:
            raise HTTPException(status_code=404, detail="Transaction not found")
        
        db.update_category(
            transaction_id=transaction_id,
            category=update.category,
            subcategory=update.subcategory
        )
        
        if update.learn:
            learn_from_categorization(
                transaction['description_normalized'],
                update.category,
                update.subcategory
            )
        
        return {"message": "Category updated successfully", "learned": update.learn}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/transactions/delete-multiple")
async def delete_multiple_transactions(
    transaction_ids: List[int],
    current_user: dict = Depends(get_current_user)
):
    """Delete multiple transactions (only if belong to current user)"""
    try:
        # Verify all transactions belong to user
        user_transactions = db.get_user_transactions(current_user['id'])
        user_tx_ids = {t['id'] for t in user_transactions}
        
        # Filter to only delete user's transactions
        valid_ids = [tid for tid in transaction_ids if tid in user_tx_ids]
        
        if not valid_ids:
            raise HTTPException(status_code=403, detail="No valid transactions to delete")
        
        deleted_count = db.delete_transactions(valid_ids)
        return {"message": f"{deleted_count} transazioni eliminate", "deleted_count": deleted_count}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# CATEGORIES & RULES ENDPOINTS (PROTECTED)
# ============================================================================

@app.get("/api/categories")
async def get_categories(current_user: dict = Depends(get_current_user)):
    """Get categories for current user"""
    try:
        transactions = db.get_user_transactions(current_user['id'])
        
        categories = {}
        for t in transactions:
            cat = t.get('category') or 'UNCATEGORIZED'
            
            if cat not in categories:
                categories[cat] = {
                    'name': cat,
                    'type': t.get('type', 'expense'),
                    'subcategories': set(),
                    'count': 0,
                    'total': 0
                }
            
            categories[cat]['count'] += 1
            categories[cat]['total'] += t['amount']
            
            subcat = t.get('subcategory')
            if subcat:
                categories[cat]['subcategories'].add(subcat)
        
        result = []
        for cat_name, cat_data in categories.items():
            result.append({
                'name': cat_name,
                'type': cat_data['type'],
                'subcategories': list(cat_data['subcategories']),
                'transaction_count': cat_data['count'],
                'total_amount': cat_data['total'],
                'average_amount': cat_data['total'] / cat_data['count'] if cat_data['count'] > 0 else 0
            })
        
        result.sort(key=lambda x: abs(x['total_amount']), reverse=True)
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/user-categories")
async def get_user_categories(current_user: dict = Depends(get_current_user)):
    """Get list of user-defined categories"""
    try:
        rules_data = load_rules()
        categories = set()
        
        for rule in rules_data.get('rules', []):
            categories.add(rule['category'])
        
        for rule in rules_data.get('learned_rules', []):
            categories.add(rule['category'])
        
        transactions = db.get_user_transactions(current_user['id'])
        for tx in transactions:
            cat = tx.get('category')
            if cat and cat not in ['UNCATEGORIZED', 'NON_CATEGORIZZATO']:
                categories.add(cat)
        
        return sorted(list(categories))
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/user-categories")
async def create_user_category(
    category_name: str = Query(...),
    current_user: dict = Depends(get_current_user)
):
    """Create a new user category"""
    try:
        category_name = category_name.upper().strip()
        
        if not category_name:
            raise HTTPException(status_code=400, detail="Category name cannot be empty")
        
        return {"message": "Category created", "category": category_name}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/rules")
async def get_rules(current_user: dict = Depends(get_current_user)):
    """Get all categorization rules"""
    try:
        rules_data = load_rules()
        all_rules = []
        
        for rule in rules_data.get('rules', []):
            all_rules.append({**rule, 'source': 'manual', 'learned': False})
        
        for rule in rules_data.get('learned_rules', []):
            all_rules.append({**rule, 'source': 'learned', 'learned': True})
        
        return all_rules
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/rules")
async def create_rule(
    rule: CategoryRule,
    current_user: dict = Depends(get_current_user)
):
    """Create new categorization rule"""
    try:
        rules_data = load_rules()
        
        new_rule = {
            "keywords": rule.keywords,
            "category": rule.category,
            "subcategory": rule.subcategory,
            "type": rule.type,
            "priority": rule.priority or 1
        }
        
        if 'rules' not in rules_data:
            rules_data['rules'] = []
        
        rules_data['rules'].append(new_rule)
        save_rules(rules_data)
        
        return {"message": "Rule created successfully", "rule": new_rule}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/rules/{rule_index}")
async def delete_rule(
    rule_index: int,
    learned: bool = Query(False),
    current_user: dict = Depends(get_current_user)
):
    """Delete a categorization rule"""
    try:
        rules_data = load_rules()
        rule_key = 'learned_rules' if learned else 'rules'
        
        if rule_key in rules_data and 0 <= rule_index < len(rules_data[rule_key]):
            deleted_rule = rules_data[rule_key].pop(rule_index)
            save_rules(rules_data)
            return {"message": "Rule deleted successfully", "deleted_rule": deleted_rule}
        else:
            raise HTTPException(status_code=404, detail="Rule not found")
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/rules/{rule_index}")
async def update_rule(
    rule_index: int,
    rule: CategoryRule,
    learned: bool = Query(False),
    current_user: dict = Depends(get_current_user)
):
    """Update a categorization rule"""
    try:
        rules_data = load_rules()
        rule_key = 'learned_rules' if learned else 'rules'
        
        if rule_key not in rules_data:
            rules_data[rule_key] = []
        
        if 0 <= rule_index < len(rules_data[rule_key]):
            updated_rule = {
                "keywords": rule.keywords,
                "category": rule.category,
                "subcategory": rule.subcategory,
                "type": rule.type,
                "priority": rule.priority or 1
            }
            
            if learned and 'learned' in rules_data[rule_key][rule_index]:
                updated_rule['learned'] = True
                updated_rule['learn_count'] = rules_data[rule_key][rule_index].get('learn_count', 1)
                updated_rule['created_at'] = rules_data[rule_key][rule_index].get('created_at')
                updated_rule['updated_at'] = datetime.now().isoformat()
            
            rules_data[rule_key][rule_index] = updated_rule
            save_rules(rules_data)
            
            return {"message": "Rule updated successfully", "rule": updated_rule}
        else:
            raise HTTPException(status_code=404, detail="Rule not found")
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/smart-categorize")
async def smart_categorize_transaction(
    description: str = Query(...),
    current_user: dict = Depends(get_current_user)
):
    """Use AI to suggest category"""
    try:
        user_categories = await get_user_categories(current_user)
        result = smart_categorize_with_ai(description, user_categories)
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/bulk-categorize")
async def bulk_categorize_uncategorized(current_user: dict = Depends(get_current_user)):
    """Automatically categorize all uncategorized transactions"""
    try:
        user_categories = await get_user_categories(current_user)
        transactions = db.get_user_transactions(current_user['id'])
        
        uncategorized = [
            tx for tx in transactions 
            if not tx.get('category') or tx.get('category') in ['UNCATEGORIZED', 'NON_CATEGORIZZATO']
        ]
        
        categorized_count = 0
        
        for tx in uncategorized:
            result = smart_categorize_with_ai(tx['description_normalized'], user_categories)
            
            if result['confidence'] > 0.5 and result['category'] not in ['UNCATEGORIZED', 'NON_CATEGORIZZATO']:
                db.update_category(
                    transaction_id=tx['id'],
                    category=result['category']
                )
                
                learn_from_categorization(
                    tx['description_normalized'],
                    result['category']
                )
                
                categorized_count += 1
        
        transactions_after = db.get_user_transactions(current_user['id'])
        uncategorized_after = [
            tx for tx in transactions_after 
            if not tx.get('category') or tx.get('category') in ['UNCATEGORIZED', 'NON_CATEGORIZZATO']
        ]
        
        return {
            "message": f"Categorized {categorized_count} transactions",
            "total_uncategorized": len(uncategorized),
            "categorized": categorized_count,
            "remaining": len(uncategorized_after)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# IMPORT & UPLOAD ENDPOINTS (PROTECTED)
# ============================================================================

@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...),
    allow_duplicates: bool = Query(False),
    current_user: dict = Depends(get_current_user)
):
    """Upload and process bank statement file for current user"""
    try:
        batch_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
        file_path = Path("uploads") / file.filename
        
        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        raw_transactions = parse_file(file_path)
        normalized_transactions = normalize_transactions(raw_transactions)
        
        # Add user_id and batch_id
        for tx in normalized_transactions:
            tx['user_id'] = current_user['id']  # IMPORTANT
            tx['import_batch_id'] = batch_id
        
        if allow_duplicates:
            unique_transactions = normalized_transactions
            duplicates = 0
        else:
            unique_transactions, duplicates = deduplicate_transactions(normalized_transactions, db)
        
        categorized_transactions = categorize_transactions(
            unique_transactions,
            rules_path="config/rules.json",
            db=db
        )
        
        saved_count = db.insert_transactions(categorized_transactions)
        file_path.unlink()
        
        return {
            "message": "File processed successfully",
            "batch_id": batch_id,
            "filename": file.filename,
            "total_parsed": len(raw_transactions),
            "normalized": len(normalized_transactions),
            "new_transactions": saved_count,
            "duplicates": duplicates,
            "allow_duplicates": allow_duplicates
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/imports")
async def get_imports(current_user: dict = Depends(get_current_user)):
    """Get list of import batches for current user"""
    try:
        batches = get_import_batches()
        return batches
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# STATISTICS ENDPOINTS (PROTECTED)
# ============================================================================

@app.get("/api/stats/summary")
async def get_summary(
    current_user: dict = Depends(get_current_user),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None)
):
    """Get summary statistics for current user"""
    try:
        transactions = db.get_user_transactions(current_user['id'])
        
        if start_date:
            transactions = [t for t in transactions if t['date'] >= start_date]
        if end_date:
            transactions = [t for t in transactions if t['date'] <= end_date]
        
        if not transactions:
            return {
                "total_income": 0,
                "total_expenses": 0,
                "net_savings": 0,
                "transaction_count": 0,
                "daily_average": 0,
                "weekly_average": 0,
                "date_range": None
            }
        
        total_income = sum(t['amount'] for t in transactions if t['amount'] > 0)
        total_expenses = sum(t['amount'] for t in transactions if t['amount'] < 0)
        net_savings = total_income + total_expenses
        
        dates = [t['date'] for t in transactions]
        min_date = min(dates)
        max_date = max(dates)
        
        date_diff = (datetime.fromisoformat(max_date) - datetime.fromisoformat(min_date)).days + 1
        daily_avg = abs(total_expenses) / date_diff if date_diff > 0 else 0
        weekly_avg = daily_avg * 7
        
        return {
            "total_income": total_income,
            "total_expenses": abs(total_expenses),
            "net_savings": net_savings,
            "transaction_count": len(transactions),
            "daily_average": daily_avg,
            "weekly_average": weekly_avg,
            "date_range": {
                "start": min_date,
                "end": max_date,
                "days": date_diff
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats/monthly")
async def get_monthly_stats(current_user: dict = Depends(get_current_user)):
    """Get monthly breakdown for current user"""
    try:
        import pandas as pd
        
        transactions = db.get_user_transactions(current_user['id'])
        
        if not transactions:
            return []
        
        df = pd.DataFrame(transactions)
        df['date'] = pd.to_datetime(df['date'])
        df['month'] = df['date'].dt.to_period('M').astype(str)
        
        monthly_stats = []
        
        for month in sorted(df['month'].unique()):
            month_data = df[df['month'] == month]
            
            income = month_data[month_data['amount'] > 0]['amount'].sum()
            expenses = abs(month_data[month_data['amount'] < 0]['amount'].sum())
            net = income - expenses
            
            categories = {}
            for _, tx in month_data.iterrows():
                cat = tx.get('category', 'UNCATEGORIZED')
                if cat not in categories:
                    categories[cat] = 0
                categories[cat] += tx['amount']
            
            monthly_stats.append({
                'month': month,
                'income': income,
                'expenses': expenses,
                'net': net,
                'transaction_count': len(month_data),
                'categories': categories
            })
        
        return monthly_stats
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats/categories")
async def get_category_stats(
    current_user: dict = Depends(get_current_user),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None)
):
    """Get category statistics for current user"""
    try:
        transactions = db.get_user_transactions(current_user['id'])
        
        if start_date:
            transactions = [t for t in transactions if t['date'] >= start_date]
        if end_date:
            transactions = [t for t in transactions if t['date'] <= end_date]
        
        category_stats = {}
        
        for t in transactions:
            cat = t.get('category', 'UNCATEGORIZED')
            
            if cat not in category_stats:
                category_stats[cat] = {
                    'category': cat,
                    'total': 0,
                    'count': 0,
                    'transactions': [],
                    'type': t.get('type', 'expense')
                }
            
            category_stats[cat]['total'] += t['amount']
            category_stats[cat]['count'] += 1
            category_stats[cat]['transactions'].append({
                'date': t['date'],
                'amount': t['amount'],
                'description': t['description_raw'][:50]
            })
        
        result = list(category_stats.values())
        result.sort(key=lambda x: abs(x['total']), reverse=True)
        
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# REPORTS ENDPOINTS (PROTECTED)
# ============================================================================

@app.post("/api/reports/generate")
async def generate_excel_report(
    current_user: dict = Depends(get_current_user),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    categories: Optional[str] = Query(None)
):
    """Generate Excel report for current user"""
    try:
        transactions = db.get_user_transactions(current_user['id'])
        
        if start_date:
            transactions = [t for t in transactions if t['date'] >= start_date]
        if end_date:
            transactions = [t for t in transactions if t['date'] <= end_date]
        if categories:
            cat_list = categories.split(',')
            transactions = [t for t in transactions if t.get('category') in cat_list]
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = f"reports/report_{timestamp}.xlsx"
        
        generate_report(transactions, report_path)
        
        return FileResponse(
            report_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"financial_report_{timestamp}.xlsx"
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# RECURRING PAYMENTS ENDPOINTS (PROTECTED)
# ============================================================================

@app.get("/api/recurring-payments")
async def get_recurring_payments(current_user: dict = Depends(get_current_user)):
    """Get recurring payments for current user"""
    try:
        payments = db.get_user_recurring_payments(current_user['id'])
        return payments
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/recurring-payments")
async def create_recurring_payment(
    payment: RecurringPaymentCreate,
    current_user: dict = Depends(get_current_user)
):
    """Create recurring payment for current user"""
    try:
        payment_id = db.create_recurring_payment(
            user_id=current_user['id'],  # IMPORTANT
            name=payment.name,
            amount=payment.amount,
            frequency=payment.frequency,
            type=payment.type,
            start_date=payment.start_date,
            end_date=payment.end_date,
            description=payment.description,
            category=payment.category
        )
        
        return {"message": "Recurring payment created", "id": payment_id}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/recurring-payments/{payment_id}")
async def get_recurring_payment(
    payment_id: int,
    current_user: dict = Depends(get_current_user)
):
    """Get specific recurring payment (only if belongs to user)"""
    try:
        payment = db.get_recurring_payment(payment_id)
        
        if not payment or payment.get('user_id') != current_user['id']:
            raise HTTPException(status_code=404, detail="Payment not found")
        
        return payment
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/recurring-payments/{payment_id}")
async def update_recurring_payment(
    payment_id: int,
    payment: RecurringPaymentCreate,
    current_user: dict = Depends(get_current_user)
):
    """Update recurring payment (only if belongs to user)"""
    try:
        existing = db.get_recurring_payment(payment_id)
        
        if not existing or existing.get('user_id') != current_user['id']:
            raise HTTPException(status_code=404, detail="Payment not found")
        
        db.update_recurring_payment(
            payment_id=payment_id,
            name=payment.name,
            amount=payment.amount,
            frequency=payment.frequency,
            type=payment.type,
            start_date=payment.start_date,
            end_date=payment.end_date,
            description=payment.description,
            category=payment.category
        )
        
        return {"message": "Recurring payment updated"}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/recurring-payments/{payment_id}")
async def delete_recurring_payment(
    payment_id: int,
    current_user: dict = Depends(get_current_user)
):
    """Delete recurring payment (only if belongs to user)"""
    try:
        existing = db.get_recurring_payment(payment_id)
        
        if not existing or existing.get('user_id') != current_user['id']:
            raise HTTPException(status_code=404, detail="Payment not found")
        
        db.delete_recurring_payment(payment_id)
        return {"message": "Recurring payment deleted"}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/recurring-payments/stats")
async def get_recurring_payments_stats(current_user: dict = Depends(get_current_user)):
    """Get recurring payments statistics for current user"""
    try:
        payments = db.get_user_recurring_payments(current_user['id'])
        
        monthly_total = 0
        active_subscriptions = 0
        active_financing = 0
        
        for payment in payments:
            if payment.get('is_active', True):
                amount = payment['amount']
                if payment['frequency'] == 'yearly':
                    amount = amount / 12
                elif payment['frequency'] == 'weekly':
                    amount = amount * 4.33
                
                monthly_total += amount
                
                if payment['type'] == 'subscription':
                    active_subscriptions += 1
                elif payment['type'] == 'financing':
                    active_financing += 1
        
        return {
            "monthly_total": monthly_total,
            "active_subscriptions": active_subscriptions,
            "active_financing": active_financing
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("🚀 Finance Tracker API v3.0 - Multi-User Edition")
    print("=" * 70)
    print("📍 API URL: http://localhost:8000")
    print("📚 API Docs: http://localhost:8000/docs")
    print("=" * 70)
    print("✨ Features:")
    print("  • Multi-user authentication with JWT")
    print("  • User-isolated data")
    print("  • Smart category learning")
    print("  • Advanced analytics")
    print("  • Recurring payments tracking")
    print("=" * 70)
    
    uvicorn.run("backend_main_with_auth:app", host="0.0.0.0", port=8000, reload=True)
