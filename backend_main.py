"""
Finance Tracker - FastAPI Backend v2.0
Enhanced with multi-import, category management, and advanced analytics
"""

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from pydantic import BaseModel
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

app = FastAPI(
    title="Personal Finance Tracker API v2.0",
    description="Enhanced API for managing personal finances with multi-import and learning",
    version="2.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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
# PYDANTIC MODELS
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
    learn: bool = True  # If true, create rule from this

class Budget(BaseModel):
    category: str
    monthly_limit: float
    alert_threshold: float = 0.8  # Alert at 80%

class ImportBatch(BaseModel):
    batch_id: str
    filename: str
    upload_date: str
    transaction_count: int
    date_range: Dict[str, str]

# ============================================================================
# HELPER FUNCTIONS
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
    """
    AI-powered categorization using rules-based matching with priority hierarchy.
    
    Priority Order:
    1. Manual rules from rules.json (highest priority - Confidence 95%)
    2. Learned rules from rules.json (Confidence 90%)
    3. Generic keyword patterns (fallback - Confidence 75%)
    4. Fuzzy matching (variable confidence)
    
    Args:
        description: Transaction description
        user_categories: List of user-defined category names
        
    Returns:
        Dict with suggested category and confidence score
    """
    description_clean = description.upper().strip()
    
    # Load rules from rules.json
    rules_data = load_rules()
    
    # ========================================================================
    # PRIORITY 1: Check MANUAL RULES (Confidence: 95%)
    # ========================================================================
    for rule in rules_data.get('rules', []):
        for keyword in rule.get('keywords', []):
            if keyword.upper() in description_clean:
                return {
                    'category': rule['category'],
                    'confidence': 0.95,
                    'method': 'manual_rule',
                    'matched_keyword': keyword
                }
    
    # ========================================================================
    # PRIORITY 2: Check LEARNED RULES (Confidence: 90%)
    # ========================================================================
    for rule in rules_data.get('learned_rules', []):
        for keyword in rule.get('keywords', []):
            if keyword.upper() in description_clean:
                return {
                    'category': rule['category'],
                    'confidence': 0.90,
                    'method': 'learned_rule',
                    'matched_keyword': keyword
                }
    
    # ========================================================================
    # PRIORITY 3: Generic Keyword Patterns (Confidence: 75%)
    # ========================================================================
    category_patterns = {
        'ALIMENTARI': ['MARKET', 'SUPER', 'CONAD', 'COOP', 'ESSELUNGA', 'CARREFOUR', 'LIDL', 'EUROSPIN', 'FOOD', 'GROCERY'],
        'RISTORANTI': ['RISTORANTE', 'PIZZERIA', 'BAR', 'CAFE', 'TRATTORIA', 'OSTERIA', 'PUB', 'MCDONALD', 'BURGER', 'KFC'],
        'TRASPORTI': ['BENZINA', 'DIESEL', 'ENI', 'SHELL', 'Q8', 'TAMOIL', 'TRENITALIA', 'ITALO', 'ATM', 'METRO', 'BUS', 'ATAC'],
        'SHOPPING': ['ZARA', 'H&M', 'NIKE', 'ADIDAS', 'AMAZON', 'EBAY', 'TIGER', 'IKEA', 'DECATHLON'],
        'UTENZE': ['ENEL', 'GAS', 'ACQUA', 'LUCE', 'BOLLETTA', 'VODAFONE', 'TIM', 'WIND', 'ILIAD'],
        'INTRATTENIMENTO': ['CINEMA', 'NETFLIX', 'SPOTIFY', 'AMAZON PRIME', 'DISNEY', 'TEATRO', 'CONCERT'],
        'SALUTE': ['FARMACIA', 'MEDICO', 'OSPEDALE', 'DENTISTA', 'ANALISI', 'VISITA'],
        'CASA': ['AFFITTO', 'MUTUO', 'CONDOMINIO', 'IDRAULICO', 'ELETTRICISTA'],
        'STIPENDIO': ['STIPENDIO', 'SALARY', 'PAYROLL', 'WAGE', 'BUSTA PAGA'],
        'BONIFICO': ['BONIFICO', 'TRANSFER', 'WIRE', 'GIROCONTO'],
    }
    
    for pattern_cat, keywords in category_patterns.items():
        for keyword in keywords:
            if keyword in description_clean:
                # Check if this category exists in user categories
                matching_user_cat = next((uc for uc in user_categories if uc.upper() == pattern_cat), None)
                if matching_user_cat:
                    return {'category': matching_user_cat, 'confidence': 0.75, 'method': 'generic_pattern'}
    
    # ========================================================================
    # PRIORITY 4: Fuzzy Matching (Confidence: variable)
    # ========================================================================
    best_match = None
    best_score = 0
    
    for user_cat in user_categories:
        user_cat_upper = user_cat.upper()
        
        # Direct match
        if user_cat_upper in description_clean:
            return {'category': user_cat, 'confidence': 0.85, 'method': 'direct_match'}
        
        # Fuzzy matching - check word overlap
        user_words = set(user_cat_upper.split())
        desc_words = set(description_clean.split())
        overlap = len(user_words & desc_words)
        
        if overlap > 0:
            score = overlap / max(len(user_words), 1)
            if score > best_score:
                best_score = score
                best_match = user_cat
    
    if best_match and best_score > 0.5:
        return {'category': best_match, 'confidence': best_score, 'method': 'fuzzy_match'}
    
    # ========================================================================
    # NO MATCH FOUND
    # ========================================================================
    return {'category': 'UNCATEGORIZED', 'confidence': 0.0, 'method': 'no_match'}

def learn_from_categorization(description: str, category: str, subcategory: Optional[str] = None):
    """
    Learn from manual categorization and create/update rule.
    Enhanced with smarter keyword extraction.
    """
    rules_data = load_rules()
    
    # Extract keywords from description (improved approach)
    words = description.upper().split()
    
    # Common words to ignore (expanded)
    common_words = {
        'DEL', 'ITA', 'SRL', 'SPA', 'OPERAZIONE', 'CARTA', 'THE', 'AND', 'DI', 'DA',
        'CON', 'PER', 'SU', 'IN', 'A', 'IL', 'LA', 'LE', 'I', 'GLI', 'UN', 'UNA',
        'PRESSO', 'VIA', 'ROMA', 'MILANO', 'ITALIA', 'ITALIAN'
    }
    
    # Extract meaningful keywords
    keywords = []
    for word in words:
        # Remove numbers and special chars
        clean_word = re.sub(r'[^A-Z]', '', word)
        if len(clean_word) > 3 and clean_word not in common_words:
            keywords.append(clean_word.lower())
    
    # Take top 5 most distinctive keywords
    keywords = list(set(keywords))[:5]
    
    if not keywords:
        # Fallback: use first meaningful word
        for word in words:
            if len(word) > 3:
                keywords = [word.lower()]
                break
    
    if not keywords:
        return
    
    # Check if rule already exists
    if 'learned_rules' not in rules_data:
        rules_data['learned_rules'] = []
    
    existing_rule = None
    for rule in rules_data['learned_rules']:
        if rule['category'] == category:
            # Update existing rule
            for kw in keywords:
                if kw.lower() not in [k.lower() for k in rule['keywords']]:
                    rule['keywords'].append(kw.lower())
            existing_rule = rule
            break
    
    if not existing_rule:
        # Create new learned rule
        new_rule = {
            "keywords": [kw.lower() for kw in keywords],
            "category": category,
            "subcategory": subcategory,
            "type": "expense" if category not in ["INCOME", "SALARY", "STIPENDIO", "BONIFICO"] else "income",
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
    """Get list of all import batches from database"""
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
                'date_range': {
                    'start': row[2],
                    'end': row[3]
                },
                'upload_date': row[4]
            })
        
        return batches
    except:
        return []

# ============================================================================
# ROOT & INFO ENDPOINTS
# ============================================================================

@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "message": "Personal Finance Tracker API v2.0",
        "version": "2.0.0",
        "features": [
            "Multi-file import with batch tracking",
            "Smart category learning",
            "Advanced filtering and analytics",
            "Budget tracking",
            "Custom period reports"
        ],
        "endpoints": {
            "transactions": "/api/transactions",
            "categories": "/api/categories",
            "rules": "/api/rules",
            "budgets": "/api/budgets",
            "stats": "/api/stats",
            "imports": "/api/imports",
            "reports": "/api/reports"
        }
    }

# ============================================================================
# TRANSACTIONS ENDPOINTS
# ============================================================================

@app.get("/api/transactions")
async def get_transactions(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    min_amount: Optional[float] = Query(None),
    max_amount: Optional[float] = Query(None),
    review_only: bool = Query(False),
    batch_id: Optional[str] = Query(None),
    limit: int = Query(1000)
):
    """Get transactions with advanced filters"""
    try:
        transactions = db.get_all_transactions()
        
        # Apply filters
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
        
        # Sort by date descending
        transactions.sort(key=lambda x: x['date'], reverse=True)
        
        return transactions[:limit]
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/transactions/{transaction_id}/category")
async def update_transaction_category(transaction_id: int, update: CategoryUpdate):
    """Update transaction category and optionally learn from it"""
    try:
        # Get transaction details for learning
        transactions = db.get_all_transactions()
        transaction = next((t for t in transactions if t['id'] == transaction_id), None)
        
        if not transaction:
            raise HTTPException(status_code=404, detail="Transaction not found")
        
        # Update category
        db.update_category(
            transaction_id=transaction_id,
            category=update.category,
            subcategory=update.subcategory
        )
        
        # Learn from this categorization
        if update.learn:
            learn_from_categorization(
                transaction['description_normalized'],
                update.category,
                update.subcategory
            )
        
        return {
            "message": "Category updated successfully",
            "learned": update.learn
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# NEW TRANSACTION CREATION ENDPOINT
# ============================================================================

class TransactionCreate(BaseModel):
    date: str
    description: str
    amount: float
    type: str  # "income" or "expense"

@app.post("/api/transactions")
async def create_transaction(transaction: TransactionCreate):
    """Create a new transaction manually"""
    try:
        # Validate input
        if not transaction.description.strip():
            raise HTTPException(status_code=400, detail="Description cannot be empty")
        
        if transaction.type not in ["income", "expense"]:
            raise HTTPException(status_code=400, detail="Type must be 'income' or 'expense'")
        
        # Adjust amount based on type (expenses are negative)
        final_amount = abs(transaction.amount) if transaction.type == "income" else -abs(transaction.amount)
        
        # Get user categories for AI categorization
        user_categories_response = await get_user_categories()
        user_categories = user_categories_response if isinstance(user_categories_response, list) else []
        
        # Use AI to suggest category
        ai_result = smart_categorize_with_ai(transaction.description, user_categories)
        suggested_category = ai_result.get('category', 'UNCATEGORIZED')
        
        # Generate fingerprint hash for deduplication
        fingerprint_string = f"{transaction.date}_{transaction.description.upper().strip()}_{final_amount}"
        fingerprint_hash = hashlib.md5(fingerprint_string.encode()).hexdigest()
        
        # Create transaction object
        new_transaction = {
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
        
        # Insert into database
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

# CATEGORIES & RULES ENDPOINTS
# ============================================================================

@app.get("/api/categories")
async def get_categories():
    """Get all categories with statistics"""
    try:
        transactions = db.get_all_transactions()
        
        categories = {}
        for t in transactions:
            cat = t.get('category') or 'UNCATEGORIZED'
            
            if cat not in categories:
                categories[cat] = {
                    'name': cat,
                    'type': t.get('type', 'expense'),
                    'subcategories': set(),
                    'count': 0,
                    'total': 0,
                    'avg': 0
                }
            
            categories[cat]['count'] += 1
            categories[cat]['total'] += t['amount']
            
            subcat = t.get('subcategory')
            if subcat:
                categories[cat]['subcategories'].add(subcat)
        
        # Calculate averages and convert sets to lists
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
        
        # Sort by total amount (absolute value)
        result.sort(key=lambda x: abs(x['total_amount']), reverse=True)
        
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/rules")
async def get_rules():
    """Get all categorization rules"""
    try:
        rules_data = load_rules()
        
        all_rules = []
        
        # Manual rules
        for rule in rules_data.get('rules', []):
            all_rules.append({
                **rule,
                'source': 'manual',
                'learned': False
            })
        
        # Learned rules
        for rule in rules_data.get('learned_rules', []):
            all_rules.append({
                **rule,
                'source': 'learned',
                'learned': True
            })
        
        return all_rules
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/rules")
async def create_rule(rule: CategoryRule):
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
async def delete_rule(rule_index: int, learned: bool = Query(False)):
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

# ============================================================================

@app.put("/api/rules/{rule_index}")
async def update_rule(rule_index: int, rule: CategoryRule, learned: bool = Query(False)):
    """Update a categorization rule"""
    try:
        rules_data = load_rules()
        
        rule_key = 'learned_rules' if learned else 'rules'
        
        if rule_key not in rules_data:
            rules_data[rule_key] = []
        
        if 0 <= rule_index < len(rules_data[rule_key]):
            # Update the rule
            updated_rule = {
                "keywords": rule.keywords,
                "category": rule.category,
                "subcategory": rule.subcategory,
                "type": rule.type,
                "priority": rule.priority or 1
            }
            
            # Preserve learned metadata if it's a learned rule
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

# ============================================================================
# USER CATEGORIES MANAGEMENT
# ============================================================================

# ============================================================================

@app.get("/api/user-categories")
async def get_user_categories():
    """Get list of user-defined categories"""
    try:
        rules_data = load_rules()
        
        # Extract unique categories from both manual and learned rules
        categories = set()
        
        for rule in rules_data.get('rules', []):
            categories.add(rule['category'])
        
        for rule in rules_data.get('learned_rules', []):
            categories.add(rule['category'])
        
        # Also get categories from transactions
        transactions = db.get_all_transactions()
        for tx in transactions:
            cat = tx.get('category')
            if cat and cat != 'UNCATEGORIZED' and cat != 'NON_CATEGORIZZATO':
                categories.add(cat)
        
        return sorted(list(categories))
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/user-categories")
async def create_user_category(category_name: str = Query(...)):
    """Create a new user category"""
    try:
        category_name = category_name.upper().strip()
        
        if not category_name:
            raise HTTPException(status_code=400, detail="Category name cannot be empty")
        
        # Just return success - category will be created when first used
        return {"message": "Category created", "category": category_name}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/smart-categorize")
async def smart_categorize_transaction(description: str = Query(...)):
    """
    Use AI to suggest category for a transaction description.
    Returns suggested category with confidence score.
    """
    try:
        # Get user categories
        user_categories_response = await get_user_categories()
        user_categories = user_categories_response if isinstance(user_categories_response, list) else []
        
        # Use AI categorization
        result = smart_categorize_with_ai(description, user_categories)
        
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/bulk-categorize")
async def bulk_categorize_uncategorized():
    """
    Automatically categorize all uncategorized transactions using AI.
    Returns count of categorized transactions.
    """
    try:
        # Get user categories
        user_categories_response = await get_user_categories()
        user_categories = user_categories_response if isinstance(user_categories_response, list) else []
        
        # Get uncategorized transactions
        transactions = db.get_all_transactions()
        uncategorized = [
            tx for tx in transactions 
            if not tx.get('category') or tx.get('category') in ['UNCATEGORIZED', 'NON_CATEGORIZZATO']
        ]
        
        categorized_count = 0
        
        for tx in uncategorized:
            result = smart_categorize_with_ai(tx['description_normalized'], user_categories)
            
            # CRITICAL FIX: Only categorize if confidence is high AND category is NOT UNCATEGORIZED
            if result['confidence'] > 0.5 and not result.get('suggestion') and result['category'] not in ['UNCATEGORIZED', 'NON_CATEGORIZZATO']:
                # Auto-categorize if confidence is high enough
                db.update_category(
                    transaction_id=tx['id'],
                    category=result['category']
                )
                
                # Learn from this categorization
                learn_from_categorization(
                    tx['description_normalized'],
                    result['category']
                )
                
                categorized_count += 1

        
        # CRITICAL FIX: Recalculate uncategorized count AFTER categorization
        transactions_after = db.get_all_transactions()
        uncategorized_after = [
            tx for tx in transactions_after 
            if not tx.get('category') or tx.get('category') in ['UNCATEGORIZED', 'NON_CATEGORIZZATO']
        ]
        
        return {
            "message": f"Categorized {categorized_count} transactions",
            "total_uncategorized": len(uncategorized),
            "categorized": categorized_count,
            "remaining": len(uncategorized_after)  # ← FIX: Use recalculated count
        }

    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# IMPORT & BATCH ENDPOINTS
# ============================================================================

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...), allow_duplicates: bool = Query(False)):
    """
    Upload and process bank statement file
    Supports multiple imports of same period for tracking changes
    """
    try:
        # Generate batch ID
        batch_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
        
        # Save uploaded file temporarily
        file_path = Path("uploads") / file.filename
        
        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        # Process file
        raw_transactions = parse_file(file_path)
        normalized_transactions = normalize_transactions(raw_transactions)
        
        # Add batch_id to transactions
        for tx in normalized_transactions:
            tx['import_batch_id'] = batch_id
        
        # Deduplicate (unless allow_duplicates is True)
        if allow_duplicates:
            unique_transactions = normalized_transactions
            duplicates = 0
        else:
            unique_transactions, duplicates = deduplicate_transactions(normalized_transactions, db)
        
        # Categorize using both manual and learned rules
        categorized_transactions = categorize_transactions(
            unique_transactions,
            rules_path="config/rules.json",
            db=db
        )
        
        # Save to database
        saved_count = db.insert_transactions(categorized_transactions)
        
        # Clean up
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
async def get_imports():
    """Get list of all import batches"""
    try:
        batches = get_import_batches()
        return batches
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# STATISTICS & ANALYTICS ENDPOINTS
# ============================================================================

@app.get("/api/stats/summary")
async def get_summary(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None)
):
    """Get comprehensive summary statistics"""
    try:
        transactions = db.get_all_transactions()
        
        # Apply date filters
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
                "category_breakdown": {},
                "daily_average": 0,
                "weekly_average": 0,
                "date_range": None
            }
        
        # Calculate stats
        total_income = sum(t['amount'] for t in transactions if t['amount'] > 0)
        total_expenses = sum(t['amount'] for t in transactions if t['amount'] < 0)
        net_savings = total_income + total_expenses
        
        # Category breakdown
        category_breakdown = {}
        for t in transactions:
            cat = t.get('category', 'UNCATEGORIZED')
            if cat not in category_breakdown:
                category_breakdown[cat] = {
                    'total': 0,
                    'count': 0,
                    'percentage': 0
                }
            category_breakdown[cat]['total'] += t['amount']
            category_breakdown[cat]['count'] += 1
        
        # Calculate percentages (of expenses only)
        total_expense_abs = abs(total_expenses)
        for cat, data in category_breakdown.items():
            if data['total'] < 0:  # Only for expenses
                data['percentage'] = (abs(data['total']) / total_expense_abs * 100) if total_expense_abs > 0 else 0
        
        # Date range and averages
        dates = [t['date'] for t in transactions]
        min_date = min(dates)
        max_date = max(dates)
        
        from datetime import datetime
        date_diff = (datetime.fromisoformat(max_date) - datetime.fromisoformat(min_date)).days + 1
        
        daily_avg = abs(total_expenses) / date_diff if date_diff > 0 else 0
        weekly_avg = daily_avg * 7
        
        return {
            "total_income": total_income,
            "total_expenses": abs(total_expenses),
            "net_savings": net_savings,
            "transaction_count": len(transactions),
            "category_breakdown": category_breakdown,
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
async def get_monthly_stats():
    """Get monthly breakdown with trends"""
    try:
        import pandas as pd
        
        transactions = db.get_all_transactions()
        
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
            
            # Category breakdown for this month
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
        
        # Calculate trends
        for i in range(1, len(monthly_stats)):
            prev = monthly_stats[i-1]
            curr = monthly_stats[i]
            
            curr['trends'] = {
                'income_change': ((curr['income'] - prev['income']) / prev['income'] * 100) if prev['income'] > 0 else 0,
                'expenses_change': ((curr['expenses'] - prev['expenses']) / prev['expenses'] * 100) if prev['expenses'] > 0 else 0
            }
        
        return monthly_stats
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats/categories")
async def get_category_stats(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None)
):
    """Get detailed statistics by category for charts"""
    try:
        transactions = db.get_all_transactions()
        
        # Apply filters
        if start_date:
            transactions = [t for t in transactions if t['date'] >= start_date]
        if end_date:
            transactions = [t for t in transactions if t['date'] <= end_date]
        
        # Group by category
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
        
        # Convert to list and sort
        result = list(category_stats.values())
        result.sort(key=lambda x: abs(x['total']), reverse=True)
        
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# REPORTS ENDPOINTS
# ============================================================================

@app.post("/api/reports/generate")
async def generate_excel_report(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    categories: Optional[str] = Query(None)
):
    """Generate Excel report for specified period"""
    try:
        transactions = db.get_all_transactions()
        
        # Apply filters
        if start_date:
            transactions = [t for t in transactions if t['date'] >= start_date]
        if end_date:
            transactions = [t for t in transactions if t['date'] <= end_date]
        if categories:
            cat_list = categories.split(',')
            transactions = [t for t in transactions if t.get('category') in cat_list]
        
        # Generate report
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
# RECURRING PAYMENTS ENDPOINTS (NEW)
# ============================================================================

class RecurringPaymentCreate(BaseModel):
    name: str
    amount: float
    frequency: str  # monthly, yearly, weekly
    type: str  # subscription, financing
    start_date: str
    end_date: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None

@app.get("/api/recurring-payments/stats")
async def get_recurring_payments_stats():
    """Get statistics for recurring payments dashboard"""
    try:
        payments = db.get_recurring_payments()
        
        monthly_total = 0
        active_subscriptions = 0
        active_financing = 0
        
        for payment in payments:
            if payment['is_active']:
                # Calculate monthly equivalent
                amount = payment['amount']
                if payment['frequency'] == 'yearly':
                    amount = amount / 12
                elif payment['frequency'] == 'weekly':
                    amount = amount * 4.33  # Average weeks per month
                
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

@app.get("/api/recurring-payments")
async def get_recurring_payments():
    """Get all recurring payments"""
    try:
        payments = db.get_recurring_payments()
        return payments
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/recurring-payments")
async def create_recurring_payment(payment: RecurringPaymentCreate):
    """Create a new recurring payment"""
    try:
        payment_id = db.create_recurring_payment(
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
async def get_recurring_payment(payment_id: int):
    """Get a specific recurring payment"""
    try:
        payment = db.get_recurring_payment(payment_id)
        
        if not payment:
            raise HTTPException(status_code=404, detail="Payment not found")
        
        return payment
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/recurring-payments/{payment_id}")
async def update_recurring_payment(payment_id: int, payment: RecurringPaymentCreate):
    """Update a recurring payment"""
    try:
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
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/recurring-payments/{payment_id}")
async def delete_recurring_payment(payment_id: int):
    """Delete a recurring payment"""
    try:
        db.delete_recurring_payment(payment_id)
        return {"message": "Recurring payment deleted"}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# ============================================================================
# BULK DELETE TRANSACTIONS ENDPOINT (NEW)
# ============================================================================

@app.post("/api/transactions/delete-multiple")
async def delete_multiple_transactions(transaction_ids: List[int]):
    """Delete multiple transactions"""
    try:
        deleted_count = db.delete_transactions(transaction_ids)
        return {
            "message": f"{deleted_count} transazioni eliminate",
            "deleted_count": deleted_count
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("🚀 Finance Tracker API v2.0 - Enhanced Edition")
    print("=" * 70)
    print("📍 API URL: http://localhost:8000")
    print("📚 API Docs: http://localhost:8000/docs")
    print("=" * 70)
    print("✨ New Features:")
    print("  • Multi-file import with batch tracking")
    print("  • Smart category learning from your choices")
    print("  • Advanced analytics and trends")
    print("  • Custom period reports with charts")
    print("=" * 70)
    
    uvicorn.run("backend_main:app", host="0.0.0.0", port=8000, reload=False)


