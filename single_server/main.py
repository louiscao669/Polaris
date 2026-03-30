from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import uuid
from datetime import datetime

app = FastAPI(title="Single Server Market System")
security = HTTPBearer()

# --- 1. DATABASE SIMULATION (In-Memory) ---
users = {"user_1": {"balance_cents": 10000, "portfolio": {}}}
markets = {
    1: {"title": "Gold Price", "price_cents": 1850, "status": "OPEN", "expiry": "2026-12-31T23:59:59Z"}
}
idempotency_store: Dict[str, dict] = {}


class BuyOrder(BaseModel):
    request_id: str
    market_id: int
    quantity: int
    price_limit_cents: int

class MarketCreate(BaseModel):
    request_id: str
    title: str
    initial_liquidity: int
    expiry_iso: str

class MarketUpdate(BaseModel):
    request_id: str
    changes: Dict[str, Any]

# --- SERVER-SIDE API (Internal Logic Functions) ---

def Authenticate(username, password_hash) -> Optional[str]:
    if username == "admin" and password_hash == "secret":
        return "token_1"
    return None

# --- SERVER-SIDE API (Internal Logic) ---

def Handle_Buy(requestID, userID, marketID, quantity, priceLimitCents) -> dict:
    if requestID in idempotency_store:
        return idempotency_store[requestID]

    market = markets.get(marketID)
    if not market or market["status"] != "OPEN":
        return {"status": "FAIL", "reason": "Market Unavailable"}
    
    total_cost = quantity * priceLimitCents
    if users[userID]["balance_cents"] < total_cost:
        return {"status": "FAIL", "reason": "Insufficient Funds"}

    users[userID]["balance_cents"] -= total_cost
    users[userID]["portfolio"][marketID] = users[userID]["portfolio"].get(marketID, 0) + quantity
    
    receipt = {
        "status": "SUCCESS", 
        "order_id": str(uuid.uuid4()), 
        "rem_balance_cents": users[userID]["balance_cents"],
        "units_owned": users[userID]["portfolio"][marketID]
    }

    idempotency_store[requestID] = receipt
    return receipt

def Handle_Sell(requestID, userID, marketID, quantity, priceLimitCents) -> dict:
    if requestID in idempotency_store:
        return idempotency_store[requestID]

    # 2. Inventory Check (Preventing over-selling)
    current_owned = users[userID]["portfolio"].get(marketID, 0)
    if current_owned < quantity:
        return {
            "status": "FAIL", 
            "reason": "Insufficient Assets", 
            "owned": current_owned, 
            "attempted_sell": quantity
        }
    
    revenue = quantity * priceLimitCents
    users[userID]["balance_cents"] += revenue
    users[userID]["portfolio"][marketID] -= quantity
    
    receipt = {
        "status": "SUCCESS", 
        "payout_cents": revenue, 
        "rem_balance_cents": users[userID]["balance_cents"],
        "units_remaining": users[userID]["portfolio"][marketID]
    }

    # Save for Idempotency
    idempotency_store[requestID] = receipt
    return receipt

def Get_Market_Description(marketID) -> dict:
    return markets.get(marketID, {})

def Create_Market_Internal(creatorID, title, liquidity, expiryISO) -> int:
    new_id = max(markets.keys() or [0]) + 1
    markets[new_id] = {
        "title": title, 
        "price_cents": 1000, 
        "status": "OPEN", 
        "expiry": expiryISO,
        "creator": creatorID
    }
    return new_id

def Update_Market_Internal(marketID, updatedFieldsMap) -> bool:
    if marketID in markets:
        markets[marketID].update(updatedFieldsMap)
        return True
    return False

# --- 4. AUTHENTICATION DEPENDENCY ---
def get_current_user(auth: HTTPAuthorizationCredentials = Depends(security)):
    token = auth.credentials 
    if token == "token_1": return "user_1" 
    raise HTTPException(status_code=403, detail="Invalid token")

# --- 5. CUSTOMER-FACING API (Client Access Points) ---

@app.post("/login")
def login(username: str, password_hash: str):
    token = Authenticate(username, password_hash)
    if token:
        return {"session_token": token, "expires_at": "2026-03-30T17:00:00Z"}
    raise HTTPException(status_code=400, detail="Invalid credentials")

@app.post("/buy")
def buy(order: BuyOrder, user_id: str = Depends(get_current_user)):
    if order.request_id in idempotency_store: return idempotency_store[order.request_id]
    
    result = Handle_Buy(order.request_id, user_id, order.market_id, order.quantity, order.price_limit_cents)
    idempotency_store[order.request_id] = result
    return result

@app.post("/sell")
def sell(order: BuyOrder, user_id: str = Depends(get_current_user)):
    if order.request_id in idempotency_store: return idempotency_store[order.request_id]
    
    result = Handle_Sell(order.request_id, user_id, order.market_id, order.quantity, order.price_limit_cents)
    idempotency_store[order.request_id] = result
    return result

@app.get("/view/{market_id}")
def view(market_id: int):
    return Get_Market_Description(market_id)

@app.post("/market/create")
def create_market(m: MarketCreate, user_id: str = Depends(get_current_user)):
    m_id = Create_Market_Internal(user_id, m.title, m.initial_liquidity, m.expiry_iso)
    return {"status": "CREATED", "market_id": m_id}

@app.patch("/market/update/{market_id}")
def update_market(market_id: int, u: MarketUpdate, user_id: str = Depends(get_current_user)):
    success = Update_Market_Internal(market_id, u.changes)
    return {"success": success}