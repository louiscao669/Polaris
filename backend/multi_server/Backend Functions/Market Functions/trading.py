from typing import Dict, Any, List, Optional
import uuid

users = {"user_1": {"balance_cents": 10000, "portfolio": {}}}
markets = {
    1: {"title": "Gold Price", "price_cents": 1850, "status": "OPEN", "expiry": "2026-12-31T23:59:59Z"}
}
idempotency_store: Dict[str, dict] = {}

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
