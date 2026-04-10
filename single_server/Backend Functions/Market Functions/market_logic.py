
def create_m(user_id, event_id, question, description): 
    # Check if user has permission to create market in the event (or is organization leader)
    
    # Check if any markets with the same question already exist in the event

    # Create market and insert it into the database, returning the market ID

    return None

def designate_m_token(user_id, market_id, token_id): 
    # Check if user is the market creator or organization leader

    # Check if the token is valid and belongs to the organization

    # Designate the token for the market in the database

    return None

def designate_m_result(user_id, market_id, result): 
    # Check if user is the market creator or organization leader

    # Check if the result is valid

    # Designate the result for the market in the database

    # Call do_m_payout for all tokens in the market to distribute winnings

    # Close the market in the database

    return None
def designate_m_contraint(user_id, market_id, constraint_id, value):
    # Check if user is the market creator or organization leader

    # Check if the constraint is valid

    # Designate the constraint for the market in the database 
    
    return None

def designate_m_open_to_as(user_id, market_id, role_id, as_id): 
    # Check if user is the market creator or organization leader

    # Check if the role and AS are valid

    # Designate the market to be open to the specified role as an AS in the database

    return None

def do_m_transaction(user_id, market_id, token_id, type, side, qty): 
    # Check if user has permission to trade in the market (or is organization leader)

    # Check if the market is open and the token is valid

    # Check if the user is following event/market constraints

    # Execute the transaction in the database, updating liquidity and user balances accordingly
    
    return None

def do_m_payout(user_id, market_id, token_id): 
    # Check if user has permission to execute payout (or is organization leader)

    # Check if the market is closed and the token is valid

    # Calculate and distribute winnings to users based on their token holdings

    return None

def stats_m_liquidity(user_id, market_id): 
    # Check if user has permission to view market statistics (or is organization leader)

    # Retrieve and return liquidity statistics for the market

    return None

def stats_m_time_focus(user_id, market_id): 
    # Check if user has permission to view market statistics (or is organization leader)

    # Retrieve and return time focus statistics for the market

    return None

def stats_m_whales(user_id, market_id): 
    # Check if user has permission to view market statistics (or is organization leader)

    # Retrieve and return whale statistics for the market

    return None

def points_m(user_id, market_id, span): 
    # Check if user has permission to view market points (or is organization leader)

    # Retrieve and return points for the market over the specified time span

    return None