
def create_e(user_id, organization_id, caption): 
    # Check if user is the orgnization leader

    # Check if event with the same caption already exists in the organization

    # Create event and insert it into the database, returning the event ID

    return None

def designate_e_token(user_id, event_id, token_id): 
    # Check if user is the organization leader

    # Check if the token is valid and belongs to the organization

    # Designate the token for the event in the database

    return None

def designate_e_market_creator(user_id, event_id, market_creator_id): 
    # Check if user is the organization leader

    # Check if the market creator is valid and belongs to the organization

    # Designate the market creator for the event in the database

    return None

def designate_e_contraint(user_id, event_id, constraint_id, value): 
    # Check if user is the organization leader

    # Check if the constraint is valid

    # Designate the constraint for the event in the database

    return None

def designate_e_open_to(user_id, event_id, role_id): 
    # Check if user is the organization leader

    # Check if the role is valid

    # Designate the event to be open to the specified role in the database

    return None
    
def designate_e_closed(user_id, event_id): 
    # Check if user is the organization leader

    # Check if markets are all closed for the event

    # Close the event in the database

    return None