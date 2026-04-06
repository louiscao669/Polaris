### Organization Module for Polaris Single Server
def create_organization(org_name, org_description, owner_username):
    # FIND OWNER IN DATABASE AND OBTAIN OWNER ID

    # INSERT NEW ORGANIZATION INTO DATABASE WITH OWNER ID, NAME, AND DESCRIPTION
    
    return True

def create_rule_to_organization(org_id, rule_name, rule_description):
    # INSERT NEW RULE INTO DATABASE ASSOCIATED WITH THE ORGANIZATION ID
    
    return True

def create_tokens_for_organization(org_id, token_name, token_permissions):
    # GENERATE A SECURE TOKEN
    token = secrets.token_hex(32)
    # STORE TOKEN IN DATABASE WITH ASSOCIATED ORGANIZATION ID AND PERMISSIONS
    return token

def create_role_for_organization(org_id, role_name):
    # INSERT NEW ROLE INTO DATABASE ASSOCIATED WITH THE ORGANIZATION ID AND PERMISSIONS
    
    return True

def create_event_for_organization(org_id, event_name, event_description):
    # INSERT NEW EVENT INTO DATABASE ASSOCIATED WITH THE ORGANIZATION ID
    
    return True

def create_event_market_author(org_id, event_id, new_user_id, leader_user_id):
    # INSERT NEW EVENT MARKET AUTHOR INTO DATABASE ASSOCIATED WITH THE ORGANIZATION ID AND EVENT ID
    
    return True