### Utility functions for user management in the Polaris single server implementation
from argon2 import PasswordHasher
import secrets

def validate_credentials(username, password):

    # FIND USER IN DATABASE AND RETRIEVE STORED PASSWORD HASH
    
    # COMPARE PROVIDED PASSWORD WITH STORED PASSWORD HASH
    ph = PasswordHasher()
    try:
        ph.verify(stored_password_hash, password)
        return True
    except:
        return False

def generate_session_token(username):
    # GENERATE A SECURE SESSION TOKEN
    session_token = secrets.token_hex(32)
    # STORE SESSION TOKEN IN DATABASE WITH ASSOCIATED USERNAME AND EXPIRATION TIME
    return session_token

def create_user(username, password_hash):
    # INTERACT WITH DATABASE TO CREATE USER
    return True

def hash_password(password):
    # Hash the password using Argon2
    ph = PasswordHasher()
    return ph.hash(password)

def invalidate_session(session_token):
    # INTERACT WITH DATABASE TO INVALIDATE SESSION
    return True