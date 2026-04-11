### User Authenticity Module for Polaris Single Server
from user_utils import validate_credentials, generate_session_token, create_user, hash_password, invalidate_session
import pymysql
from fastapi import HTTPException

class AuthenticationError(HTTPException):
    def __init__(self, message):
        super().__init__(status_code=401, detail=message)

class LogoutError(HTTPException):
    def __init__(self, message):
        super().__init__(status_code=400, detail=message)

class SignupError(HTTPException):
    def __init__(self, message):
        super().__init__(status_code=400, detail=message)


        
def user_login(username, password):
    # Validate user credentials
    if validate_credentials(username, password):
        # Generate a session token
        session_token = generate_session_token(username)
        return session_token
    else:
        raise AuthenticationError("Invalid username or password")

def user_signup(cursor, db, first, last, email, username, password, age=None):
    hashed_pw = hash_password(password) 
    try:
        cursor.execute(
            """
            INSERT INTO users (first, last, age, email, username, password_hash)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (first, last, age, email, username, hashed_pw),
        )
        db.commit()
    
    except pymysql.err.Error as e:
        db.rollback()
        raise SignupError(str(e)) from e
    return cursor.lastrowid

def user_logout(session_token):
    # Invalidate the session token
    if invalidate_session(session_token):
        return True
    else:
        raise LogoutError("Invalid session token") from None                
