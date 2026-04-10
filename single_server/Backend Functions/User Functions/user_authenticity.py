### User Authenticity Module for Polaris Single Server
from user_utils import validate_credentials, generate_session_token, create_user, hash_password, invalidate_session

def user_login(username, password):
    # Validate user credentials
    if validate_credentials(username, password):
        # Generate a session token
        session_token = generate_session_token(username)
        return session_token
    else:
        raise AuthenticationError("Invalid username or password")

def user_signup(username, password):
    # Create a new user
    if create_user(username, hash_password(password)):
        return True
    else:
        raise SignupError("Username already exists")

def user_logout(session_token):
    # Invalidate the session token
    if invalidate_session(session_token):
        return True
    else:
        raise LogoutError("Invalid session token")
