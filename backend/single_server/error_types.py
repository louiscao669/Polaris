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

class AuthorizationError(Exception):
    pass    

class DuplicateEntryError(Exception):
    pass

class ValidationError(Exception):
    pass