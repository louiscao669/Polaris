from typing import Any

import pymysql
from fail import _fail, _log_result
try:
    from app.database import get_connection
except ImportError:
    from backend.event_bus.app.database import get_connection

from user_utils import (
    create_session,
    hash_password,
    invalidate_session,
    validate_credentials,
    validate_user_password,
)

def user_login(data: dict[str, Any]):
    username = data.get("username", data.get("login_identifier"))
    password = data.get("password")

    if username is None:
        result = _fail("validation", "user_login payload is missing required field 'username'.")
        _log_result("user_login", result)
        return result

    if password is None:
        result = _fail("validation", "user_login payload is missing required field 'password'.")
        _log_result("user_login", result)
        return result

    with get_connection() as db:
        cursor = db.cursor()
        result = _user_login(cursor, db, username, password)
    _log_result("user_login", result)
    return result


def _user_login(cursor, db, username, password):
    try:
        user_id = validate_credentials(cursor, username, password)
        if user_id is None:
            return _fail("auth", "Invalid username or password.")

        cursor.execute(
            """
            SELECT first, last, username
            FROM users
            WHERE id = %s
            """,
            (user_id,),
        )
        user_row = cursor.fetchone()
        if user_row is None:
            return _fail("validation", "Unable to load user profile after login.")

        token, expires_at = create_session(cursor, db, user_id)

        return {
            "user_id": user_id,
            "first": user_row[0],
            "last": user_row[1],
            "username": user_row[2],
            "session_token": token,
            "expires_at": expires_at,
        }

    except Exception as e:
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to log in: {e}")

def user_signup(data: dict[str, Any]):
    first = data.get("first")
    last = data.get("last")
    email = data.get("email")
    username = data.get("username")
    password = data.get("password")
    age = data.get("age")

    if first is None:
        result = _fail("validation", "user_signup payload is missing required field 'first'.")
        _log_result("user_signup", result)
        return result

    if last is None:
        result = _fail("validation", "user_signup payload is missing required field 'last'.")
        _log_result("user_signup", result)
        return result

    if email is None:
        result = _fail("validation", "user_signup payload is missing required field 'email'.")
        _log_result("user_signup", result)
        return result

    if username is None:
        result = _fail("validation", "user_signup payload is missing required field 'username'.")
        _log_result("user_signup", result)
        return result

    if password is None:
        result = _fail("validation", "user_signup payload is missing required field 'password'.")
        _log_result("user_signup", result)
        return result

    with get_connection() as db:
        cursor = db.cursor()
        result = _user_signup(cursor, db, first, last, email, username, password, age)
    _log_result("user_signup", result)
    return result


def _user_signup(cursor, db, first, last, email, username, password, age=None):
    try:
        hashed_pw = hash_password(password)

        # Check if a user with the same username or email already exists
        cursor.execute(
            """
            SELECT id
            FROM users
            WHERE username = %s OR email = %s
            """,
            (username, email),
        )
        existing_user = cursor.fetchone()
        if existing_user is not None:
            return existing_user[0]

        cursor.execute(
            """
            INSERT INTO users (first, last, age, email, username, password_hash)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (first, last, age, email, username, hashed_pw),
        )
        db.commit()

        return cursor.lastrowid

    except pymysql.err.IntegrityError as e:
        db.rollback()
        if e.args and e.args[0] == 1062:
            cursor.execute(
                """
                SELECT id
                FROM users
                WHERE username = %s OR email = %s
                """,
                (username, email),
            )
            existing_user = cursor.fetchone()
            if existing_user is not None:
                return existing_user[0]

        return _fail("validation", "Unable to create user.")

    except Exception as e:
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to create user: {e}")

def user_logout(data: dict[str, Any]):
    session_token = data.get("session_token")

    if session_token is None:
        result = _fail("validation", "user_logout payload is missing required field 'session_token'.")
        _log_result("user_logout", result)
        return result

    with get_connection() as db:
        cursor = db.cursor()
        result = _user_logout(cursor, db, session_token)
    _log_result("user_logout", result)
    return result


def _user_logout(cursor, db, session_token):
    try:
        if invalidate_session(cursor, db, session_token):
            return True

        return _fail("auth", "Invalid session token.")

    except Exception as e:
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to log out: {e}")
