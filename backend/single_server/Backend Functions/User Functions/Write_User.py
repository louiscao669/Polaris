import pymysql

try:
    from mysql.connector import errors as mysql_errors
except ImportError:
    mysql_errors = None

def _fail(error, message):
    return {"ok": False, "error": error, "message": message}

from user_utils import (
    create_session,
    hash_password,
    invalidate_session,
    validate_credentials,
    validate_user_password,
)

def user_login(cursor, db, login_identifier, password):
    try:
        user_id = validate_credentials(cursor, login_identifier, password)
        if user_id is None:
            return _fail("auth", "Invalid username or password.")

        token, expires_at = create_session(cursor, db, user_id)

        return {
            "user_id": user_id,
            "session_token": token,
            "expires_at": expires_at,
        }

    except Exception as e:
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to log in: {e}")

def user_signup(cursor, db, first, last, email, username, password, age=None):
    hashed_pw = hash_password(password)
    try:
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
            return _fail(
                "duplicate",
                "A user with that username or email already exists.",
            )

        cursor.execute(
            """
            INSERT INTO users (first, last, age, email, username, password_hash)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (first, last, age, email, username, hashed_pw),
        )
        db.commit()

        return cursor.lastrowid

    except Exception as e:
        db.rollback()
        is_dup = False
        if isinstance(e, pymysql.err.IntegrityError):
            is_dup = bool(e.args and e.args[0] == 1062)
        elif mysql_errors and isinstance(e, mysql_errors.IntegrityError):
            is_dup = getattr(e, "errno", None) == 1062
        if is_dup:
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
                return _fail(
                    "duplicate",
                    "A user with that username or email already exists.",
                )
            return _fail("validation", "Unable to create user.")

        print(f"Transaction failed, rolled back: {e}")
        return _fail("validation", f"Unable to create user: {e}")

def user_logout(cursor, db, session_token):
    try:
        if invalidate_session(cursor, db, session_token):
            return True

        return _fail("auth", "Invalid session token.")

    except Exception as e:
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to log out: {e}")
