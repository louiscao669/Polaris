import secrets
from datetime import datetime, timedelta, timezone

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError

_PASSWORD_HASHER = PasswordHasher()
_SESSION_TTL = timedelta(days=7)


def validate_credentials(cursor, username, password):
    cursor.execute(
        """
        SELECT id, password_hash
        FROM users
        WHERE username = %s OR email = %s
        """,
        (username, username),
    )
    row = cursor.fetchone()
    if row is None:
        return None

    user_id, stored_password_hash = row
    try:
        if _PASSWORD_HASHER.verify(stored_password_hash, password):
            return user_id
    except (VerifyMismatchError, InvalidHashError):
        return None

    return None


def validate_user_password(cursor, user_id, password):
    cursor.execute(
        """
        SELECT password_hash
        FROM users
        WHERE id = %s
        """,
        (user_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return False

    try:
        return bool(_PASSWORD_HASHER.verify(row[0], password))
    except (VerifyMismatchError, InvalidHashError):
        return False


def generate_session_token():
    return secrets.token_hex(32)


def create_session(cursor, db, user_id):
    session_token = generate_session_token()
    expires_at = datetime.now(timezone.utc) + _SESSION_TTL
    cursor.execute(
        """
        INSERT INTO user_session (user_id, session_token, expires_at)
        VALUES (%s, %s, %s)
        """,
        (user_id, session_token, expires_at),
    )
    db.commit()
    return session_token, expires_at


def hash_password(password):
    return _PASSWORD_HASHER.hash(password)


def invalidate_session(cursor, db, session_token):
    cursor.execute(
        """
        DELETE FROM user_session
        WHERE session_token = %s
        """,
        (session_token,),
    )
    db.commit()
    return cursor.rowcount > 0
