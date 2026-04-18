import secrets
from datetime import datetime, timedelta, timezone

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError

_PASSWORD_HASHER = PasswordHasher()
_SESSION_TTL = timedelta(days=7)


def validate_credentials(cursor, login_identifier, password):
    """Verify password for a row matched by username *or* email (same string users type at sign-in)."""
    if isinstance(login_identifier, (bytes, bytearray)):
        login_identifier = login_identifier.decode("utf-8")
    else:
        login_identifier = str(login_identifier)
    login_identifier = login_identifier.strip()
    if not login_identifier:
        return None

    password = password.decode("utf-8") if isinstance(password, (bytes, bytearray)) else str(password)

    # Match username exactly, email exactly, or email case-insensitively (sign-in uses email).
    # No LIMIT: duplicate emails/usernames in DB would make LIMIT 1 pick the wrong row and fail Argon2.
    email_lower = login_identifier.lower()
    cursor.execute(
        """
        SELECT id, password_hash
        FROM users
        WHERE username = %s
           OR email = %s
           OR LOWER(TRIM(email)) = %s
        """,
        (login_identifier, login_identifier, email_lower),
    )
    rows = cursor.fetchall()
    if not rows:
        return None

    for user_id, stored_password_hash in rows:
        if isinstance(stored_password_hash, (bytes, bytearray)):
            stored_password_hash = stored_password_hash.decode("utf-8")
        else:
            stored_password_hash = str(stored_password_hash)
        stored_password_hash = stored_password_hash.strip()

        try:
            _PASSWORD_HASHER.verify(stored_password_hash, password)
            return int(user_id)
        except (VerifyMismatchError, InvalidHashError):
            continue

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

    stored = row[0]
    if isinstance(stored, (bytes, bytearray)):
        stored = stored.decode("utf-8")
    else:
        stored = str(stored)

    try:
        return bool(_PASSWORD_HASHER.verify(stored, password))
    except (VerifyMismatchError, InvalidHashError):
        return False


def generate_session_token():
    return secrets.token_hex(32)


def create_session(cursor, db, user_id):
    session_token = generate_session_token()
    expires_at = datetime.now(timezone.utc) + _SESSION_TTL
    # mysql.connector TIMESTAMP pairs reliably with naive UTC here.
    expires_db = expires_at.replace(tzinfo=None)
    cursor.execute(
        """
        INSERT INTO user_session (user_id, session_token, expires_at)
        VALUES (%s, %s, %s)
        """,
        (user_id, session_token, expires_db),
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
