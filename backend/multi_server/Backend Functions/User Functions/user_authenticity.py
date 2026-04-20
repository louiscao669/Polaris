"""User authentication helpers. Expected failures are returned as dicts, not raised."""

from __future__ import annotations

from typing import Any

import pymysql

from user_utils import (
    generate_session_token,
    hash_password,
    invalidate_session,
    validate_credentials,
)


def _ok(**extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"ok": True}
    out.update(extra)
    return out


def _fail(error: str, message: str) -> dict[str, Any]:
    """error: auth | duplicate | validation"""
    return {"ok": False, "error": error, "message": message}


def user_login(cursor, db, username: str, password: str):
    """Validate credentials and issue a session token (cursor/db reserved for DB-backed auth)."""
    del cursor, db
    if not validate_credentials(username, password):
        return _fail("auth", "Invalid username or password.")
    token = generate_session_token(username)
    return _ok(session_token=token)


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
    except pymysql.err.IntegrityError as e:
        db.rollback()
        if e.args and e.args[0] == 1062:
            return _fail(
                "duplicate",
                "That username or email is already registered.",
            )
        return _fail("validation", str(e))
    except pymysql.err.Error as e:
        db.rollback()
        return _fail("validation", str(e))
    return _ok(user_id=cursor.lastrowid)


def user_logout(cursor, db, session_token: str):
    del cursor, db
    if invalidate_session(session_token):
        return _ok()
    return _fail("auth", "Invalid session token.")
