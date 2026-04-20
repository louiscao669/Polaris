from typing import Any

import pymysql
from fail import _fail
try:
    from ..database import get_connection
except ImportError:
    from backend.event_bus.app.database import get_connection

from user_utils import (
    create_session,
    hash_password,
    invalidate_session,
    validate_credentials,
    validate_user_password,
)

def update_user_profile(data: dict[str, Any]):
    user_id = data.get("user_id")
    target_user_id = data.get("target_user_id")
    username = data.get("username")
    email = data.get("email")
    first = data.get("first")
    last = data.get("last")
    age = data.get("age")

    if user_id is None:
        return _fail("validation", "update_user_profile payload is missing required field 'user_id'.")

    if target_user_id is None:
        return _fail("validation", "update_user_profile payload is missing required field 'target_user_id'.")

    with get_connection() as db:
        cursor = db.cursor()
        return _update_user_profile(cursor, db, user_id, target_user_id, username, email, first, last, age)


def _update_user_profile(cursor, db, user_id, target_user_id, username=None, email=None, first=None, last=None, age=None):
    try:
        # Check if the user is updating their own profile
        if user_id != target_user_id:
            return _fail("permission", "Users can only update their own profile.")

        # Check if the target user exists
        cursor.execute(
            """
            SELECT username, email, first, last, age
            FROM users
            WHERE id = %s
            """,
            (target_user_id,),
        )
        current_user = cursor.fetchone()
        if current_user is None:
            return _fail("validation", "That user does not exist.")

        next_username = current_user[0] if username is None else username
        next_email = current_user[1] if email is None else email
        next_first = current_user[2] if first is None else first
        next_last = current_user[3] if last is None else last
        next_age = current_user[4] if age is None else age

        # Check if the updated username or email are already in use
        cursor.execute(
            """
            SELECT id
            FROM users
            WHERE (username = %s OR email = %s) AND id <> %s
            """,
            (next_username, next_email, target_user_id),
        )
        if cursor.fetchone() is not None:
            return _fail("duplicate", "That username or email is already in use.")

        # Check if the user profile is already set to those values
        if current_user == (next_username, next_email, next_first, next_last, next_age):
            return target_user_id

        # Update the user profile in the database
        cursor.execute(
            """
            UPDATE users
            SET username = %s, email = %s, first = %s, last = %s, age = %s
            WHERE id = %s
            """,
            (next_username, next_email, next_first, next_last, next_age, target_user_id),
        )
        db.commit()

        return target_user_id

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:
            return _fail("duplicate", "That username or email is already in use.")

        return _fail("validation", "Unable to update the user profile.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to update the user profile: {e}")

def update_user_password(data: dict[str, Any]):
    user_id = data.get("user_id")
    current_password = data.get("current_password")
    new_password = data.get("new_password")

    if user_id is None:
        return _fail("validation", "update_user_password payload is missing required field 'user_id'.")

    if current_password is None:
        return _fail("validation", "update_user_password payload is missing required field 'current_password'.")

    if new_password is None:
        return _fail("validation", "update_user_password payload is missing required field 'new_password'.")

    with get_connection() as db:
        cursor = db.cursor()
        return _update_user_password(cursor, db, user_id, current_password, new_password)


def _update_user_password(cursor, db, user_id, current_password, new_password):
    try:
        # Check if the user exists and lock the user row
        cursor.execute(
            """
            SELECT password_hash
            FROM users
            WHERE id = %s
            FOR UPDATE
            """,
            (user_id,),
        )
        current_user = cursor.fetchone()
        if current_user is None:
            return _fail("validation", "That user does not exist.")

        # Check if the current password is valid
        if validate_user_password(cursor, user_id, current_password) is False:
            return _fail("auth", "Current password is incorrect.")

        next_password_hash = hash_password(new_password)

        # Check if the password already matches the requested value
        if validate_user_password(cursor, user_id, new_password):
            return True

        # Update the user password in the database
        cursor.execute(
            """
            UPDATE users
            SET password_hash = %s
            WHERE id = %s
            """,
            (next_password_hash, user_id),
        )
        db.commit()

        return True

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to update the user password: {e}")
