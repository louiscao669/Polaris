from typing import Any

import pymysql
from fail import _fail
try:
    from ..database import get_connection
except ImportError:
    from backend.event_bus.app.database import get_connection

def update_o(data: dict[str, Any]):
    user_id = data.get("user_id")
    organization_id = data.get("organization_id")
    name = data.get("name")
    description = data.get("description")

    if user_id is None:
        return _fail("validation", "update_o payload is missing required field 'user_id'.")

    if organization_id is None:
        return _fail("validation", "update_o payload is missing required field 'organization_id'.")

    with get_connection() as db:
        cursor = db.cursor()
        return _update_o(cursor, db, user_id, organization_id, name, description)


def _update_o(cursor, db, user_id, organization_id, name=None, description=None):
    try:
        # Check if user is the organization leader
        cursor.execute(
            """
            SELECT 1
            FROM organization_leader
            WHERE org_id = %s AND user_id = %s
            """,
            (organization_id, user_id),
        )
        if cursor.fetchone() is None:
            return _fail("permission", "Only the organization leader can update the organization.")

        # Check if the organization exists
        cursor.execute(
            """
            SELECT name, description
            FROM organization
            WHERE org_id = %s
            """,
            (organization_id,),
        )
        current_organization = cursor.fetchone()
        if current_organization is None:
            return _fail("validation", "That organization does not exist.")

        next_name = current_organization[0] if name is None else name
        next_description = current_organization[1] if description is None else description

        # Check if the updated organization name is already in use
        cursor.execute(
            """
            SELECT org_id
            FROM organization
            WHERE name = %s AND org_id <> %s
            """,
            (next_name, organization_id),
        )
        if cursor.fetchone() is not None:
            return _fail("duplicate", "An organization with that name already exists.")

        # Check if the organization is already set to those values
        if current_organization == (next_name, next_description):
            return organization_id

        # Update the organization in the database
        cursor.execute(
            """
            UPDATE organization
            SET name = %s, description = %s
            WHERE org_id = %s
            """,
            (next_name, next_description, organization_id),
        )
        db.commit()

        return organization_id

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:
            return _fail("duplicate", "An organization with that name already exists.")

        return _fail("validation", "Unable to update the organization.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to update the organization: {e}")

def update_o_role(data: dict[str, Any]):
    user_id = data.get("user_id")
    organization_id = data.get("organization_id")
    role_id = data.get("role_id")
    desc = data.get("desc")

    if user_id is None:
        return _fail("validation", "update_o_role payload is missing required field 'user_id'.")

    if organization_id is None:
        return _fail("validation", "update_o_role payload is missing required field 'organization_id'.")

    if role_id is None:
        return _fail("validation", "update_o_role payload is missing required field 'role_id'.")

    with get_connection() as db:
        cursor = db.cursor()
        return _update_o_role(cursor, db, user_id, organization_id, role_id, desc)


def _update_o_role(cursor, db, user_id, organization_id, role_id, desc=None):
    try:
        # Check if user is the organization leader
        cursor.execute(
            """
            SELECT 1
            FROM organization_leader
            WHERE org_id = %s AND user_id = %s
            """,
            (organization_id, user_id),
        )
        if cursor.fetchone() is None:
            return _fail("permission", "Only the organization leader can update roles.")

        # Check if the role exists in the organization
        cursor.execute(
            """
            SELECT description
            FROM organization_role
            WHERE org_id = %s AND role = %s
            """,
            (organization_id, role_id),
        )
        current_role = cursor.fetchone()
        if current_role is None:
            return _fail("validation", "That role does not exist in the organization.")

        next_description = current_role[0] if desc is None else desc

        # Check if the role is already set to that description
        if current_role[0] == next_description:
            return role_id

        # Update the role description in the database
        cursor.execute(
            """
            UPDATE organization_role
            SET description = %s
            WHERE org_id = %s AND role = %s
            """,
            (next_description, organization_id, role_id),
        )
        db.commit()

        return role_id

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to update the organization role: {e}")

def update_o_token(data: dict[str, Any]):
    user_id = data.get("user_id")
    organization_id = data.get("organization_id")
    token_id = data.get("token_id")
    token_name = data.get("token_name")
    description = data.get("description")

    if user_id is None:
        return _fail("validation", "update_o_token payload is missing required field 'user_id'.")

    if organization_id is None:
        return _fail("validation", "update_o_token payload is missing required field 'organization_id'.")

    if token_id is None:
        return _fail("validation", "update_o_token payload is missing required field 'token_id'.")

    with get_connection() as db:
        cursor = db.cursor()
        return _update_o_token(cursor, db, user_id, organization_id, token_id, token_name, description)


def _update_o_token(cursor, db, user_id, organization_id, token_id, token_name=None, description=None):
    try:
        # Check if user is the organization leader
        cursor.execute(
            """
            SELECT 1
            FROM organization_leader
            WHERE org_id = %s AND user_id = %s
            """,
            (organization_id, user_id),
        )
        if cursor.fetchone() is None:
            return _fail("permission", "Only the organization leader can update tokens.")

        # Check if the token exists in the organization
        cursor.execute(
            """
            SELECT name, description
            FROM organization_token
            WHERE org_id = %s AND token_id = %s
            """,
            (organization_id, token_id),
        )
        current_token = cursor.fetchone()
        if current_token is None:
            return _fail("validation", "That token does not exist in the organization.")

        next_name = current_token[0] if token_name is None else token_name
        next_description = current_token[1] if description is None else description

        # Check if the updated token name is already in use in the organization
        cursor.execute(
            """
            SELECT token_id
            FROM organization_token
            WHERE org_id = %s AND name = %s AND token_id <> %s
            """,
            (organization_id, next_name, token_id),
        )
        if cursor.fetchone() is not None:
            return _fail("duplicate", "That token name is already in use in the organization.")

        # Check if the token is already set to those values
        if current_token == (next_name, next_description):
            return token_id

        # Update the token in the database
        cursor.execute(
            """
            UPDATE organization_token
            SET name = %s, description = %s
            WHERE org_id = %s AND token_id = %s
            """,
            (next_name, next_description, organization_id, token_id),
        )
        db.commit()

        return token_id

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:
            return _fail("duplicate", "That token name is already in use in the organization.")

        return _fail("validation", "Unable to update the organization token.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to update the organization token: {e}")
