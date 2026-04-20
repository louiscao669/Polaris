"""Organization domain logic. Expected failures are returned as dicts, not raised."""

from __future__ import annotations

from typing import Any


def _ok(**extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"ok": True}
    out.update(extra)
    return out


def _fail(error: str, message: str) -> dict[str, Any]:
    """error: permission | duplicate | validation"""
    return {"ok": False, "error": error, "message": message}


def create_o(cursor, db, user_id, name, description):
    cursor.execute("SELECT 1 FROM users WHERE id = %s", (user_id,))
    if cursor.fetchone() is None:
        return _fail(
            "validation",
            f"No user with id {user_id}. Create the user in `users` first.",
        )

    cursor.execute(
        """
        SELECT 1
        FROM organization
        WHERE name = %s
        """,
        (name,),
    )
    if cursor.fetchone() is not None:
        return _fail(
            "duplicate",
            f"An organization named {name!r} already exists.",
        )

    cursor.execute(
        """
        INSERT INTO organization (name, description)
        VALUES (%s, %s)
        """,
        (name, description),
    )
    organization_id = cursor.lastrowid

    cursor.execute(
        """
        INSERT INTO organization_leader (org_id, user_id)
        VALUES (%s, %s)
        """,
        (organization_id, user_id),
    )
    db.commit()

    return _ok(organization_id=organization_id)


def create_o_role(cursor, db, user_id, organization_id, name, desc):
    cursor.execute(
        """
        SELECT 1
        FROM organization_leader
        WHERE org_id = %s AND user_id = %s
        """,
        (organization_id, user_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "permission",
            f"User {user_id} is not a leader of organization {organization_id}.",
        )

    cursor.execute(
        """
        SELECT 1
        FROM organization_role
        WHERE org_id = %s AND role = %s
        """,
        (organization_id, name),
    )
    if cursor.fetchone() is not None:
        return _fail(
            "duplicate",
            f"Role {name!r} already exists in organization {organization_id}.",
        )

    cursor.execute(
        """
        INSERT INTO organization_role (org_id, role, description)
        VALUES (%s, %s, %s)
        """,
        (organization_id, name, desc),
    )
    db.commit()

    return _ok(role=name)


def create_o_token(cursor, db, user_id, organization_id, token_name, description=None):
    cursor.execute(
        """
        SELECT 1
        FROM organization_leader
        WHERE org_id = %s AND user_id = %s
        """,
        (organization_id, user_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "permission",
            f"User {user_id} is not a leader of organization {organization_id}.",
        )

    cursor.execute(
        """
        SELECT token_id
        FROM organization_token
        WHERE org_id = %s AND name = %s
        """,
        (organization_id, token_name),
    )
    if cursor.fetchone() is not None:
        return _fail(
            "duplicate",
            f"Token {token_name!r} already exists in organization {organization_id}.",
        )

    cursor.execute(
        """
        INSERT INTO organization_token (org_id, name, description)
        VALUES (%s, %s, %s)
        """,
        (organization_id, token_name, description),
    )
    db.commit()

    return _ok(token_id=cursor.lastrowid)
