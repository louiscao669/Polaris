import pymysql
from fail import _fail

def read_user_organizations(cursor, db, user_id):
    try:
        # Check if the user exists
        cursor.execute(
            """
            SELECT 1
            FROM users
            WHERE id = %s
            """,
            (user_id,),
        )
        if cursor.fetchone() is None:
            return _fail("validation", "That user does not exist.")

        # Read every organization where the user is a leader or has an organization role
        cursor.execute(
            """
            SELECT
                o.org_id,
                o.name,
                o.description,
                MAX(CASE WHEN ol.user_id = %s THEN 1 ELSE 0 END) AS is_leader,
                MAX(uor.role_id) AS role_id
            FROM organization o
            LEFT JOIN organization_leader ol ON o.org_id = ol.org_id
            LEFT JOIN user_org_role uor ON o.org_id = uor.org_id AND uor.user_id = %s
            WHERE ol.user_id = %s OR uor.user_id = %s
            GROUP BY o.org_id, o.name, o.description
            ORDER BY o.org_id ASC
            """,
            (user_id, user_id, user_id, user_id),
        )

        return [
            {
                "organization_id": row[0],
                "name": row[1],
                "description": row[2],
                "is_leader": bool(row[3]),
                "role_id": row[4],
            }
            for row in cursor.fetchall()
        ]

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to read the user's organizations: {e}")

def read_o(cursor, db, user_id, organization_id):
    try:
        # Check if the user is the organization leader or a member of the organization
        cursor.execute(
            """
            SELECT
                o.name,
                o.description,
                MAX(CASE WHEN ol.user_id = %s THEN 1 ELSE 0 END) AS is_leader,
                MAX(uor.role_id) AS role_id
            FROM organization o
            LEFT JOIN organization_leader ol ON o.org_id = ol.org_id
            LEFT JOIN user_org_role uor ON o.org_id = uor.org_id AND uor.user_id = %s
            WHERE o.org_id = %s
            GROUP BY o.org_id, o.name, o.description
            """,
            (user_id, user_id, organization_id),
        )
        organization = cursor.fetchone()
        if organization is None:
            return _fail("validation", "That organization does not exist.")

        if organization[2] == 0 and organization[3] is None:
            return _fail("permission", "You do not have permission to read that organization.")

        # Read organization metadata, roles, tokens, and current members
        cursor.execute(
            """
            SELECT role, description
            FROM organization_role
            WHERE org_id = %s
            ORDER BY role ASC
            """,
            (organization_id,),
        )
        roles = [
            {
                "role_id": row[0],
                "description": row[1],
            }
            for row in cursor.fetchall()
        ]

        cursor.execute(
            """
            SELECT token_id, name, description
            FROM organization_token
            WHERE org_id = %s
            ORDER BY token_id ASC
            """,
            (organization_id,),
        )
        tokens = [
            {
                "token_id": row[0],
                "name": row[1],
                "description": row[2],
            }
            for row in cursor.fetchall()
        ]

        cursor.execute(
            """
            SELECT u.id, u.username, u.first, u.last, 'leader'
            FROM organization_leader ol
            JOIN users u ON ol.user_id = u.id
            WHERE ol.org_id = %s
            UNION ALL
            SELECT u.id, u.username, u.first, u.last, uor.role_id
            FROM user_org_role uor
            JOIN users u ON uor.user_id = u.id
            WHERE uor.org_id = %s
            ORDER BY 1 ASC, 5 ASC
            """,
            (organization_id, organization_id),
        )
        members = [
            {
                "user_id": row[0],
                "username": row[1],
                "first": row[2],
                "last": row[3],
                "role_id": row[4],
            }
            for row in cursor.fetchall()
        ]

        return {
            "organization_id": organization_id,
            "name": organization[0],
            "description": organization[1],
            "is_leader": bool(organization[2]),
            "role_id": organization[3],
            "roles": roles,
            "tokens": tokens,
            "members": members,
        }

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to read the organization: {e}")
