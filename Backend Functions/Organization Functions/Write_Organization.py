import pymysql
from fail import _fail

def create_o(cursor, db, user_id, name, description):
    try:
        # Check if an organization with the same name already exists
        cursor.execute(
            """
            SELECT o.org_id, o.description, ol.user_id
            FROM organization o
            LEFT JOIN organization_leader ol ON o.org_id = ol.org_id
            WHERE o.name = %s
            """,
            (name,),
        )
        existing_org = cursor.fetchone()
        if existing_org is not None:
            existing_org_id, existing_description, leader_user_id = existing_org

            # Treat an identical retry from the same leader as success
            if leader_user_id == user_id and existing_description == description:
                return existing_org_id

            return _fail("duplicate", "An organization with that name already exists.")

        # Create the organization and assign its leader in the database
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

        return organization_id

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:
            # Re-check the existing organization so duplicate requests remain idempotent
            cursor.execute(
                """
                SELECT o.org_id, o.description, ol.user_id
                FROM organization o
                LEFT JOIN organization_leader ol ON o.org_id = ol.org_id
                WHERE o.name = %s
                """,
                (name,),
            )
            existing_org = cursor.fetchone()
            if existing_org is not None:
                existing_org_id, existing_description, leader_user_id = existing_org

                if leader_user_id == user_id and existing_description == description:
                    return existing_org_id

        return _fail("validation", "Unable to create organization.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to create organization: {e}")

def create_o_role(cursor, db, user_id, organization_id, name, desc): 
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
            return _fail("permission", "Only the organization leader can create roles.")

        # Check if the role already exists in the organization
        cursor.execute(
            """
            SELECT description
            FROM organization_role
            WHERE org_id = %s AND role = %s
            """,
            (organization_id, name),
        )
        existing_role = cursor.fetchone()
        if existing_role is not None:
            if existing_role[0] == desc:
                return name

            return _fail("duplicate", "That role already exists with a different description.")

        # Create the role and insert it into the database
        cursor.execute(
            """
            INSERT INTO organization_role (org_id, role, description)
            VALUES (%s, %s, %s)
            """,
            (organization_id, name, desc),
        )
        db.commit()

        return name

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:
            # Re-check the existing role so duplicate requests remain idempotent
            cursor.execute(
                """
                SELECT description
                FROM organization_role
                WHERE org_id = %s AND role = %s
                """,
                (organization_id, name),
            )
            existing_role = cursor.fetchone()
            if existing_role is not None and existing_role[0] == desc:
                return name

        return _fail("validation", "Unable to create organization role.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to create organization role: {e}")

def create_o_token(cursor, db, user_id, organization_id, token_name, description=None):
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
            return _fail("permission", "Only the organization leader can create tokens.")

        # Check if the token already exists in the organization
        cursor.execute(
            """
            SELECT token_id, description
            FROM organization_token
            WHERE org_id = %s AND name = %s
            """,
            (organization_id, token_name),
        )
        existing_token = cursor.fetchone()
        if existing_token is not None:
            if existing_token[1] == description:
                return existing_token[0]

            return _fail("duplicate", "That token already exists with a different description.")

        # Create the token and insert it into the database
        cursor.execute(
            """
            INSERT INTO organization_token (org_id, name, description)
            VALUES (%s, %s, %s)
            """,
            (organization_id, token_name, description),
        )
        db.commit()

        return cursor.lastrowid

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:
            # Re-check the existing token so duplicate requests remain idempotent
            cursor.execute(
                """
                SELECT token_id, description
                FROM organization_token
                WHERE org_id = %s AND name = %s
                """,
                (organization_id, token_name),
            )
            existing_token = cursor.fetchone()
            if existing_token is not None and existing_token[1] == description:
                return existing_token[0]

        return _fail("validation", "Unable to create organization token.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to create organization token: {e}")

def create_user_in_role(cursor, db, user_id, organization_id, target_user_id, role_id):
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
            return _fail("permission", "Only the organization leader can assign organization roles.")

        # Check if the target user exists
        cursor.execute(
            """
            SELECT 1
            FROM users
            WHERE id = %s
            """,
            (target_user_id,),
        )
        if cursor.fetchone() is None:
            return _fail("validation", "The target user does not exist.")

        # Check if the role exists in the organization
        cursor.execute(
            """
            SELECT 1
            FROM organization_role
            WHERE org_id = %s AND role = %s
            """,
            (organization_id, role_id),
        )
        if cursor.fetchone() is None:
            return _fail("validation", "That role does not exist in the organization.")

        # Check if the user already has a role in the organization
        cursor.execute(
            """
            SELECT role_id
            FROM user_org_role
            WHERE org_id = %s AND user_id = %s
            """,
            (organization_id, target_user_id),
        )
        existing_role = cursor.fetchone()
        if existing_role is not None:
            if existing_role[0] == role_id:
                return True

            return _fail("precondition", "That user already has a different role in the organization.")

        # Assign the user to the role in the organization
        cursor.execute(
            """
            INSERT INTO user_org_role (org_id, role_id, user_id)
            VALUES (%s, %s, %s)
            """,
            (organization_id, role_id, target_user_id),
        )
        db.commit()

        return True

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:
            cursor.execute(
                """
                SELECT role_id
                FROM user_org_role
                WHERE org_id = %s AND user_id = %s
                """,
                (organization_id, target_user_id),
            )
            existing_role = cursor.fetchone()
            if existing_role is not None and existing_role[0] == role_id:
                return True

        return _fail("validation", "Unable to assign the user to the organization role.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to assign the user to the organization role: {e}")
