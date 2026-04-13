def create_o(cursor, db, user_id, name, description):

    # Check if organization with the same name already exists

    # Create organization and insert it into the database, returning the organization ID
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


def create_o_role(cursor, db, user_id, organization_id, name, desc): 
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
        return None

    # Check if role with the same name already exists in the organization
    cursor.execute(
        """
        SELECT 1
        FROM organization_role
        WHERE org_id = %s AND role = %s
        """,
        (organization_id, name),
    )
    if cursor.fetchone() is not None:
        return None

    # Create role and insert it into the database, returning the role ID
    cursor.execute(
        """
        INSERT INTO organization_role (org_id, role, description)
        VALUES (%s, %s, %s)
        """,
        (organization_id, name, desc),
    )
    db.commit()
    
    return name


def create_o_token(cursor, db, user_id, organization_id, token_name, description=None):
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
        return None

    # Check if token with the same name already exists in the organization
    cursor.execute(
        """
        SELECT token_id
        FROM organization_token
        WHERE org_id = %s AND name = %s
        """,
        (organization_id, token_name),
    )
    if cursor.fetchone() is not None:
        return None

    # Create token and insert it into the database, returning the token ID 
    cursor.execute(
        """
        INSERT INTO organization_token (org_id, name, description)
        VALUES (%s, %s, %s)
        """,
        (organization_id, token_name, description),
    )
    db.commit()
    
    return cursor.lastrowid