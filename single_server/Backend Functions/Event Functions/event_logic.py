def create_e(cursor, db, user_id, organization_id, caption): 
    # Check if user is the orgnization leader
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

    # Check if event with the same caption already exists in the organization
    cursor.execute(
        """
        SELECT event_id
        FROM events
        WHERE org_id = %s AND caption = %s
        """,
        (organization_id, caption),
    )
    if cursor.fetchone() is not None:
        return None

    # Create event and insert it into the database, returning the event ID
    cursor.execute(
        """
        INSERT INTO events (org_id, caption)
        VALUES (%s, %s)
        """,
        (organization_id, caption),
    )
    db.commit()

    return cursor.lastrowid


def designate_e_token(cursor, db, user_id, event_id, token_id): 
    # Check if user is the organization leader
    cursor.execute(
        """
        SELECT e.org_id
        FROM events e
        JOIN organization_leader ol ON e.org_id = ol.org_id
        WHERE e.event_id = %s AND ol.user_id = %s
        """,
        (event_id, user_id),
    )
    event = cursor.fetchone()
    if event is None:
        return None

    organization_id = event[0]

    # Check if the token is valid and belongs to the organization
    cursor.execute(
        """
        SELECT 1
        FROM organization_token
        WHERE token_id = %s AND org_id = %s
        """,
        (token_id, organization_id),
    )
    if cursor.fetchone() is None:
        return None

    # Designate the token for the event in the database
    cursor.execute(
        """
        SELECT 1
        FROM event_tokens_allowed
        WHERE event_id = %s AND token_id = %s
        """,
        (event_id, token_id),
    )
    if cursor.fetchone() is not None:
        return None

    cursor.execute(
        """
        INSERT INTO event_tokens_allowed (event_id, token_id)
        VALUES (%s, %s)
        """,
        (event_id, token_id),
    )
    db.commit()

    return True


def designate_e_market_creator(cursor, db, user_id, event_id, market_creator_id): 
    # Check if user is the organization leader
    cursor.execute(
        """
        SELECT e.org_id
        FROM events e
        JOIN organization_leader ol ON e.org_id = ol.org_id
        WHERE e.event_id = %s AND ol.user_id = %s
        """,
        (event_id, user_id),
    )
    event = cursor.fetchone()
    if event is None:
        return None

    organization_id = event[0]

    # Check if the market creator is valid and belongs to the organization
    cursor.execute(
        """
        SELECT 1
        FROM user_org_role
        WHERE org_id = %s AND user_id = %s
        """,
        (organization_id, market_creator_id),
    )
    if cursor.fetchone() is None:
        return None

    # Designate the market creator for the event in the database
    cursor.execute(
        """
        SELECT 1
        FROM event_market_creators
        WHERE event_id = %s AND user_id = %s
        """,
        (event_id, market_creator_id),
    )
    if cursor.fetchone() is not None:
        return None

    cursor.execute(
        """
        INSERT INTO event_market_creators (event_id, user_id)
        VALUES (%s, %s)
        """,
        (event_id, market_creator_id),
    )
    db.commit()

    return True


def designate_e_contraint(cursor, db, user_id, event_id, constraint_id, value): 
    # Check if user is the organization leader
    cursor.execute(
        """
        SELECT 1
        FROM events e
        JOIN organization_leader ol ON e.org_id = ol.org_id
        WHERE e.event_id = %s AND ol.user_id = %s
        """,
        (event_id, user_id),
    )
    if cursor.fetchone() is None:
        return None

    # Check if the constraint is valid
    cursor.execute(
        """
        SELECT 1
        FROM constraint_type
        WHERE constraint_id = %s
        """,
        (constraint_id,),
    )
    if cursor.fetchone() is None:
        return None

    # Designate the constraint for the event in the database
    cursor.execute(
        """
        SELECT 1
        FROM event_constraints
        WHERE event_id = %s AND constraint_id = %s
        """,
        (event_id, constraint_id),
    )
    if cursor.fetchone() is not None:
        return None

    cursor.execute(
        """
        INSERT INTO event_constraints (event_id, constraint_id, constraint_value)
        VALUES (%s, %s, %s)
        """,
        (event_id, constraint_id, value),
    )
    db.commit()

    return True


def designate_e_open_to(cursor, db, user_id, event_id, role_id): 
    # Check if user is the organization leader
    cursor.execute(
        """
        SELECT e.org_id
        FROM events e
        JOIN organization_leader ol ON e.org_id = ol.org_id
        WHERE e.event_id = %s AND ol.user_id = %s
        """,
        (event_id, user_id),
    )
    event = cursor.fetchone()
    if event is None:
        return None

    organization_id = event[0]

    # Check if the role is valid
    cursor.execute(
        """
        SELECT 1
        FROM organization_role
        WHERE org_id = %s AND role = %s
        """,
        (organization_id, role_id),
    )
    if cursor.fetchone() is None:
        return None

    # Designate the event to be open to the specified role in the database
    cursor.execute(
        """
        SELECT 1
        FROM event_open_to
        WHERE event_id = %s AND org_id = %s AND role_id = %s
        """,
        (event_id, organization_id, role_id),
    )
    if cursor.fetchone() is not None:
        return None

    cursor.execute(
        """
        INSERT INTO event_open_to (event_id, org_id, role_id)
        VALUES (%s, %s, %s)
        """,
        (event_id, organization_id, role_id),
    )
    db.commit()

    return True
    

def designate_e_closed(cursor, db, user_id, event_id): 
    # Check if user is the organization leader
    cursor.execute(
        """
        SELECT 1
        FROM events e
        JOIN organization_leader ol ON e.org_id = ol.org_id
        WHERE e.event_id = %s AND ol.user_id = %s
        """,
        (event_id, user_id),
    )
    if cursor.fetchone() is None:
        return None

    # Check if markets are all closed for the event
    cursor.execute(
        """
        SELECT 1
        FROM market
        WHERE event_id = %s AND is_open = TRUE
        """,
        (event_id,),
    )
    if cursor.fetchone() is not None:
        return None

    # Close the event in the database
    cursor.execute(
        """
        UPDATE events
        SET is_open = FALSE
        WHERE event_id = %s
        """,
        (event_id,),
    )
    db.commit()

    return True
