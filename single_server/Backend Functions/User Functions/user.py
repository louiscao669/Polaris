def user_signup(cursor, db, first, last, email, username, password, age=None):

    cursor.execute(
        """
        INSERT INTO users (first, last, age, email, username, password_hash)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (first, last, age, email, username, password),
    )
    db.commit()
    
    # 3. Return the ID so the frontend can use it
    return cursor.lastrowid