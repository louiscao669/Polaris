def user_signup(cursor, db, first, last, email, username, password, age=None):
    cursor.execute(
        """
        INSERT INTO users (first, last, age, email, username, password)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (first, last, age, email, username, password),
    )
    db.commit()
    return cursor.lastrowid
