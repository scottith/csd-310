# Scott Macioce
# Module 8.2
# Movies Update and Delete

import mysql.connector
from mysql.connector import errorcode
from dotenv import dotenv_values

# Load secrets from .env
secrets = dotenv_values(".env")

# Database configuration
config = {
    "user": secrets["USER"],
    "password": secrets["PASSWORD"],
    "host": secrets["HOST"],
    "database": secrets["DATABASE"],
    "raise_on_warnings": True
}

def show_films(cursor, title):
    """ Method to execute an inner join on all tables and output the results """
    cursor.execute("""
        SELECT film_name AS Name, film_director AS Director, genre_name AS Genre, studio_name AS 'Studio Name'
        FROM film
        INNER JOIN genre ON film.genre_id = genre.genre_id
        INNER JOIN studio ON film.studio_id = studio.studio_id
    """)
    films = cursor.fetchall()

    print("\n-- {} --".format(title))

    for film in films:
        print("Film Name: {}\nDirector: {}\nGenre Name ID: {}\nStudio Name: {}\n".format(film[0], film[1], film[2], film[3]))

try:
    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    # Display films before changes
    show_films(cursor, "DISPLAYING FILMS")

    # Insert a new film (Inception)
    cursor.execute("""
        INSERT INTO film (film_name, film_releaseDate, film_runtime, film_director, studio_id, genre_id)
        VALUES ('Inception', '2010', 148, 'Christopher Nolan', 1, 2)
    """)
    db.commit()

    # Display films after insert
    show_films(cursor, "DISPLAYING FILMS AFTER INSERT")

    # Update Alien to be a Horror film (change genre_id to 1)
    cursor.execute("""
        UPDATE film
        SET genre_id = 1
        WHERE film_name = 'Alien'
    """)
    db.commit()

    # Display films after update
    show_films(cursor, "DISPLAYING FILMS AFTER UPDATE- Changed Alien to Horror")

    # Delete Gladiator
    cursor.execute("""
        DELETE FROM film
        WHERE film_name = 'Gladiator'
    """)
    db.commit()

    # Display films after delete
    show_films(cursor, "DISPLAYING FILMS AFTER DELETE")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("The supplied username or password are invalid.")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("The specified database does not exist.")
    else:
        print(err)

finally:
    if db.is_connected():
        db.close()
