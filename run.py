#!/usr/bin/env python3

import csv
import mysql.connector

db: mysql.connector.CMySQLConnection

# Problem 1 (5 pt.)
def initialize_database() -> None:
    schemas = [
        '''
        CREATE TABLE IF NOT EXISTS movies (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            title VARCHAR(255) NOT NULL UNIQUE,
            director VARCHAR(255) NOT NULL,
            price INT NOT NULL
        )
        ''',
        '''
        CREATE TABLE IF NOT EXISTS customers (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255) NOT NULL,
            age INT NOT NULL,
            UNIQUE (name, age)
        )
        ''',
        '''
        CREATE TABLE IF NOT EXISTS bookings (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            movie_id BIGINT NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
            customer_id BIGINT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
            UNIQUE (movie_id, customer_id)
        )
        ''',
        '''
        CREATE TABLE IF NOT EXISTS ratings (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            movie_id BIGINT NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
            customer_id BIGINT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
            rating INT NOT NULL,
            UNIQUE (movie_id, customer_id)
        )
        ''',
    ]

    movies = []
    customers = []
    bookings = []

    with open('data.csv', newline='') as f:
        data = csv.reader(f)
        next(data) # skip header
        for (title, director, price, name, age) in data:
            movie = (title, director, int(price))
            customer = (name, int(age))
            booking = (title, name, int(age))

            if movie not in movies:
                movies.append(movie)
            if customer not in customers:
                customers.append(customer)
            if booking not in bookings:
                bookings.append(booking)

    with db.cursor() as c:
        for schema in schemas:
            c.execute(schema)

        c.executemany(
            '''
            INSERT IGNORE INTO movies (title, director, price)
            VALUES (%s, %s, %s)
            ''',
            movies,
        )
        c.executemany(
            '''
            INSERT IGNORE INTO customers (name, age)
            VALUES (%s, %s)
            ''',
            customers,
        )
        c.executemany(
            '''
            INSERT IGNORE INTO bookings (movie_id, customer_id)
            VALUES (
                (SELECT id FROM movies WHERE title = %s),
                (SELECT id FROM customers WHERE name = %s AND age = %s)
            )
            ''',
            bookings,
        )
        db.commit()

    print('Database successfully initialized')

# Problem 15 (5 pt.)
def reset() -> None:
    ok = input('are you sure? (y/N) ')
    if ok != 'y':
        return

    with db.cursor() as c:
        c.execute(
            '''
            DROP TABLE IF EXISTS movies, customers, bookings, ratings;
            ''',
            multi=True,
        )
        db.commit()
    initialize_database()

# Problem 2 (4 pt.)
def print_movies() -> None:
    with db.cursor() as c:
        c.execute(
            '''
            SELECT movies.id, title, director, price, COUNT(customers.id), AVG(rating)
            FROM movies
                LEFT JOIN bookings ON movies.id = bookings.movie_id
                LEFT JOIN customers ON bookings.customer_id = customers.id
                LEFT JOIN ratings ON movies.id = ratings.movie_id
            GROUP BY movies.id
            ORDER BY movies.id ASC;
            ''',
        )
        print('-' * 80)
        print(f'{"id":<8}{"title":<16}{"director":<16}{"price":<12}{"reservation":<13}{"avg. rating":<15}')
        print('-' * 80)
        for (id, title, director, price, bookings, rating) in c.fetchall():
            if rating is None:
                rating = 'None'
            print(f'{id:<8}{title:<16}{director:<16}{price:<12}{bookings:<13}{rating:<15}')
        print('-' * 80)

# Problem 3 (4 pt.)
def print_users() -> None:
    with db.cursor() as c:
        c.execute(
            '''
            SELECT id, name, age FROM customers
            ORDER BY id ASC;
            ''',
        )
        print('-' * 80)
        print(f'{"id":<8}{"name":<56}{"age":<16}')
        print('-' * 80)
        for (id, name, age) in c.fetchall():
            print(f'{id:<8}{name:<56}{age:<16}')
        print('-' * 80)

# Problem 4 (4 pt.)
def insert_movie() -> None:
    title = input('Movie title: ')
    director = input('Movie director: ')
    price = input('Movie price: ')

    if not 0 <= int(price) <= 100000:
        print('Movie price should be from 0 to 100000')
        return

    with db.cursor() as c:
        try:
            c.execute(
                '''
                INSERT INTO movies (title, director, price)
                VALUES (%s, %s, %s);
                ''',
                (title, director, int(price)),
            )
            db.commit()
        except mysql.connector.errors.IntegrityError:
            print(f'Movie {title} already exists')
            return

    print('One movie successfully inserted')

# Problem 6 (4 pt.)
def remove_movie() -> None:
    movie_id = input('Movie ID: ')

    with db.cursor() as c:
        c.execute(
            '''
            DELETE FROM movies
            WHERE id = %s;
            ''',
            (movie_id,),
        )
        if c.rowcount != 1:
            print(f'Movie {movie_id} does not exist')
            return
        db.commit()

    print('One movie successfully removed')

# Problem 5 (4 pt.)
def insert_user() -> None:
    name = input('User name: ')
    age = input('User age: ')

    if not 12 <= int(age) <= 110:
        print('User age should be from 12 to 110')
        return

    with db.cursor() as c:
        try:
            c.execute(
                '''
                INSERT INTO customers (name, age)
                VALUES (%s, %s);
                ''',
                (name, int(age)),
            )
            db.commit()
        except mysql.connector.errors.IntegrityError:
            print(f'User ({name}, {age}) already exists')
            return

    print('One user successfully inserted')

# Problem 7 (4 pt.)
def remove_user() -> None:
    user_id = input('User ID: ')

    with db.cursor() as c:
        c.execute(
            '''
            DELETE FROM customers
            WHERE id = %s;
            ''',
            (user_id,),
        )
        if c.rowcount != 1:
            print(f'User {user_id} does not exist')
            return
        db.commit()

    print('One user successfully removed')

# Problem 8 (5 pt.)
def book_movie() -> None:
    movie_id = input('Movie ID: ')
    user_id = input('User ID: ')

    with db.cursor() as c:
        c.execute(
            '''
            SELECT COUNT(*) FROM bookings
            WHERE movie_id = %s;
            ''',
            (movie_id,),
        )
        count = c.fetchone()
        if count is not None and count[0] >= 10:
            print(f'Movie {movie_id} has already been fully booked')
            return

        c.execute(
            '''
            SELECT id FROM movies WHERE id = %s;
            ''',
            (movie_id,),
        )
        if c.fetchone() is None:
            print(f'Movie {movie_id} does not exist')
            return

        c.execute(
            '''
            SELECT id FROM customers WHERE id = %s;
            ''',
            (user_id,),
        )
        if c.fetchone() is None:
            print(f'User {user_id} does not exist')
            return

        try:
            c.execute(
                '''
                INSERT INTO bookings (movie_id, customer_id)
                VALUES (%s, %s);
                ''',
                (movie_id, user_id),
            )
            db.commit()
        except mysql.connector.errors.IntegrityError:
            print(f'User {user_id} already booked movie {movie_id}')
            return

    print('Movie successfully booked')

# Problem 9 (5 pt.)
def rate_movie() -> None:
    movie_id = input('Movie ID: ')
    user_id = input('User ID: ')
    rating = input('Ratings (1~5): ')

    if not 1 <= int(rating) <= 5:
        print(f'Wrong value for a rating')
        return

    with db.cursor() as c:
        c.execute(
            '''
            SELECT id FROM movies WHERE id = %s;
            ''',
            (movie_id,),
        )
        if c.fetchone() is None:
            print(f'Movie {movie_id} does not exist')
            return

        c.execute(
            '''
            SELECT id FROM customers WHERE id = %s;
            ''',
            (user_id,),
        )
        if c.fetchone() is None:
            print(f'User {user_id} does not exist')
            return

        c.execute(
            '''
            SELECT id FROM bookings
            WHERE movie_id = %s AND customer_id = %s;
            ''',
            (movie_id, user_id),
        )
        if c.fetchone() is None:
            print(f'User {user_id} has not booked movie {movie_id} yet')
            return

        try:
            c.execute(
                '''
                INSERT INTO ratings (movie_id, customer_id, rating)
                VALUES (%s, %s, %s);
                ''',
                (movie_id, user_id, int(rating)),
            )
            db.commit()
        except mysql.connector.errors.IntegrityError:
            print(f'User {user_id} has already rated movie {movie_id}')
            return

    print('Movie successfully rated')

# Problem 10 (5 pt.)
def print_users_for_movie() -> None:
    movie_id = input('Movie ID: ')

    with db.cursor() as c:
        c.execute(
            '''
            SELECT id FROM movies WHERE id = %s;
            ''',
            (movie_id,),
        )
        if c.fetchone() is None:
            print(f'Movie {movie_id} does not exist')
            return

        c.execute(
            '''
            SELECT customers.id, name, age, rating
            FROM movies
                LEFT JOIN bookings ON movies.id = bookings.movie_id
                LEFT JOIN customers ON bookings.customer_id = customers.id
                LEFT JOIN ratings ON movies.id = ratings.movie_id AND customers.id = ratings.customer_id
            WHERE movies.id = %s
            ORDER BY customers.id ASC;
            ''',
            (movie_id,),
        )

        print('-' * 80)
        print(f'{"id":<8}{"name":<40}{"age":<8}{"rating":<24}')
        print('-' * 80)
        for (id, name, age, rating) in c.fetchall():
            if rating is None:
                rating = 'None'
            print(f'{id:<8}{name:<40}{age:<8}{rating:<24}')
        print('-' * 80)

# Problem 11 (5 pt.)
def print_movies_for_user() -> None:
    user_id = input('User ID: ')

    with db.cursor() as c:
        c.execute(
            '''
            SELECT id FROM customers WHERE id = %s;
            ''',
            (user_id,),
        )
        if c.fetchone() is None:
            print(f'User {user_id} does not exist')
            return

        c.execute(
            '''
            SELECT movies.id, title, director, price, rating
            FROM movies
                LEFT JOIN bookings ON movies.id = bookings.movie_id
                LEFT JOIN customers ON bookings.customer_id = customers.id
                LEFT JOIN ratings ON movies.id = ratings.movie_id AND customers.id = ratings.customer_id
            WHERE customers.id = %s
            ORDER BY movies.id ASC;
            ''',
            (user_id,),
        )

        print('-' * 80)
        print(f'{"id":<8}{"title":<32}{"director":<16}{"price":<16}{"rating":<8}')
        print('-' * 80)
        for (id, title, director, price, rating) in c.fetchall():
            if rating is None:
                rating = 'None'
            print(f'{id:<8}{title:<32}{director:<16}{price:<16}{rating:<8}')
        print('-' * 80)

# Problem 12 (6 pt.)
def recommend_popularity() -> None:
    user_id = input('User ID: ')

    with db.cursor() as c:
        c.execute(
            '''
            SELECT id FROM customers WHERE id = %s;
            ''',
            (user_id,),
        )
        if c.fetchone() is None:
            print(f'User {user_id} does not exist')
            return

        c.execute(
            '''
            WITH
                -- movies that the user has seen
                seen_movies AS (
                    SELECT movies.id
                    FROM movies
                        LEFT JOIN bookings ON movies.id = bookings.movie_id
                        LEFT JOIN customers ON bookings.customer_id = customers.id
                    WHERE customers.id = %s
                ),
                -- movies that the user has not seen
                unseen_movies AS (
                    SELECT id FROM movies EXCEPT SELECT id FROM seen_movies
                ),
                -- unseen movies, with rating and reservation count
                rated_movies AS (
                    SELECT movies.id, AVG(rating) AS rating, COUNT(bookings.id) AS bookings
                    FROM movies
                        LEFT JOIN bookings ON movies.id = bookings.movie_id
                        LEFT JOIN customers ON bookings.customer_id = customers.id
                        LEFT JOIN ratings ON movies.id = ratings.movie_id AND customers.id = ratings.customer_id
                    WHERE movies.id IN (SELECT id FROM unseen_movies)
                    GROUP BY movies.id
                )

            (
                SELECT movies.id, title, director, price, bookings, rating
                FROM rated_movies as m
                    LEFT JOIN movies ON m.id = movies.id
                ORDER BY rating DESC, m.id ASC
                LIMIT 1
            ) UNION ALL (
                SELECT movies.id, title, director, price, bookings, rating
                FROM rated_movies as m
                    LEFT JOIN movies ON m.id = movies.id
                ORDER BY bookings DESC, m.id ASC
                LIMIT 1
            )
            ''',
            (user_id,),
        )

        print('Rating-based')
        print('-' * 80)
        print(f'{"id":<8}{"title":<16}{"director":<16}{"price":<12}{"reservation":<13}{"avg. rating":<15}')
        print('-' * 80)
        row = c.fetchone()
        if row is not None:
            (id, title, director, price, bookings, rating) = row
            if rating is None:
                rating = 'None'
            print(f'{id:<8}{title:<16}{director:<16}{price:<12}{bookings:<13}{rating:<15}')
        print('-' * 80)

        print('Popularity-based')
        print('-' * 80)
        print(f'{"id":<8}{"title":<16}{"director":<16}{"price":<12}{"reservation":<13}{"avg. rating":<15}')
        print('-' * 80)
        row = c.fetchone()
        if row is not None:
            (id, title, director, price, bookings, rating) = row
            if rating is None:
                rating = 'None'
            print(f'{id:<8}{title:<16}{director:<16}{price:<12}{bookings:<13}{rating:<15}')
        print('-' * 80)

# Problem 13 (10 pt.)
def recommend_item_based() -> None:
    user_id = input('User ID: ')

    with db.cursor() as c:
        c.execute(
            '''
            SELECT id FROM customers WHERE id = %s;
            ''',
            (user_id,),
        )
        if c.fetchone() is None:
            print(f'User {user_id} does not exist')
            return

        c.execute(
            '''
            SELECT EXISTS (SELECT 1 FROM ratings WHERE customer_id = %s LIMIT 1);
            ''',
            (user_id,),
        )
        row = c.fetchone()
        if row is None or row[0] == 0:
            print('Rating does not exist')
            return

        c.execute(
            '''
            WITH
                -- step 1: average rating for each user
                avg_rating AS (
                    SELECT
                        c.id AS customer_id,
                        IFNULL(AVG(rating), 0) AS avg_rating
                    FROM
                        customers c
                        LEFT JOIN ratings r ON c.id = r.customer_id
                    GROUP BY c.id
                ),
                -- step 1: user-item matrix, with average rating if NULL
                user_item_matrix AS (
                    SELECT
                        m.id AS movie_id,
                        c.id AS customer_id,
                        IFNULL(r.rating, ar.avg_rating) AS rating
                    FROM movies m
                        CROSS JOIN customers c
                        LEFT JOIN ratings r ON m.id = r.movie_id AND c.id = r.customer_id
                        LEFT JOIN avg_rating ar ON c.id = ar.customer_id
                ),
                -- step 2: cosine similarity between users
                user_similarity AS (
                    SELECT
                        m1.customer_id AS customer_id_1,
                        m2.customer_id AS customer_id_2,
                        SUM(m1.rating * m2.rating) / SQRT(SUM(m1.rating * m1.rating) * SUM(m2.rating * m2.rating)) AS similarity
                    FROM
                        user_item_matrix m1
                        LEFT JOIN user_item_matrix m2 ON m1.movie_id = m2.movie_id
                    GROUP BY m1.customer_id, m2.customer_id
                ),
                -- step 3: predict rating
                predicted_rating AS (
                    SELECT
                        m.movie_id,
                        SUM(s.similarity * m.rating) / SUM(s.similarity) AS rating
                    FROM
                        user_item_matrix m
                        LEFT JOIN user_similarity s ON m.customer_id = s.customer_id_1 AND m.customer_id != s.customer_id_2
                    WHERE
                        m.customer_id = %s
                    GROUP BY m.movie_id
                ),

                -- movies that the user has seen
                seen_movies AS (
                    SELECT movies.id
                    FROM movies
                        LEFT JOIN bookings ON movies.id = bookings.movie_id
                        LEFT JOIN customers ON bookings.customer_id = customers.id
                    WHERE customers.id = %s
                ),
                -- movies that the user has not seen
                unseen_movies AS (
                    SELECT id FROM movies EXCEPT SELECT id FROM seen_movies
                )

            SELECT
                movies.id, title, director, price,
                AVG(ratings.rating) AS avg_rating, predicted_rating.rating as predicted_rating
            FROM movies
                LEFT JOIN predicted_rating ON movies.id = predicted_rating.movie_id
                LEFT JOIN ratings ON movies.id = ratings.movie_id
            WHERE
                movies.id IN (SELECT id FROM unseen_movies)
            GROUP BY movies.id
            ORDER BY predicted_rating DESC, movies.id ASC
            LIMIT 1
            ''',
            (user_id, user_id),
        )
        row = c.fetchone()
        if row is not None:
            (id, title, director, price, avg_rating, predicted_rating) = row
            if avg_rating is None:
                avg_rating = 'None'
            if predicted_rating is None:
                predicted_rating = 'None'
            print('-' * 80)
            print(f'{"id":<8}{"title":<16}{"director":<16}{"price":<8}{"avg. rating":<15}{"expected rating":<17}')
            print('-' * 80)
            print(f'{id:<8}{title:<16}{director:<16}{price:<8}{avg_rating:<15}{predicted_rating:<17}')
            print('-' * 80)

# Total of 70 pt.
def main() -> None:
    global db

    _db = mysql.connector.connect(
        host='astronaut.snu.ac.kr',
        port=7000,
        user='DB2017_19937',
        password='DB2017_19937',
        database='DB2017_19937',
    )
    if isinstance(_db, mysql.connector.CMySQLConnection):
        db = _db
    else:
        print(_db)
        raise Exception('Failed to connect to the database')

    with db:
        while True:
            print('============================================================')
            print('1. initialize database')
            print('2. print all movies')
            print('3. print all users')
            print('4. insert a new movie')
            print('5. remove a movie')
            print('6. insert a new user')
            print('7. remove an user')
            print('8. book a movie')
            print('9. rate a movie')
            print('10. print all users who booked for a movie')
            print('11. print all movies booked by an user')
            print('12. recommend a movie for a user using popularity-based method')
            print('13. recommend a movie for a user using user-based collaborative filtering')
            print('14. exit')
            print('15. reset database')
            print('============================================================')
            menu = int(input('Select your action: '))

            if menu == 1:
                initialize_database()
            elif menu == 2:
                print_movies()
            elif menu == 3:
                print_users()
            elif menu == 4:
                insert_movie()
            elif menu == 5:
                remove_movie()
            elif menu == 6:
                insert_user()
            elif menu == 7:
                remove_user()
            elif menu == 8:
                book_movie()
            elif menu == 9:
                rate_movie()
            elif menu == 10:
                print_users_for_movie()
            elif menu == 11:
                print_movies_for_user()
            elif menu == 12:
                recommend_popularity()
            elif menu == 13:
                recommend_item_based()
            elif menu == 14:
                print('Bye!')
                break
            elif menu == 15:
                reset()
            else:
                print('Invalid action')
            print()


if __name__ == "__main__":
    main()
