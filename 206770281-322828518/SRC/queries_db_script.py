import csv
import mysql.connector



def connect_to_database():
    conn = mysql.connector.connect(host="localhost", username="sibaazab", password="sib42918", database="sibaazab", port=3305)
    if conn.is_connected():
        #print("Successfully Connected")
        return conn
    print("Connection Failed")
    exit(-1)

#Show top 10 movies based on rating
def query_1():
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute(""" SELECT md.title_x AS movie_name
                        FROM movies_data md
                        ORDER BY rating DESC
                        LIMIT 10;""")
    return cursor.fetchall()

#show the information about a movie when clicking the name
def query_2(movie_name):
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute("""SELECT *
                    FROM movies_data
                    WHERE title_x = %s;""", (movie_name,))
    return cursor.fetchall()

# Show the last 10 released movies
def query_3():
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute("""SELECT title_x AS movie_name, release_date
                    FROM movies_data
                    ORDER BY release_date DESC
                    LIMIT 10;""")
    return cursor.fetchall()

#Show 10 most watched movies
def query_4():
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute("""SELECT title_x AS movie_name, popularity
                        FROM movies_data
                        ORDER BY popularity DESC
                        LIMIT 10;""")
    return cursor.fetchall()

#Show top 10 movies in a given genre
def query_5(genreName):
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT movies.movie_id,
            movies.title_x AS movie_name,
            movies.popularity
        FROM (
            SELECT mg.movie_id,
                md.title_x,
                md.popularity,
                ROW_NUMBER() OVER (ORDER BY md.popularity DESC) AS rn
            FROM movies_data md
            JOIN movies_genres mg ON md.movie_id = mg.movie_id
            JOIN genres_table gt ON mg.genres_id = gt.genres_id
            WHERE gt.genre_name = %s
        ) AS movies
        WHERE movies.rn <= 10;
    """, (genreName,))
    return cursor.fetchall()

#Show top 10 actors in a specific genre - based on movie_count for each actor
def query_6(genreName):
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute("""SELECT a.actor_name, COUNT(ma.movie_id) AS movie_count
                    FROM actors a
                    JOIN movie_actor ma ON a.actor_id = ma.actor_id
                    JOIN movies_genres mg ON ma.movie_id = mg.movie_id
                    JOIN genres_table gt ON mg.genres_id = gt.genres_id
                    WHERE gt.genre_name = %s
                    GROUP BY a.actor_name
                    ORDER BY movie_count DESC
                    LIMIT 10;""", (genreName,))
    return cursor.fetchall()

#Show 10 most profitable movies
def query_7():
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute("""SELECT m.title_x AS movie_name,
                    p.revenue,
                    p.budget,
                    (p.revenue - p.budget) AS profit
                    FROM movies_data m
                    JOIN profit p ON m.movie_id = p.movie_id
                    ORDER BY profit DESC
                    LIMIT 10;""")
    return cursor.fetchall()


#Show 10 most profitable production companies based on revenue and budget from all the movies that they produced.
def query_8():
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute("""SELECT p.production_company,
                    SUM(p.revenue) AS total_revenue,
                    SUM(p.budget) AS total_budget,
                    (SUM(p.revenue) - SUM(p.budget)) AS total_profit
                    FROM profit p
                    GROUP BY p.production_company
                    ORDER BY total_profit DESC
                    LIMIT 10;""")
    return cursor.fetchall()

#search a movie by keywords
def query_9(keyWord):
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute("""
                    SELECT DISTINCT md.title_x
                    FROM movies_data md
                    JOIN movie_keywords mk ON md.movie_id = mk.movie_id
                    WHERE MATCH(mk.keyword) AGAINST ( %s'*' IN BOOLEAN MODE);""", (keyWord,))
    return cursor.fetchall()

#genre button
def query_10():
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute("""
                    SELECT genre_name
                    FROM genres_table;""")
    return cursor.fetchall()
