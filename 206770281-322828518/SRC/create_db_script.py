import csv
import mysql.connector
import pandas as pd


def connect_to_database():
    conn = mysql.connector.connect(host="localhost", username="sibaazab", password="sib42918", database="sibaazab", port=3305)
    if conn.is_connected():
        print("Successfully Connected")
        return conn
    print("Connection Failed")
    exit(-1)
    
#create movies_data table
def create_database_movies():
    data = pd.read_csv ('merged_tmdb_data.csv')
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE movies_data (
            movie_id INT PRIMARY KEY,
            title_x VARCHAR(1000),
            overview TEXT,
            runtime INT,
            rating INT,
            keywords TEXT,
            popularity INT,
            release_date DATE
            );
    ''')
    cursor.execute('''
            CREATE INDEX idx_movie_id_md ON movies_data(movie_id);
        ''')

#create genres_table table
def create_genres_table():
    data = pd.read_csv ('merged_tmdb_data.csv')
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE genres_table (
            genres_id INT PRIMARY KEY,
            genre_name VARCHAR(255)
            );
    ''')
    cursor.execute('''
            CREATE INDEX idx_genre_name ON genres_table(genre_name);
        ''')

    
#create movies_genres movie
def create_movie_genres():
    data = pd.read_csv ('merged_tmdb_data.csv')
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE movies_genres (
            movie_id INT,
            genres_id INT,
            FOREIGN KEY (movie_id) REFERENCES movies_data(movie_id),
            FOREIGN KEY (genres_id) REFERENCES genres_table(genres_id)
            )
    ''')
    
#create actors table
def create_actors():
    data = pd.read_csv ('merged_tmdb_data.csv')
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE actors (
            actor_id INT PRIMARY KEY,
            actor_name VARCHAR(255),
            UNIQUE (actor_id)
            );
    ''')
    cursor.execute('''
            CREATE INDEX idx_actor_id ON actors(actor_id);
        ''')


#create movie_actor table
def create_movie_actors():
    data = pd.read_csv ('merged_tmdb_data.csv')
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE movie_actor (
            movie_id INT,
            actor_id INT,
            FOREIGN KEY (movie_id) REFERENCES movies_data(movie_id),
            FOREIGN KEY (actor_id) REFERENCES actors(actor_id)
            )
    ''')
    
    
#create profit table
def create_profit():
    data = pd.read_csv ('merged_tmdb_data.csv')
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE profit (
            movie_id INT PRIMARY KEY,
            production_company VARCHAR(255),
            revenue DECIMAL(18, 2),
            budget DECIMAL(18, 2),
            FOREIGN KEY (movie_id) REFERENCES movies_data(movie_id)
            );
    ''')
    


#create movie_keywords table
def create_keywords_table():
    data = pd.read_csv ('merged_tmdb_data.csv')
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE movie_keywords (
            movie_id INT,
            keyword VARCHAR(255),
            FULLTEXT(keyword),
            FOREIGN KEY (movie_id) REFERENCES movies_data(movie_id));
            );
    ''')
    
    