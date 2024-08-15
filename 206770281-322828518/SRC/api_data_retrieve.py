
import csv
import json
import mysql.connector
from datetime import datetime


def connect_to_database():
    conn = mysql.connector.connect(host="localhost", username="sibaazab", password="sib42918", database="sibaazab", port=3305)
    if conn.is_connected():
        print("Successfully Connected")
        return conn
    print("Connection Failed")
    exit(-1)
    
def insert_movie_data(cursor):
    try :
        with open('merged_tmdb_data.csv', 'r', encoding=('utf-8')) as file:
            csv_reader = csv.reader(file)
            next(csv_reader)
            for row in csv_reader:
                movie_keywords = []
                cnt = 0
                row = [None if value == '' else value for value in row]
                if None in row:
                    continue
                keyword_json = row[7]
                keyword_data = json.loads(keyword_json)
                for keywords in keyword_data:
                    cnt = cnt +1
                    keyword = keywords['name']
                    movie_keywords.append(keyword)
                    if(cnt==10):
                        break
                date_time = datetime.strptime(row[14], '%d/%m/%Y')
                cursor.execute("""
                            INSERT INTO movies_data(movie_id, title_x, overview, runtime, rating, keywords, popularity, release_date) VALUES(%s, %s, %s, %s, %s,%s,%s,%s);""",
                            (int(row[0]), row[1], row[10], int(row[16]), float(row[21]), json.dumps(movie_keywords), float(row[11]),date_time)) #KEYWORDS-THE DREAM

        print("MOVIE_DATA INSERTED SUCCESSFULLY")
    except Exception as e:
        print("ERROR IN MOVIE_DATA", e)
        
        
def insert_profit(cursor):
    try:
        with open('merged_tmdb_data.csv', 'r', encoding=('utf-8')) as file:
            csv_reader = csv.reader(file)
            next(csv_reader)
            for row in csv_reader:
                row = [None if value == '' else value for value in row]
                movie_id = int(row[0])
                production_company = row[23]
                revenue = row[25].replace(',', '').strip()
                budget = row[24].replace(',', '').strip()
                if revenue == '-' or budget == '-' or None in row:
                    continue
                cursor.execute("""
                            INSERT INTO profit(movie_id, production_company, revenue, budget) VALUES(%s, %s, %s, %s);""",
                            (movie_id, production_company, float(revenue), float(budget)))
                
        print("PROFIT INSERTED SUCCESSFULLY")
    except Exception as e:
        print("ERROR IN PROFIT", e)

def create_actor_data():
    with open('merged_tmdb_data.csv', 'r', encoding=('utf-8')) as file:
            csv_reader = csv.reader(file)
            next(csv_reader)
            unique_ids = []
            id_name_pairs = {}
            acted_in_dict = {}
            for row in csv_reader:
                row = [None if value == '' else value for value in row]
                if None in row:
                    continue
                cast_json = row[2]
                acted_in = row[0]
                cast_data = json.loads(cast_json)
                for cast in cast_data:
                    id = cast['id']
                    name = cast['name']
                    if id not in unique_ids:
                        unique_ids.append(id)
                        id_name_pairs[id] = name
                        acted_in_dict[id] = []
                    acted_in_dict[id].append(acted_in)
    return (id_name_pairs,acted_in_dict)


def insert_actors(cursor):
    try:
        pairs = create_actor_data()[0]
        for key,value in pairs.items():
            cursor.execute("""
                            INSERT INTO actors(actor_id, actor_name) VALUES(%s, %s);""",
                            (key, value))
        print("ACTORS INSERTED SUCCESSFULLY")
    except Exception as e:
        print("ERROR IN ACTORS", e)

def insert_movie_actor(cursor):
    try:
        pairs = create_actor_data()[1]
        for key,value in pairs.items():
            for movie in value:
                cursor.execute("""
                                INSERT INTO movie_actor(movie_id, actor_id) VALUES(%s, %s);""",
                                (movie, key))
        print("MOVIE_ACTORS INSERTED SUCCESSFULLY")
    except Exception as e:
        print("ERROR IN MOVIE_ACTORS", e)



def create_genre_data():
    with open('merged_tmdb_data.csv', 'r', encoding=('utf-8')) as file:
            csv_reader = csv.reader(file)
            next(csv_reader)
            unique_ids = []
            id_name_pairs = {}
            genre_dict = {}
            for row in csv_reader:
                row = [None if value == '' else value for value in row]
                if None in row:
                    continue
                genre_json = row[5]
                movie_id = row[0]
                genre_data = json.loads(genre_json)

                for genre in genre_data:
                    id = genre['id']
                    name = genre['name']
                    if id not in unique_ids:
                        unique_ids.append(id)
                        id_name_pairs[id] = name
                        genre_dict[id] = []
                    genre_dict[id].append(movie_id)

    return (id_name_pairs,genre_dict)


def insert_genres_table(cursor):
    try:
        pairs = create_genre_data()[0]
        for key, value in pairs.items():
            cursor.execute("""
                            INSERT INTO genres_table(genres_id, genre_name) VALUES(%s, %s);""",
                            (key, value))
        print("GENRES_TABLE INSERTED SUCCESSFULLY")
    except Exception as e:
        print("ERROR IN GENRES_TABLE", e)

def insert_movies_genres(cursor):
    try:
        pairs = create_genre_data()[1]
        for key,value in pairs.items():
            for movie in value:
                cursor.execute("""
                                INSERT INTO movies_genres(movie_id, genres_id) VALUES(%s, %s);""",
                                (movie, key))
        print("MOVIES_GENRES INSERTED SUCCESSFULLY")
    except Exception as e:
        print("ERROR IN MOVIES_GENRES", e)



def insert_movie_keywords(cursor):
    try :
        with open('merged_tmdb_data.csv', 'r', encoding=('utf-8')) as file:
            csv_reader = csv.reader(file)
            next(csv_reader)
            for row in csv_reader:
                movie_keywords = []
                cnt = 0
                row = [None if value == '' else value for value in row]
                if None in row:
                    continue
                keyword_json = row[7]
                keyword_data = json.loads(keyword_json)
                for keywords in keyword_data:
                    cnt = cnt +1
                    keyword = keywords['name']
                    movie_keywords.append(keyword)
                    if(cnt==10):
                        break
                    cursor.execute("""INSERT INTO movie_keywords(movie_id, keyword) VALUES(%s, %s);""",
                                (int(row[0]),keyword))  # KEYWORDS-THE DREAM

        print("MOVIE_KEYWORDS INSERTED SUCCESSFULLY")
    except Exception as e:
        print("ERROR IN MOVIE_KEYWORDS", e)





