import create_db_script
import api_data_retrieve
import queries_db_script
import csv
import mysql.connector



def connect_to_database():
    conn = mysql.connector.connect(host="localhost", username="sibaazab", password="sib42918", database="sibaazab", port=3305)
    if conn.is_connected():
        #print("Successfully Connected")
        return conn
    print("Connection Failed")
    exit(-1)
    
    
def main():
    #create_db_script.create_database_movies()
    #create_db_script.create_genres_table()
    #create_db_script.create_movie_genres()
    #create_db_script.create_actors()
    #create_db_script.create_movie_actors()
    #create_db_script.create_profit()
    #create_db_script.create_keywords_table()
    
    
    #database = connect_to_database()
    #cursor = database.cursor(buffered=True)
    #api_data_retrieve.insert_movie_data(cursor)
    #api_data_retrieve.insert_profit(cursor)
    #api_data_retrieve.insert_actors(cursor)
    #api_data_retrieve.insert_movie_actor(cursor)
    #api_data_retrieve.insert_genres_table(cursor)
    #api_data_retrieve.insert_movies_genres(cursor)
    #api_data_retrieve.insert_movie_keywords(cursor)
    #database.commit()
    #cursor.close()
    #database.close()
    
    
    print("TOP 10 MOVIES BASED ON RATING : \n")
    print(queries_db_script.query_1())
    print("Click on a movie photo : \n")
    print(queries_db_script.query_2("winnie the pooh"))
    print("Last 10 released movies : \n")
    print(queries_db_script.query_3())
    print("10 most watched movies : \n")
    print(queries_db_script.query_4())
    
    print("Genres : \n")
    print(queries_db_script.query_10())
    print("Top 10 movies in a selected genre :\n")
    print(queries_db_script.query_5("Romance"))
    print("Top 10 actors in a selected genre : \n")
    print(queries_db_script.query_6("Romance"))
    
    print("10 most profitable movies : \n")
    print(queries_db_script.query_7())
    print("10 most profitable production companies: \n")
    print(queries_db_script.query_8())
    
    print("Search by a keyword:\n")
    print(queries_db_script.query_9("galaxy"))
    print(queries_db_script.query_9("gal"))
    

main()