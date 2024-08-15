README:
Python version: 3.10.4 
Libraries (Have to be imported but are already imported in the files):
	import csv
	import json
	import mysql.connector
	from datetime import date
	In queries_execution :
		import create_db_script
		import api_data_retrieve
		import queries_db_script
		import csv
		import mysql.connector

The Database, Tables, and Queries are already built. 
To remove them - if needed:
- please use the following commands in MySQL workbench: 
	USE sibaazab;
	SET SQL_SAFE_UPDATES = 0; -- Turn off safe mode
	DROP TABLE IF EXISTS actors,genres_table,movie_actor, movie_keywords, movies_genres, profit, movies_data;
- Remove the comments in queries_execution.py and run the program. 

The file queries_execution.py contains example prints run the program to get the result in the output section. 
The file merged_tmdb_data.csv has to be in the same folder as the other Python files. 
