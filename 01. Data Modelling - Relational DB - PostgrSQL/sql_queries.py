#---------------------------- DROP TABLES SQL QUERIES ----------------------------
# The following SQL queries drop all the tables from sparkifydb
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

#---------------------------- CREATE TABLES SQL QUERIES ----------------------------
# The following SQL queries create all the tables from sparkifydb

# FACT tables
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
        songplay_id SERIAL PRIMARY KEY NOT NULL, 
        start_time timestamp NOT NULL,
        user_id int NOT NULL,
        level varchar,
        artist_id varchar NOT NULL,
        song_id varchar NOT NULL,
        session_id int,
        location text,
        user_agent text)""")

# DIMENION tables
user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                        user_id int PRIMARY KEY NOT NULL,
                        first_name varchar NOT NULL,
                        last_name varchar NOT NULL,
                        gender char,
                        level varchar)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id varchar PRIMARY KEY NOT NULL,
                        title varchar,
                        artist_id varchar,
                        year int,
                        duration float)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                        artist_id varchar PRIMARY KEY NOT NULL,
                        name varchar,
                        location varchar,
                        lattitude numeric,
                        longitude numeric)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                        start_time timestamp PRIMARY KEY NOT NULL,
                        hour int,
                        day int,
                        week int,
                        month int,
                        year int,
                        weekday varchar)""")

#---------------------------- LIST OF CREATE AND DROP QUERIES ----------------------------
# The following list can be executed in bulk to drop all the tables or create all the tables 
# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

#---------------------------- INSERT SQL QUERIES ----------------------------
# The following SQL queries list insert rows to the sparkify tables 
# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays( 
                            start_time,
                            user_id,
                            level,
                            artist_id,
                            song_id,
                            session_id,
                            location,
                            user_agent)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""")

user_table_insert = ("""INSERT INTO users(
                        user_id,
                        first_name,
                        last_name,
                        gender,
                        level)
                        VALUES(%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id) 
                        DO UPDATE SET level = excluded.level""")

song_table_insert = ("""INSERT INTO songs(
                        song_id,
                        title, 
                        artist_id,
                        year,
                        duration)
                        VALUES(%s, %s, %s, %s, %s)
                        ON CONFLICT (song_id) 
                        DO NOTHING""")
#To prevent duplicates use "ON CONFLICT (song_id) DO NOTHING" 

artist_table_insert = ("""INSERT INTO artists(
                            artist_id, 
                            name,location,
                            lattitude, 
                            longitude)
                          VALUES(%s, %s, %s, %s, %s)
                          ON CONFLICT (artist_id) 
                          DO NOTHING""")
#To prevent duplicates use "ON CONFLICT (artist_id) DO NOTHING" 

time_table_insert = ("""INSERT INTO time(
                        start_time,
                        hour,
                        day,
                        week,
                        month, 
                        year,
                        weekday)
                        VALUES(%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (start_time) 
                        DO NOTHING""")

#---------------------------- ETL PROCESS: GET ARTIST AND SONG ID ----------------------------
# The following SQL query gets the artist_id and song_id from the artists and songs tables in order to create the songplays table
# FIND SONGS

song_select = ("""SELECT 
               songs.song_id,
               artists.artist_id 
               FROM songs
               JOIN artists ON songs.artist_id=artists.artist_id
               WHERE songs.title=%s AND artists.name=%s AND songs.duration=%s;
                  """)
#In Postgresql, use %s as a string operator for incoming variables
#e.g. INSERT ...VALUES (%s, %s) or SELECT ...WHERE s.name = %s AND a.artist_id = %s AND s.length = %s

