# DROP TABLES

songplay_table_drop="DROP TABLE IF EXISTS songplays"
user_table_drop="DROP TABLE IF EXISTS users"
song_table_drop="DROP TABLE IF EXISTS songs"
artist_table_drop="DROP TABLE IF EXISTS artists"
time_table_drop="DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(timestamp varchar(255), user_id int, level varchar(255), song_id varchar(255), artist_id varchar(255), session_id varchar(255), location varchar(255), user_agent varchar(255))""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id int, first_name varchar(255), last_name varchar(255), gender varchar(255), level varchar(255) )""")

song_table_create=("""CREATE TABLE IF NOT EXISTS songs(song_id varchar(255), title varchar(255), artist_id varchar(255), year int, duration real)""")

artist_table_create=("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar(255), name varchar(255), location varchar(255), latitude real, longitude real)""")

time_table_create=("""CREATE TABLE IF NOT EXISTS time(timestamp varchar(255), hour int, day int, week_of_year int, month int, year int)""")

# INSERT RECORDS

songplay_table_insert=("""INSERT INTO songplays(timestamp, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""")

user_table_insert=("""INSERT INTO users(user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s)""")

song_table_insert=("""INSERT INTO songs(song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)""")

artist_table_insert=("""INSERT INTO artists(artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s)""")

time_table_insert=("""INSERT INTO time(timestamp, hour, day, week_of_year, month, year) VALUES (%s, %s, %s, %s, %s, %s)""")

# FIND SONGS

song_select="""SELECT songs.song_id, artists.artist_id FROM artists JOIN songs ON artists.artist_id = songs.artist_id WHERE songs.title = %s AND artists.name = %s AND round(cast(songs.duration as numeric),3) = %s"""

# QUERY LISTS

create_table_queries=[
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]

drop_table_queries=[
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

