import configparser


# LOADING CONFIGURATION VARIABLES
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA= config.get('S3','LOG_DATA')
LOG_JSONPATH= config.get('S3','LOG_JSONPATH')
SONG_DATA= config.get('S3','SONG_DATA')

DWH_ROLE_ARN= config.get('IAM_ROLE','ARN')

# DROP TABLES -

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events "
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs "
songplay_table_drop = "DROP TABLE IF EXISTS  songplays "
user_table_drop = "DROP TABLE IF EXISTS users "
song_table_drop = "DROP TABLE IF EXISTS songs "
artist_table_drop = "DROP TABLE IF EXISTS artists "
time_table_drop = "DROP TABLE IF EXISTS time "
# CREATE TABLES

# Staging tables are meant to temporarly hold the data in the raw files, 
# so types are not fully optimized and they are expected to take a relatively significant space on disk, 
# but once they are in the database, we have more room for optimization and running stats 
# to figure out precise types and sizes.
staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events( 
artist text,
auth varchar(100),
firstName varchar(100),
gender char(1),
itemInSession int,
lastName varchar(100),
length float8,
level varchar(20),
location text,
method varchar(5),
page varchar(50),
registration bigint,
sessionId int,
song text,
status int,
ts bigint,
userAgent text,
userId bigint)
""")

staging_songs_table_create = ("""create table staging_songs(num_songs int, 
       artist_id varchar(100), 
       artist_latitude float8 , 
       artist_longitude float8,
       artist_location text, 
       artist_name text, 
       song_id text, 
       title text, 
       duration decimal(10,0) ,
       year float8)
""")

# The star schema tables are going to hold the data in a form more suitable for analytics, 
# usual postgres check constraints were dropped because redshift doesn't support them, 
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays 
                             (songplay_id bigint identity(0,1), 
                             start_time timestamp not null sortkey, 
                             user_id VARCHAR(50) not null , 
                             level VARCHAR(10), 
                             song_id  VARCHAR(50)  , 
                             artist_id VARCHAR(30) , 
                             session_id int, 
                             location text, 
                             user_agent text);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
                       (user_id VARCHAR(50) , 
                       first_name  VARCHAR, 
                       last_name  VARCHAR, 
                       gender char(1), 
                       level VARCHAR(10)) DISTSTYLE ALL ; """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                     song_id VARCHAR(30), 
                     title text not null, 
                     artist_id text, 
                     year INT, 
                     duration float8 )
                     """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR(30) , 
                       name VARCHAR not null, 
                       location VARCHAR, 
                       latitude FLOAT8, 
                       longitude FLOAT8)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp primary key sortkey, 
                     hour INT not null , 
                     day INT not null , 
                     week INT not null , 
                     month INT not null , 
                     year INT not null, 
                     weekday INT not null )""")


#loading the data into STAGING TABLES from the S3 bucket
#the events have a provided path file to point to the columns we are using
staging_events_copy = ("""
    copy staging_events from '{}'
    credentials 'aws_iam_role={}'
    json '{}'
    region 'us-east-1';
""").format(LOG_DATA,DWH_ROLE_ARN,LOG_JSONPATH)

# the songs are in the Json like format that redshift expects, and columns correspond exactly,
# so auto is our option we also ignore the case as it has no importance here. 
staging_songs_copy = ("""
    copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    json 'auto ignorecase'
    region 'us-east-1';
""").format(SONG_DATA,DWH_ROLE_ARN)

# Final tables inserts from staging tables
songplay_table_insert = ("""INSERT INTO songplays
( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
'1970-01-01'::date + ts/1000 * interval '1 second' AS start_time, 
events.userId, 
events.level, 
songs.song_id, 
songs.artist_id, 
events.sessionId, 
events.location, 
events.userAgent
         
    FROM staging_events events
    LEFT JOIN staging_songs songs
      ON events.song ilike songs.title 
      AND events.artist ilike songs.artist_name 
WHERE page='NextSong'
""")

user_table_insert = ("""INSERT INTO users
(user_id, first_name, last_name, gender, level) 
SELECT distinct userId, firstName, lastName, gender, level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""INSERT INTO songs
(song_id, title, artist_id, year, duration)
SELECT distinct song_id, title, artist_id, year, duration
FROM staging_songs
""")


artist_table_insert = ("""INSERT INTO artists
(artist_id, name, location, latitude, longitude)
SELECT distinct artist_id, artist_name,artist_location, artist_latitude, artist_longitude
FROM staging_songs

""")


time_table_insert = ("""INSERT INTO time
(start_time, hour, day, week, month, year, weekday)
SELECT 
distinct '1970-01-01'::date + ts/1000 * interval '1 second', 
EXTRACT(HOUR FROM '1970-01-01'::date + ts/1000 * interval '1 second'), 
EXTRACT(DAY FROM '1970-01-01'::date + ts/1000 * interval '1 second'),
EXTRACT(WEEK FROM '1970-01-01'::date + ts/1000 * interval '1 second'),
EXTRACT(MONTH FROM '1970-01-01'::date + ts/1000 * interval '1 second'),
EXTRACT(YEAR FROM '1970-01-01'::date + ts/1000 * interval '1 second'),
EXTRACT(DOW FROM '1970-01-01'::date + ts/1000 * interval '1 second')
FROM staging_events where page = 'NextSong'
""")

songplay_table_truncate="truncate table songplays"
user_table_truncate="truncate table users"
song_table_truncate="truncate table songs"
artist_table_truncate="truncate table artists"
time_table_truncate="truncate table time"


# QUERY LISTS - to facilitate iteration over tables

create_table_queries = [staging_events_table_create, staging_songs_table_create,song_table_create, 
                        artist_table_create, time_table_create, user_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, 
                      user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, 
                        artist_table_insert, time_table_insert]

#QUERY LISTS COMPLEMENT -FOR DEBUGGING and avoiding to wait for staging tables to copy each time we run.
truncate_final_table_queries = [songplay_table_truncate, user_table_truncate, song_table_truncate, 
                                artist_table_truncate, time_table_truncate]
drop_final_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, 
                            artist_table_drop, time_table_drop]
create_final_table_queries = [song_table_create, artist_table_create,
                              time_table_create, user_table_create, songplay_table_create] 