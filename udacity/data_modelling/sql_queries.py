# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS Songplays(
    songplay_id serial PRIMARY KEY,
    start_time bigint,
    user_id int,
    level text,
    song_id text,
    artist_id text,
    session_id int,
    location text,
    user_agent text
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS Users(
    user_id int PRIMARY KEY,
    first_name text,
    last_name text,
    gender text,
    level text
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS Songs(
    song_id text PRIMARY KEY,
    title text,
    artist_id text,
    year int,
    duration float
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS Artists(
    artist_id text PRIMARY KEY,
    name text,
    location text,
    latitude float,
    longitude float
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS Time(
    start_time bigint PRIMARY KEY,
    hour bigint,
    day bigint,
    week bigint,
    month bigint,
    year bigint,
    weekday bigint
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO Songplays(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent)
VALUES(
    %(startTime)s,
    %(userId)s,
    %(level)s,
    %(songId)s,
    %(artistId)s,
    %(sessionId)s,
    %(location)s,
    %(userAgent)s)
""")

user_table_insert = ("""
INSERT INTO Users(user_id,
    first_name,
    last_name,
    gender,
    level)
VALUES(
    %(userId)s,
    %(firstName)s,
    %(lastName)s,
    %(gender)s,
    %(level)s)
ON CONFLICT(user_id)
DO UPDATE
SET first_name = %(firstName)s,
    last_name  = %(lastName)s,
    gender     = %(gender)s,
    level      = %(level)s
""")

song_table_insert = ("""
INSERT INTO Songs(song_id,
    title,
    artist_id,
    year,
    duration)
VALUES(
%(songId)s,
%(title)s,
%(artistId)s,
%(year)s,
%(duration)s)
ON CONFLICT (song_id)
do nothing
""")

artist_table_insert = ("""
INSERT INTO Artists(artist_id,
    name,
    location,
    latitude,
    longitude)
VALUES(
%(artistId)s,
%(name)s,
%(location)s,
%(latitude)s,
%(longitude)s)
ON CONFLICT(artist_id)
DO UPDATE
SET name = %(name)s,
    location  = %(location)s,
    latitude  = %(latitude)s,
    longitude = %(longitude)s
""")


time_table_insert = ("""
INSERT INTO Time(start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
VALUES(
%(startTime)s,
%(hour)s,
%(day)s,
%(week)s,
%(month)s,
%(year)s,
%(weekday)s
)
ON CONFLICT(start_time)
do nothing
""")

# FIND SONGS

song_select = ("""
    SELECT s.song_id, a.artist_id
    FROM songs AS s INNER JOIN artists AS a
    ON s.artist_id = a.artist_id
    WHERE s.title = %(title)s
        AND a.name = %(artistName)s
        AND s.duration = %(duration)s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]