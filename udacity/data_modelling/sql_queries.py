# DROP TABLES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""
create table if not exists Songplays(
    songplay_id serial primary key,
    start_time bigint,
    user_id int not null,
    level text,
    song_id text not null,
    artist_id text not null,
    session_id int,
    location text,
    user_agent text
)
""")

user_table_create = ("""
create table if not exists Users(
    user_id int primary key,
    first_name text,
    last_name text,
    gender text,
    level text
)
""")

song_table_create = ("""
create table if not exists Songs(
    song_id text primary key,
    title text,
    artist_id text,
    year int,
    duration float
)
""")

artist_table_create = ("""
create table if not exists Artists(
    artist_id text primary key,
    name text,
    location text,
    latitude float,
    longitude float
)
""")

time_table_create = ("""
create table if not exists Time(
    start_time bigint primary key,
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
insert into Songplays(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent)
values(
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
insert into Users(user_id,
    first_name,
    last_name,
    gender,
    level)
values(
    %(userId)s,
    %(firstName)s,
    %(lastName)s,
    %(gender)s,
    %(level)s)
on conflict(user_id)
do update
set first_name = %(firstName)s,
    last_name  = %(lastName)s,
    gender     = %(gender)s,
    level      = %(level)s
""")

song_table_insert = ("""
insert into Songs(song_id,
    title,
    artist_id,
    year,
    duration)
values(
%(song_id)s,
%(title)s,
%(artist_id)s,
%(year)s,
%(duration)s)
on conflict (song_id)
do nothing
""")

artist_table_insert = ("""
insert into Artists(artist_id,
    name,
    location,
    latitude,
    longitude)
values(
%(artistId)s,
%(name)s,
%(location)s,
%(latitude)s,
%(longitude)s)
on conflict(artist_id)
do update
set name = %(name)s,
    location  = %(location)s,
    latitude  = %(latitude)s,
    longitude = %(longitude)s
""")


time_table_insert = ("""
insert into Time(start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
values(
%(startTime)s,
%(hour)s,
%(day)s,
%(week)s,
%(month)s,
%(year)s,
%(weekday)s
)
on conflict(start_time)
do nothing
""")

# FIND SONGS

song_select = ("""
    select s.song_id, a.artist_id
    from songs as s inner join artists as a
    on s.artist_id = a.artist_id
    where s.title = %(title)s
        and a.name = %(artistName)s
        and s.duration = %(duration)s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]