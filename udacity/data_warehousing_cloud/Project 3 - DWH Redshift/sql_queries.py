import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# var_name = config.get("", "")
log_data_location = config.get("S3", "LOG_DATA")
song_data_location = config.get("S3", "SONG_DATA")
logs_json_format = config.get("S3", "LOG_JSONPATH")
songs_json_format = 'auto'
aws_region = config.get("S3", "REGION")
iam_role = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user_data"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE staging_events (
    "artist"            varchar,
    "auth"              varchar,
    "firstName"         varchar,
    "gender"            varchar,
    "itemInSession"     smallint,
    "lastName"          varchar,
    "length"            decimal,
    "level"             varchar,
    "location"          varchar,
    "method"            varchar,
    "page"              varchar,
    "registration"      float8,
    "sessionId"         int,
    "song"              varchar,
    "status"            smallint,
    "ts"                bigint,
    "userAgent"         varchar,
    "userId"            varchar   
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    "artist_id"             varchar,
    "artist_latitude"       decimal,
    "artist_location"       varchar,
    "artist_longitude"      decimal,
    "artist_name"           varchar,
    "duration"              decimal,
    "num_songs"             integer,
    "song_id"               varchar,
    "title"                 varchar,
    "year"                  smallint
)
""")

songplay_table_create = ("""
CREATE TABLE songplay (
    "songplay_id"   bigint identity(0,1),
    "start_time"    bigint      NOT NULL SORTKEY,
    "user_id"       varchar     NOT NULL,
    "level"         varchar,
    "song_id"       varchar     NOT NULL DISTKEY,
    "artist_id"     varchar     NOT NULL,
    "session_id"    int         NOT NULL,
    "location"      varchar,
    "user_agent"    varchar,
    PRIMARY KEY ("songplay_id"),
    FOREIGN KEY ("start_time")  REFERENCES time("start_time"),
    FOREIGN KEY ("user_id")     REFERENCES user_data("user_id"),
    FOREIGN KEY ("song_id")     REFERENCES song("song_id"),
    FOREIGN KEY ("artist_id")   REFERENCES artist("artist_id")
)
""")

user_table_create = ("""
CREATE TABLE user_data (
    "user_id"       varchar NOT NULL,
    "first_name"    varchar,
    "last_name"     varchar,
    "gender"        varchar,
    "level"         varchar,
    PRIMARY KEY ("user_id")
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE song (
    "song_id"   varchar,
    "title"     varchar,
    "artist_id" varchar,
    "year"      smallint,
    "duration"  decimal,
    PRIMARY KEY ("song_id")
) diststyle all;
""")

artist_table_create = ("""
CREATE TABLE artist(
    "artist_id" varchar,
    "name"      varchar,
    "location"  varchar,
    "latitude"  decimal,
    "longitude" decimal,
    PRIMARY KEY("artist_id")
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE time(
    "start_time"    bigint,
    "hour"          int,
    "day"           int,
    "week"          int,
    "month"         int,
    "year"          int,
    "weekday"       int,
    PRIMARY KEY("start_time")
) diststyle all;
""")

# STAGING TABLES

staging_events_copy = f"""
copy staging_events from {log_data_location} 
credentials 'aws_iam_role={iam_role}'
region {aws_region}
format as json {logs_json_format}
"""

staging_songs_copy = f"""
copy staging_songs from {song_data_location}
credentials 'aws_iam_role={iam_role}'
region {aws_region}
format as json '{songs_json_format}'
"""

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
