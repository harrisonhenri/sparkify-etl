import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")
ROLE_ARN = config.get("IAM", "ROLE_ARN")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_DATA = config.get("S3", "LOG_DATA")
JSON_SCHEMA = config.get("S3", "LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS song_plays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    artist_name VARCHAR,
    user_auth VARCHAR,
    user_first_name VARCHAR,
    user_gender CHAR(1),
    item_in_session INTEGER NOT NULL,
    user_last_name VARCHAR,
    length FLOAT,
    user_level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    session_id INTEGER,
    song_name VARCHAR,
    status INTEGER,
    ts BIGINT NOT NULL,
    user_agent VARCHAR,
    user_id INTEGER);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER NOT NULL,
    artist_id VARCHAR NOT NULL,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location TEXT,
    artist_name VARCHAR NOT NULL,
    song_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    title VARCHAR NOT NULL,
    duration FLOAT NOT NULL,
    year INTEGER NOT NULL);
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS song_plays (
    song_play_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER,
    level VARCHAR,
    song_id INTEGER,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id INTEGER PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INTEGER NOT NULL,
    duration FLOAT NOT NULL);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS times (
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday VARCHAR);
"""

# STAGING TABLES

staging_events_copy = (
    """
copy staging_events from '{}'
credentials 'aws_iam_role={}'
format as json '{}' 
region 'us-west-2';
"""
).format(LOG_DATA, ROLE_ARN, JSON_SCHEMA)

staging_songs_copy = (
    """
copy staging_songs from '{}'
credentials 'aws_iam_role={}'
format as json 'auto' 
region 'us-west-2';
"""
).format(SONG_DATA, ROLE_ARN)

# FINAL TABLES

songplay_table_insert = """
INSERT INTO song_plays (
        start_time, 
        user_id, 
        level,
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent
    ) 
    SELECT 
        timestamp 'epoch' + se.ts/1000 * interval '1 second', 
        se.user_id, 
        se.user_level, 
        ss.song_id, 
        ss.artist_id, 
        se.session_id, 
        se.location, 
        se.user_agent
    FROM staging_events se 
    JOIN staging_songs ss ON (se.song_name = ss.title AND se.artist_name = ss.artist_name)
    WHERE se.page = 'NextSong';
"""

user_table_insert = """
INSERT INTO users (
        user_id, 
        first_name,
        last_name,
        gender,
        level
    ) 
    SELECT 
        DISTINCT user_id, 
        user_first_name,
        user_last_name,
        user_gender,
        user_level
    FROM staging_events 
    WHERE page = 'NextSong' AND user_id IS NOT NULL;
"""

song_table_insert = """
INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    ) 
    SELECT 
        DISTINCT song_id, 
        title,
        artist_id,
        year,
        duration
    FROM staging_songs;
"""

artist_table_insert = """
INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    ) 
    SELECT 
        DISTINCT artist_id, 
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs;
"""

time_table_insert = """
INSERT INTO times (
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday
    )
    SELECT 
        DISTINCT start_time, 
        EXTRACT(hour from start_time), 
        EXTRACT(day from start_time), 
        EXTRACT(week from start_time), 
        EXTRACT(month from start_time), 
        EXTRACT(year from start_time), 
        EXTRACT(weekday from start_time)
    FROM song_plays
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
