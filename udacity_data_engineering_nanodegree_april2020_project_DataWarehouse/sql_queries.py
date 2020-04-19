import configparser


""" Configuration """

config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN_ROLE = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_PATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')


""" Drop tables"""

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

""" Creates tables"""

staging_events_table_create= ("""
create table staging_events(
    artist text,
    auth text,
    firstName text,    
    gender varchar(1),
    itemInSession int,
    lastName text,    
    length numeric,
    level text,
    location text,
    method text,
    page text,
    registration text,
    sessionId int,
    song text,
    status int,
    ts bigint,
    userAgent text,
    userId text
)
""")

staging_songs_table_create = ("""
create table staging_songs(
    song_id text,
    num_songs int,
    title text,
    artist_name text,
    artist_latitude text,
    year int,
    duration numeric,
    artist_id text,
    artist_longitude text,    
    artist_location text
)
""")

songplay_table_create = ("""
create table songplays(
    songplay_id integer identity(0,1) primary key,
    start_time timestamp not null,
    user_id text not null,
    level text,
    song_id text not null,
    artist_id text not null,
    session_id int,
    location text,
    user_agent text
)
""")

user_table_create = ("""
create table users(
    user_id text primary key,
    first_name text,
    last_name text, 
    gender varchar(1),
    level text
)
""")

song_table_create = ("""
create table songs(
    song_id text primary key, 
    title text, 
    artist_id text,
    year int, 
    duration float
) 
""")

artist_table_create = ("""
create table artists(
    artist_id text primary key, 
    name text, 
    location  text, 
    latitude numeric,
    longitude numeric
)
""")

time_table_create = ("""
create table time(
    start_time timestamp primary key,
    hour int, 
    day int,
    week int, 
    month int,
    year int,
    weekday int
)
""")

""" STAGING TABLES """

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    timeformat as 'epochmillisecs'
    format as json {};
""").format(LOG_DATA,ARN_ROLE,LOG_PATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json 'auto';
""").format(SONG_DATA, ARN_ROLE)

""" FINAL TABLES """

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select timestamp 'epoch' + se.ts/1000*interval '1 second' as start_time,
        se.userId as user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId as session_id,
        ss.artist_location,
        se.userAgent as user_agent
from staging_events se
join staging_songs ss
on se.artist = ss.artist_name
where se.page='NextSong' and start_time is not null and user_id is not null and song_id is not null and artist_id is not null and session_id is not null
""")

user_table_insert = ("""
insert into users(user_id, first_name, last_name, gender, level)
    select userId as user_id, 
    firstName as first_name,
    lastName as last_name,
    gender, 
    level
    from staging_events
    where user_id is not null
""")

song_table_insert = ("""
insert into songs(song_id, title, artist_id, year, duration)
   select song_id, title, artist_id, year, duration
   from staging_songs
   where song_id is not null and artist_id is not null
""")

artist_table_insert = ("""
insert into artists(artist_id, name, location, latitude, longitude)
    select artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    from staging_songs
    where artist_id is not null
""")

time_table_insert = ("""
insert into time(start_time, hour, day, week, month, year, weekday)
select start_time,
        extract (hour from start_time),
        extract (day from start_time),
        extract (week from start_time),
        extract (month from start_time),
        extract (year from start_time),
        extract (weekday from start_time)
from songplays where start_time is not null
""")

""" QUERY LISTS """

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
