import configparser
import boto3

def get_ARN(config):
    """
    get ARN
    param:
    * config file info
    """
    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')
    DWH_CLUSTER_IDENTIFIER = config.get("INFO","DWH_CLUSTER_IDENTIFIER")
    redshift = boto3.client('redshift',
                     region_name='us-east-1',
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)
    response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
    return response['Clusters'][0]['IamRoles'][0]['IamRoleArn']
# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
# get iam role arn
ARN_ROLE = get_ARN(config)
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')
# DROP TABLES
staging_events_table_drop = "drop table if exists event_staging"
staging_songs_table_drop = "drop table if exists song_staging"
songplay_table_drop = "drop table if exists songplays_table"
user_table_drop = "drop table if exists users_table"
song_table_drop = "drop table if exists songs_table"
artist_table_drop = "drop table if exists artists_table"
time_table_drop = "drop table if exists time_table"

# CREATE TABLES
# staging 2 table
staging_events_table_create= ("""
    create table if not exists events_staging (
        artist varchar,
        auth varchar not null,
        firstName varchar,
        gender char (1),
        itemInSession int not null,
        lastName varchar,
        length numeric,
        level varchar not null,
        location varchar,
        method varchar not null,
        page varchar not null,
        registration numeric,
        sessionId int not null,
        song varchar,
        status int not null,
        ts numeric not null,
        userAgent varchar,
        userId int
    )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs_staging (
                            num_songs int not null,
                            artist_id char (18) not null,
                            artist_latitude varchar,
                            artist_longitude varchar,
                            artist_location varchar,
                            artist_name varchar not null,
                            song_id char (18) not null,
                            title varchar not null,
                            duration numeric not null,
                            year int not null);
""")
# datawarehouse
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays_table(songplay_id int identity(0, 1) primary key, 
                            start_time timestamp not null, 
                            user_id int not null, 
                            level varchar, 
                            song_id varchar(18) not null, 
                            artist_id varchar not null, 
                            session_id int not null, 
                            location varchar, 
                            user_agent varchar(150)
                            );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users_table(user_id int primary key, 
                            first_name varchar, 
                            last_name varchar, 
                            gender char(1), 
                            level varchar);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs_table(song_id varchar(18) primary key, 
                            title varchar not null, 
                            artist_id char(18) not null, 
                            year int, 
                            duration numeric not null);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists_table(artist_id varchar PRIMARY KEY, 
                                name varchar not null, 
                                location varchar, 
                                latitude float, 
                                longitude float);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table (start_time timestamp PRIMARY KEY,
                                hour int,
                                day int,
                                week int,
                                month int,
                                year int,
                                weekday int);
""")

# STAGING TABLES
# move s3 to redshift
staging_events_copy = ("""
    copy events_staging from {}
    iam_role '{}'
    format as json {}
    region 'us-west-2';
""").format(
    LOG_DATA,
    ARN_ROLE,
    LOG_JSONPATH
)

staging_songs_copy = ("""
    copy songs_staging from {}
    iam_role '{}'
    json 'auto'
    region 'us-west-2';
""").format(SONG_DATA, ARN_ROLE)


# INSERT RECORDS

songplay_table_insert = ("""
    insert into songplays_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select
        timestamp 'epoch' + es.ts / 1000 * interval '1 second' as start_time,
        es.userId as user_id,
        es.level,
        s.song_id,
        s.artist_id,
        es.sessionId as session_id,
        es.location,
        es.userAgent as user_agent
    from events_staging es
    join songs_staging s on es.song = s.title and es.artist = s.artist_name
    where es.page = 'NextSong'
""")

user_table_insert = ("""
    insert into users_table
    select eo.userId, eo.firstName, eo.lastName, eo.gender, eo.level
    from events_staging eo
    join (
        select max(ts) as ts, userId
        from events_staging
        where page = 'NextSong'
        group by userId
    ) ei on eo.userId = ei.userId and eo.ts = ei.ts
""")

song_table_insert = ("""
    insert into songs_table
    select
        song_id,
        title,
        artist_id,
        year,
        duration
    from songs_staging
""")

artist_table_insert = ("""
    insert into artists_table
    select distinct
        artist_id,
        artist_name as name,
        artist_location as location,
        cast(artist_latitude as float) as latitude,
        cast(artist_longitude as float) as longitude
    from songs_staging
""")

time_table_insert = ("""
    insert into time_table
    select
        ti.start_time,
        extract(hour from ti.start_time) as hour,
        extract(day from ti.start_time) as day,
        extract(week from ti.start_time) as week,
        extract(month from ti.start_time) as month,
        extract(year from ti.start_time) as year,
        extract(weekday from ti.start_time) as weekday
    from (
        select distinct
            timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time
        from events_staging
        where page = 'NextSong'
    ) ti
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
