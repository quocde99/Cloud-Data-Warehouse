# Project 3 - Udacity 
## Overview 
This project provide solution  is process loading data from S3(AWS) to staging at AWS Redshift and we load to data model at AWS Redshift.
Technologies: AWS(S3,IAM,Redshift),Python
## Quick start
Make sure your aws account and have permision S3,EC2,IAM,Redshift.
Modify KEY and SECRET in dwh.cfg 
KEY=<your_key>
SECRET=<your_secret>
Access AWS and to do in AWS following(or following step by step in test_evn.ipynb):
- create IAM role 
- get ARN
- create and run Redshift cluster
- run cmd python create_tables.py
- run cmd python etl.py
- delete redshift (if you don't use it anymore)
## Project structure
1. create_tables.py drops and creates tables(staging and start schema).
2. etl.py reads and processes data from S3 is staging at Redshift and finally insert into data model in Redshift.
3. sql_queries.py contains sql query.
4. test_evn.ipynb this notebook contains detailed instructions on the ETL process for each of the tables.
5. dwh.cfg infomation AWS account and config size, cluster type of Redshift.
## Schema 
### Fact Table
- songplays(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent).
### Dimension Table
- users(user_id, first_name, last_name, gender, level).
- songs(song_id, title, artist_id, year, duration).
- artists(artist_id, name, location, latitude, longitude).
- time(start_time, hour, day, week, month, year, weekday).
