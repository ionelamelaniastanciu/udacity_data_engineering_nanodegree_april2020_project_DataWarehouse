# Project Data Warehouse 

### About
Sparkify is a music streaming startup. Because the interest was growing, they want to move all data onto the cloud. For this project, it's using AWS (Amazon Web Services), which offer data manipulation using different types of environmets already build-in. Files will be manipulated with Python and data insert into tables using PostgreSQL.

### Project Files
- dwh.cg = contains details for log in the cloud
- create_table.py = creates fact and dimension tables for the star schema in Redshift.
- etl.py = load data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.
- sql_queries.py = defines SQL statements, which will be imported into the two other files above.

### Fact tables
1. staging_songs
song_id, num_songs, title, artist_name, artist_latitude, year, duration, artist_id, artist_longitude, artist_location

2. staging_events
artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId

### Tables
1. songplays 
| start_time | user_id | level | song_id | artist_id | session_id | location | user_agent |
2. users = users in the app

| user_id | first_name | last_name | gender | level|

3. songs = songs in music database

| song_id | title | artist_id | year | duration |

4. artists = artists in music database

| artist_id | name | location | lattitude | longitude |

5. time = timestamps of records in songplays broken down into specific units
| start_time | hour | day | week | month | year | weekday |

## ETL
The data is stored as json files in a S3 bucket. 
The method `load_staging_tables` takes the data from the bucket and load into 2 tables staging_song and staging_events on the database created by user/tester.
Using `insert_tables` method, the data from the twos tables is manipulated in 5 tables.

### How to run
This is an example of testing. Cluster is used with public credentials.
1) Create a IAM Role with administrator database rights
2) Create a group security with Custom TCP Rule, Protocp TCP, Source 0.0.0.0/0
3) Create a cluster using that group security and a masteruser
4) In the dwh.cfg put the details from cluster
5) In terminal:
` python3 sql_queries.py`
` python3 create_tables`
` python3 etl.py`