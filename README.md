### Project Summary
This is an etl in python that feeds a star schema hosted in amazon redshift with data from JSON files in series format.
The data is from a music streaming app, and contains info such as but not limited to:
songs: song_id, title, artist_id, year, duration
users of the app: user_id, first_name, last_name, gender, level
artists: artist_id, name, location, latitude, longitude
song plays: start_time , user_id , level, song_id, artist_id, session_id, location, user_agent

### how to run the Python scripts
Create a redshift cluster, I advise you choose (us-west-2) for the region same as the S3 bucket, allow the connection and make sure your user has S3 reading privilege.
Fill the dwh.cfg with your own variables.
Run create_tables.py with no params. (loading files takes few minutes normally)
Run etl.py with no params.
### Files in the repository. 

#### sql_queries.py
Keeps the SQL queries out of the main file for readabilty and separation of concern
Note that it also contains special queries to read data from S3 in JSON format and load them into redshift.
#### create_tables.py
As its name suggests creates or replaces redshift tables to fill with data.
it imports the scripts from sql_queries.
Tables are created in order of dependency and some of them are supposed to be distributed through nodes.

#### etl.py
This one supposes that tables are created.
Data is first copied from S3 (json files) to staging tables.
It is then copied to the star schema tables using SQL queries in the form of ( insert select ..) statements
#### dwh.cfg
This is a configuration file for storing account and environnement related variables.
