### Project Summary
This is an etl in python that generates a star schema in form of parquet files, hosted in S3 or locally with data from JSON files in series format.
The data is from a music streaming app (sparify dataset), and contains info such as but not limited to:
songs: song_id, title, artist_id, year, duration
users of the app: user_id, first_name, last_name, gender, level
artists: artist_id, name, location, latitude, longitude
song plays: start_time , user_id , level, song_id, artist_id, session_id, location, user_agent

### how to run the Python scripts
Create an EMR cluster or use your own spark environnement, I advise you choose the region same as the S3 bucket, allow the connection and make sure your user has S3 reading privilege.
Fill the dl.cfg with your own credentials.
Run etl.py with no params.
#### etl.py
This .
Data is first copied from S3 (json files) to staging tables.
It is then copied to the star schema tables using SQL queries in the form of ( insert select ..) statements
#### dl.cfg
This is a configuration file for storing account and environnement related variables.