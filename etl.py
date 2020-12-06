import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS", "AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    ''' given input and outpout directories, 
    this function ingests songs from json files and generates: 
    parquet partitionned files for songs
    parquet normal file for artists '''
    
    song_data=input_data+"song_data/*/*/*/*.json"
    df = spark.read.json(song_data)

    song_cols=["song_id" , "title" , "artist_id" , "year" , "duration"] 
    songs_table = df.select(*song_cols)
    songs_table.repartition(1).write.parquet(output_data+'/songs', 'overwrite', ['year', 'artist_id'])

    artists_table_cols=["artist_id" , "artist_name" , "artist_location" , "artist_latitude" , "artist_longitude"]
    artists_table =df.select(*artists_table_cols) 
    artists_table.write.parquet(output_data+'/artists', 'overwrite')

def process_log_data(spark, input_data, output_data):
    ''' given input and outpout directories, 
    this function ingests events from json files and generates: 
    parquet partitionned files for time, and songplays
    parquet normal file for users '''
    
    log_data =input_data+"log_data/*.json"
    song_data=input_data+"song_data/*/*/*/*.json"
    
    df = spark.read.json(log_data)
    
    df.createOrReplaceTempView("staging_events")
    df = df.filter(df.page=="NextSong")
    users_table_cols =["userId" , "firstName" , "lastName" , "gender" , "level"] 
    users_table =df.select(*users_table_cols).dropDuplicates() 
    users_table.write.parquet(output_data+'/users', 'overwrite')
    
    time_table=df.select("ts")
    time_table.createOrReplaceTempView("time_table")
    time_table=spark.sql("select timestamp(ts/1000) as timestamp from time_table ")
    time_table.createOrReplaceTempView("time_table")# weirdly, this is necessary to rename ts to timestamp
    time_table=spark.sql("""select distinct timestamp, hour(timestamp) as hour,
                            day(timestamp) as day,weekofyear(timestamp) as week,
                            weekday(timestamp) as weekday,month(timestamp) as month,
                            year(timestamp) as year
                            from time_table""")
    time_table.repartition(1).write.parquet(output_data+'/time', 'overwrite', ['year', 'month'])
    
    song_df = spark.read.json(song_data)
    song_df.createOrReplaceTempView("staging_songs")
    songplays_table = spark.sql("""
    SELECT 
    timestamp(ts/1000) start_time, 
    events.userId, 
    events.level, 
    songs.song_Id, 
    songs.artist_Id, 
    events.sessionId, 
    events.location, 
    events.userAgent,
    year(timestamp(ts/1000)) year,
    month(timestamp(ts/1000)) month
         
    FROM staging_events events
    LEFT JOIN staging_songs songs
      ON lower(events.song) = lower(songs.title) 
      AND lower(events.artist) = lower(songs.artist_name) 
    WHERE page='NextSong' """)
    songplays_table.repartition(1).write.parquet(output_data+'/songplays','overwrite',['year','month'])

def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data = "data/"
    output_data = "spark-warehouse/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
