import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, unix_timestamp, from_unixtime, dayofweek
from pyspark.sql.types import IntegerType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('KEY','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('KEY','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Spark sql session creation function
    Spark allows us to to perform Big Data
    analyis and access, transform and load
    data from a variety of formats
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    This block of code reads the Song data
    from JSON files and stores them into 
    organized parquet files in the format
    of dimension tables
    """
    for i in range(0,5):
        print(" ")
    print("------- START OF CODE -------")
    for i in range(0,5):
        print(" ")
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    #song_data = os.path.join(input_data, 'song_data/A/A/*/*.json')

    # read song data file
    df_songs = spark.read.json(song_data)
    df_songs.createOrReplaceTempView("df_songs_table_view")

    # extract columns to create songs table
    songs_table =  df_songs.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, 'songs'), 'overwrite')
    
    # extract columns to create artists table
    artists_table =  df_songs.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").distinct()
 
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')

def process_log_data(spark, input_data, output_data):
    """
    This block of code reads the Log data
    from JSON files and stores them into 
    organized parquet files in the format
    of dimension tables
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')
    #log_data = os.path.join(input_data, 'log_data/2018/11/*.json')
    
    # read log data file
    df_logs = spark.read.json(log_data) 
    
    # filter by actions for song plays
    df_logs_filtered = df_logs.filter(df_logs.page == "NextSong")

    # extract columns for users table    
    users_table =df_logs_filtered.select("userId", "firstName", "lastName", "gender", "level").distinct()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, IntegerType())
    df_logs_filtered = df_logs_filtered.withColumn('start_time', get_timestamp('ts'))
    
    
    # create datetime column from original timestamp column
    df_logs_filtered = df_logs_filtered.withColumn("datetime", from_unixtime("start_time"))
    df_logs_filtered = df_logs_filtered.withColumn('weekday', dayofweek("datetime"))    
    
    # extract columns to create time table
    df_logs_filtered.createOrReplaceTempView("log_table_view")
    time_table = spark.sql('''
            SELECT
            start_time,
            extract(hour from datetime) as hour,
            extract(day from datetime) as day,
            extract(week from datetime) as week,
            extract(month from datetime) as month,
            extract(year from datetime) as year,
            weekday
            FROM log_table_view
    ''')
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, 'time'), 'overwrite')

    # read in song data to use for songplays table
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
    SELECT DISTINCT
    ROW_NUMBER() OVER( ORDER BY start_time ) AS songplay_id,
    l.start_time,
    l.userid,
    l.level,
    s.song_id,
    s.artist_id,
    l.sessionid,
    l.location,
    l.useragent,
    extract(month from l.datetime) as month,
    extract(year from l.datetime) as year   
    FROM log_table_view l
    JOIN df_songs_table_view s ON l.artist = s.artist_name 
    AND l.song = s.title
    AND l.length = s.duration
    ''')    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, 'songplays'), 'overwrite')


    ## VISUALIZE CREATED PARQUET TABLES
    #print("______songs_table________")
    #songs_table.createOrReplaceTempView("songs_table_view")
    #spark.sql("SELECT * FROM songs_table_view LIMIT 5").show()

    #print("______artists_table________")
    #artists_table.createOrReplaceTempView("artists_table_view")
    #spark.sql("SELECT * FROM artists_table_view LIMIT 5").show()
    
    #print("______users_table________")
    #users_table.createOrReplaceTempView("users_table_view")
    #park.sql("SELECT * FROM users_table_view LIMIT 5").show()

    #print("______time_table_table________")
    #time_table.createOrReplaceTempView("time_table_view")
    #spark.sql("SELECT * FROM time_table_view LIMIT 5").show()

    print("______songplays_table________")
    songplays_table.createOrReplaceTempView("songplays_table_view")
    spark.sql("SELECT * FROM songplays_table_view LIMIT 5").show()


    for i in range(0,5):
        print(" ")
    print("######## END OF CODE ############")
    for i in range(0,5):
        print(" ")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
