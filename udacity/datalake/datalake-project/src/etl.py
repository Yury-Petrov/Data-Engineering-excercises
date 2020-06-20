import configparser
import uuid
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, row_number, last, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
input_location_cfg = config['AWS']['INPUT_LOCATION']
input_location_cfg = config['AWS']['INPUT_LOCATION']
output_location_cfg = config['AWS']['OUTPUT_LOCATION']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def read_song_data(spark, input_data):
    """
    Convenience wrapper function to get a read dataframe for the song data json files
    :param spark: spark session to execute queries against
    :param input_data: s3 bucket (formatted) location of the input data
    :return: spark dataframe with the loaded song data
    """
    song_data = f"{input_data}/song_data/*/*/*/*.json"

    schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", FloatType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", FloatType()),
        StructField("artist_name", StringType()),
        StructField("duration", FloatType()),
        StructField("num_songs", IntegerType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", IntegerType())
    ])
    return spark.read.json(song_data, schema=schema)


def process_song_data(spark, input_data, output_data):
    """
    The function that starts processing of the song data set
    :param spark: spark session to execute queries against
    :param input_data: s3 bucket (formatted) location of the input data
    :param output_data: s3 bucket (formatted) of the output data
    """
    song_df = read_song_data(spark, input_data)
    song_df.cache()

    songs_table_df = song_df.dropDuplicates(["song_id"]).select(
        song_df.song_id,
        song_df.title,
        song_df.artist_id,
        song_df.year,
        song_df.duration
    )

    songs_table_df \
        .write \
        .mode("overwrite") \
        .partitionBy("year","artist_id") \
        .parquet(f"{output_data}/songs_table")

    artists_table_df = song_df.dropDuplicates(["artist_id"]).select(
        song_df.artist_id,
        song_df.artist_name.alias('name'),
        song_df.artist_location.alias('location'),
        song_df.artist_latitude.alias('latitude'),
        song_df.artist_longitude.alias('longitude')
    )

    artists_table_df \
        .write \
        .mode("overwrite") \
        .parquet(f"{output_data}/artists_table")


def process_log_data(spark, input_data, output_data):
    """
    The function that starts processing of the log data set
    :param spark: spark session to execute queries against
    :param input_data: s3 bucket (formatted) location of the input data
    :param output_data: s3 bucket (formatted) of the output data
    """
    # get filepath to log data file
    log_data = f"{input_data}/log_data/*/*/*.json"

    # read log data file
    log_df = spark.read.json(log_data)

    # filter by actions for song plays
    next_song_log_df = log_df.filter(col('page') == 'NextSong')

    # extract columns for users table
    user_id_by_ts_window = Window.partitionBy(
        col('userId')) \
        .orderBy(col('ts'))
    user_id_by_ts_window_ranged = Window.partitionBy(
        col('userId')) \
        .orderBy(col('ts').desc()) \
        .rangeBetween(Window.unboundedPreceding, Window.currentRow)

    users_df = next_song_log_df \
        .withColumn('user_row_num', row_number().over(user_id_by_ts_window)) \
        .withColumn('firstName', last('firstName').over(user_id_by_ts_window_ranged)) \
        .withColumn('lastName', last('lastName').over(user_id_by_ts_window_ranged)) \
        .withColumn('gender', last('gender').over(user_id_by_ts_window_ranged)) \
        .withColumn('level', last('level').over(user_id_by_ts_window_ranged)) \
        .select('firstName', 'lastName', 'gender', 'level', 'userid', 'user_row_num') \
        .where(col('user_row_num') == 1)

    # write users table to parquet files
    users_df \
        .write \
        .mode("overwrite") \
        .parquet(f"{output_data}/users_table")

    # create timestamp column from original timestamp column
    log_with_time_df = log_df \
        .withColumn('start_time',
                    (col('ts') / 1000).cast(IntegerType())) \
        .dropDuplicates()
    log_with_time_df = log_with_time_df \
        .withColumn('ts_as_datetime', from_unixtime(col('start_time'), 'yyyy-MM-dd HH:mm:ss').cast(TimestampType())) \
        .withColumn('hour', hour(col('ts_as_datetime'))) \
        .withColumn('day', dayofmonth(col('ts_as_datetime'))) \
        .withColumn('week', weekofyear(col('ts_as_datetime'))) \
        .withColumn('month', month(col('ts_as_datetime'))) \
        .withColumn('year', year(col('ts_as_datetime'))) \
        .withColumn('weekday', weekofyear(col('ts_as_datetime')))

    # write time table to parquet files partitioned by year and month
    log_with_time_df \
        .write \
        .partitionBy('year', 'month') \
        .mode("overwrite") \
        .parquet(f"{output_data}/time_table")

    # read in song data to use for songplays table
    song_df = read_song_data(spark, input_data)
    song_df = song_df.drop('year')

    uuidUdf = udf(lambda: str(uuid.uuid4()), StringType())
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = log_with_time_df \
        .join(song_df, song_df.title == log_df.song) \
        .select(
            uuidUdf().alias('songplay_id'),
            col('start_time').alias('start_time'),
            col('userId').alias('user_id'),
            col('level').alias('level'),
            col('song_id').alias('song_id'),
            col('artist_id').alias('artist_id'),
            col('sessionId').alias('session_id'),
            col('location').alias('location'),
            col('userAgent').alias('user_agent'),
            col('year').alias('year'),
            col('month').alias('month'),
        )

# write songplays table to parquet files partitioned by year and month
    songplays_table \
        .write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(f"{output_data}/songplays_table")

def main():
    """
    The main funcion to run for ETL
    """
    spark = create_spark_session()
    input_data = input_location_cfg
    output_data = output_location_cfg

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
