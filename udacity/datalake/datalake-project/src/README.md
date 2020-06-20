# Udacity Datalake assignment

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.
## Files description
1. etl.py - the main execution script with the logic to extract the data from an s3 location
and, transform it according to the requirements and store it in another s3 location.
1. dl.cfg - config file that needs to have the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY provided
in order to get access for the s3 locations.
## Project description
The scope of the projects includes the following aspects:
1. Extracting the data from the ``udacity-dend`` s3 bucket.
The data is devided into a collection of json files related to songs (`song_data`)
and logs (`log_data`)
The songs data is read with a provided schema in order to properly set the names and types.
This is done in a result of a test, which failed due to an incorrect schema inference on a smaller data subset.
1. Transformation involves data filtering, removing duplicates, asigning ids (songplays table requires ids,
for which guids are being used) and generating additional columns for time-based data queries.
1. Loading - data loading part of the script partitions and persists transformed data to s3.
## Tables description
The tables are described below through their schemas printed by spark.
### Dimension tables
* Songs_table
```
root
 |-- artist_id: string (nullable = true)
 |-- artist_latitude: float (nullable = true)
 |-- artist_location: string (nullable = true)
 |-- artist_longitude: float (nullable = true)
 |-- artist_name: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- duration: float (nullable = true)
 |-- num_songs: integer (nullable = true)
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- year: integer (nullable = true)

```
* Artists_table
```
root
 |-- artist_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- location: string (nullable = true)
 |-- latitude: float (nullable = true)
 |-- longitude: float (nullable = true)

```
* Time_table
```
root
 |-- start_time: integer (nullable = true)
 |-- ts_as_datetime: timestamp (nullable = true)
 |-- hour: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- week: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- houyear: integer (nullable = true)
 |-- weekday: integer (nullable = true)

```
* Users_table
```
root
 |-- firstName: string (nullable = true)
 |-- lastName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
 |-- userid: string (nullable = true)
```
### Facts table

* Songplays_table
```
root
 |-- start_time: long (nullable = true)
 |-- user_id: string (nullable = true)
 |-- level: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- session_id: long (nullable = true)
 |-- location: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- month: integer (nullable = true)
 |-- year: integer (nullable = true)
 |-- songplay_id: string (nullable = true)

```
## How to run

1. Please make sure that the dl.cfg contains the correct configuration.
The configuration requires correct aws credentials and input/output locations for the data.
1. The etl.py script needs to be run on a machine that supports pyspark.
The script was tested on spark version 2.4.4.
The script can either be run as a step on a cluster or via a jupyter notebook.