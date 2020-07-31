from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

####################################################################################################################
#                                                                                                                  #
#                                                 Private functions                                                #
#                                                                                                                  #
####################################################################################################################


def get_data_quality_test_cases():
    return [
        {
            DataQualityOperator.TEST_NAME: 'User table should have data in it',
            DataQualityOperator.TEST_QUERY:
                '''
                select count(1) from users;
                ''',
            DataQualityOperator.TEST_EXPECTED_RESULT: 0,
            DataQualityOperator.TEST_ASSERTION_TYPE: DataQualityOperator.ASSERT_MORE_THAN
        },
        {
            DataQualityOperator.TEST_NAME: 'Songs table should have data in it',
            DataQualityOperator.TEST_QUERY:
                '''
                select count(1) from songs;
                ''',
            DataQualityOperator.TEST_EXPECTED_RESULT: 0,
            DataQualityOperator.TEST_ASSERTION_TYPE: DataQualityOperator.ASSERT_MORE_THAN
        },
        {
            DataQualityOperator.TEST_NAME: 'Artists table should have data in it',
            DataQualityOperator.TEST_QUERY:
                '''
                select count(1) from artists;
                ''',
            DataQualityOperator.TEST_EXPECTED_RESULT: 0,
            DataQualityOperator.TEST_ASSERTION_TYPE: DataQualityOperator.ASSERT_MORE_THAN
        },
        {
            DataQualityOperator.TEST_NAME: 'Time table should have data in it',
            DataQualityOperator.TEST_QUERY:
                '''
                select count(1) from time;
                ''',
            DataQualityOperator.TEST_EXPECTED_RESULT: 0,
            DataQualityOperator.TEST_ASSERTION_TYPE: DataQualityOperator.ASSERT_MORE_THAN
        },
        {
            DataQualityOperator.TEST_NAME: 'SongId column in the songs table should never be null',
            DataQualityOperator.TEST_QUERY:
                '''
                select count(1) from songs where songid is null;
                ''',
            DataQualityOperator.TEST_EXPECTED_RESULT: 0,
            DataQualityOperator.TEST_ASSERTION_TYPE: DataQualityOperator.ASSERT_TRUE
        }
    ]


####################################################################################################################
#                                                                                                                  #
#                                                     DAG arguments                                                #
#                                                                                                                  #
####################################################################################################################

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 7, 29),
    'depends_on_past': False,
    'retires': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          catchup=False,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          )

####################################################################################################################
#                                                                                                                  #
#                                                     Task setup                                                   #
#                                                                                                                  #
####################################################################################################################


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    provide_context=False,
    dag=dag,
    target_table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    region="us-west-2",
    log_json_path='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    provide_context=False,
    dag=dag,
    target_table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    region="us-west-2",
    log_json_path='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    target_table='songplays',
    load_fact_sql_query=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    target_table='users',
    load_dimension_sql_query=SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    target_table='songs',
    load_dimension_sql_query=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    target_table='artists',
    load_dimension_sql_query=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    target_table='time',
    load_dimension_sql_query=SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    test_cases=get_data_quality_test_cases(),
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

####################################################################################################################
#                                                                                                                  #
#                                                DAG Tasks sequence                                                #
#                                                                                                                  #
####################################################################################################################


start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
