from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,PostgresOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 20),
    'retries': 3, #1
    'retry_delay': timedelta (minutes = 5), #1
    'email_on_retry': False,
    'catchup': False
}


dag = DAG(
          'udac_etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = 'staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    s3_key='log_data/{execution_date.year}/{execution_date.month:02}/{ds}-events.json',
    json_format='s3://udacity-dend/log_json_path.json',
    provide_context=True,
    region='us-west-2' 
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = 'staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    provide_context=True,
    s3_key='song_data/A/A/A',
    json_format='auto',
    region='us-west-2'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id = 'aws_credentials',
    table="public.songplays",
    sql_headers=' (playid,start_time,userid,"level",songid,artistid,sessionid,location,user_agent)',
    sql_statement=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id = 'aws_credentials',
    table="public.users",
    sql_headers=' (userid,first_name,last_name,gender,"level")',
    sql_statement=SqlQueries.user_table_insert,
    append_only=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id = 'aws_credentials',
    table="public.songs",
    sql_headers=' (songid,title,artistid,"year",duration)',
    sql_statement=SqlQueries.song_table_insert,
    append_only=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id = 'aws_credentials',
    table="public.artists",
    sql_headers=' (artistid,name,location,lattitude,longitude)',
    sql_statement=SqlQueries.artist_table_insert,
    append_only=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id = 'aws_credentials',
    table="public.time",
    sql_headers=' (start_time,"hour","day",week,"month","year",weekday)',
    sql_statement=SqlQueries.time_table_insert,
    append_only=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id = 'aws_credentials',
    tables=["songplays", "songs", "users", "artists", "time"]
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator>>stage_events_to_redshift
start_operator>>stage_songs_to_redshift
stage_events_to_redshift>>load_songplays_table
stage_songs_to_redshift>>load_songplays_table
load_songplays_table>>load_song_dimension_table
load_songplays_table>>load_user_dimension_table
load_songplays_table>>load_artist_dimension_table
load_songplays_table>>load_time_dimension_table
load_song_dimension_table>>run_quality_checks
load_user_dimension_table>>run_quality_checks
load_artist_dimension_table>>run_quality_checks
load_time_dimension_table>>run_quality_checks
run_quality_checks>>end_operator