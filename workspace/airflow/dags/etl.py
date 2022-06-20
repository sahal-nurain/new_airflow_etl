from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'sahal',
    'start_date': datetime(2022, 6, 18),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(
    'udacity_etl_dag_4',
    default_args=default_args,
    description='S3 TO REDSHIFT ETL PIPELINE',
    schedule_interval='0 * * * *',
    max_active_runs=1
)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)



stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_conn_id="aws",
    s3_path = "s3://udacity-dend/song_data",
    table="staging_songs",
    json_option="auto",
    copy_options = ['csv']
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_conn_id="aws",
    s3_path = "s3://udacity-dend/log_json_path.json",
    table="staging_songs",
    json_option="auto",
    copy_options = ['csv']
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    append=True,
    table='songplays',
    select_sql=SqlQueries.songplay_table_insert,
    dag=dag)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    table='users',
    select_sql=SqlQueries.user_table_insert,
    append_mode=False,
    dag=dag)
    
    
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    table='songs',
    select_sql=SqlQueries.song_table_insert,
    append_mode=False,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    table='artists',
    select_sql=SqlQueries.artist_table_insert,
    append_mode=False,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    table='time',
    select_sql=SqlQueries.time_table_insert,
    append_mode=False,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    statement ="SELECT COUNT(*) FROM artists WHERE artist_id IS NULL"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

## order of operation
    
# create tables & load data to staging tables

start_operator >>  stage_events_to_redshift
start_operator >> stage_songs_to_redshift


# create songplay table
stage_events_to_redshift >> load_songplays_table 
stage_songs_to_redshift >> load_songplays_table 

# create dimension tables
load_songplays_table  >> load_user_dimension_table
load_songplays_table  >> load_song_dimension_table
load_songplays_table  >> load_artist_dimension_table
load_songplays_table  >> load_time_dimension_table

# perform quality check
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# end operation
run_quality_checks >> end_operator

    