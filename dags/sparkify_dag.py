# The project performs a Data pipelining using airflow to stage the data from s3, load the fact, dimension tables, perform data
# quality checks, and report it.
# As per the feedback, I have created a Subdag which encapsulates the load for dimension tables (users, artists, songs, time).
# As per the feedback, I have removed create_table from data pipeline. It is now possible to create it through query editor from redshift
# Automated the data quality checks through dynamic query creation and validation on the fly.


from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator,
                               LoadFactOperator,
                               LoadDimensionOperator,
                               DataQualityOperator,
                               CreateTablesOperator)
from helpers import SqlQueries
from helpers.data_quality_checks import generate_dqs
from load_subdag import load_subdag

# Create connections for these variables in the Airflow UI.
AWS_CONN_ID="aws_credentials"
REDSHIFT_CONN_ID="redshift"

default_args = {
    'owner': 'sparkify-user',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retries_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          # Actually catchup is added here. Please see the documentation: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html?highlight=catchup#top-level-python-code
          catchup=False,
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution', retries=1, dag=dag)

# Create the tables in redshift cluster. The requirement is to create it through redshift query.
# create_tables = CreateTablesOperator(redshift_conn_id=REDSHIFT_CONN_ID,task_id='create-tables',dag=dag)

# Stage Events
stage_events = StageToRedshiftOperator(task_id="stage-events",
                                       redshift_conn_id=REDSHIFT_CONN_ID,
                                       table="public.staging_events",
                                       s3_loc="s3://udacity-dend/log_data",
                                       aws_cred_id=AWS_CONN_ID,
                                       region='us-west-2',
                                       dag=dag
                                      )


# Stage Songs
stage_songs = StageToRedshiftOperator(task_id="stage-songs",
                                       redshift_conn_id=REDSHIFT_CONN_ID,
                                       table="public.staging_songs",
                                       s3_loc="s3://udacity-dend/song_data",
                                       aws_cred_id=AWS_CONN_ID,
                                       region='us-west-2',
                                       dag=dag
                                      )




# Load Fact - songsplay
load_fact_songplays = LoadFactOperator(task_id="load-fact-songplays",
                                  redshift_conn_id="redshift",
                                  fact_table_name="songplays",
                                  query=SqlQueries.songplays_table_insert,
                                  dag=dag
                                  )

# SubDag handling inner functions of each LoadOperation based on https://www.astronomer.io/guides/subdags/

load_dim_tasks = SubDagOperator(
    task_id="load_tasks",
    subdag=load_subdag(
        parent_dag_name=dag.dag_id,
        child_dag_name="load_tasks",
        dim_tables=["users","songs","artists","time"],
        args=default_args
    ),
    default_args=default_args,
    dag=dag,
)

# Load Dimension tables
# 1. User
#load_dim_users = LoadDimensionOperator(task_id="load-dim-user",
#                                     redshift_conn_id="redshift",
#                                     table_name="users",
#                                     truncate=True,
#                                     query=SqlQueries.users_table_insert,
#                                     dag=dag)
# 2. Song
#load_dim_songs = LoadDimensionOperator(task_id="load-dim-song",
#                                     redshift_conn_id="redshift",
#                                     table_name="songs",
#                                     truncate=True,
#                                     query=SqlQueries.songs_table_insert,
#                                     dag=dag)

# 3. Artist
#load_dim_artists = LoadDimensionOperator(task_id="load-dim-artist",
#                                     redshift_conn_id="redshift",
#                                     table_name="artists",
#                                     truncate=True,
#                                     query=SqlQueries.artists_table_insert,
#                                     dag=dag)

# 4. Time
#load_dim_time = LoadDimensionOperator(task_id="load-dim-time",
#                                     redshift_conn_id="redshift",
#                                     table_name="time",
#                                     truncate=True,
#                                     query=SqlQueries.time_table_insert,
#                                     dag=dag)


# Data Quality Check and Report.
check_tables=["songplays","users","songs","artists","time"]
dq_checks = DataQualityOperator(task_id="data-quality-checks",
                                redshift_conn_id="redshift",
                                #check_tables=["songplays","users","songs","artists","time"],
                                dq_checks=generate_dqs(check_tables),
                                dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', retries=1,  dag=dag)

start_operator >> [stage_events, stage_songs] >> load_fact_songplays
# load_fact_songplays >> [load_dim_users, load_dim_songs, load_dim_artists, load_dim_time] >> dq_checks >> end_operator
load_fact_songplays >> load_dim_tasks >> dq_checks >> end_operator
