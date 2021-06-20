from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,PostgresOperator)

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta (minutes = 5),
    'email_on_retry': False,
    'catchup': False
}


dag = DAG(
          'udac_table_creation',
          start_date=datetime.utcnow(),
          default_args=default_args,
          description='Create Tables in Redshift with Airflow',
        )


create_tables_task = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_tables.sql',
  postgres_conn_id="redshift"
)
