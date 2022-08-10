from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators import LoadDimensionOperator
from helpers import SqlQueries


def load_subdag(parent_dag_name, child_dag_name, dim_tables, args):
    dag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args
    )
    # Define Subdag for each of the dim_tables
    with dag_subdag:
        for table in dim_tables:
            t = LoadDimensionOperator(task_id=f"load-dim-{table}",
                                     redshift_conn_id="redshift",
                                     table_name=table,
                                     truncate=True,
                                     query=SqlQueries.dim_table_mapping[table],
                                     dag=dag_subdag)

    return dag_subdag