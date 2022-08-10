from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """ This operator will load the fact tables"""
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 fact_table_name,
                 query,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table_name = fact_table_name
        self.query = query

    def execute(self, context):
        self.log.info('LoadFactOperator has started execution')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Running the insertion into fact table - {self.fact_table_name}')
        redshift_hook.run(self.query)
        self.log.info(f'Insertion is completed to fact table - {self.fact_table_name}')
        