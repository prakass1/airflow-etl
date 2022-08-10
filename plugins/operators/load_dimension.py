from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """ This operator will load the dimension tables"""

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 truncate,
                 query,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.truncate = truncate
        self.query = query
                
    def execute(self, context):
        self.log.info('LoadDimensionOperator has started now')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info(f'Truncating the dim table - {self.table_name} before insertion')
            redshift_hook.run(f"TRUNCATE TABLE {self.table_name}")
        self.log.info(f"Starting insert into dim table - {self.table_name}")
        redshift_hook.run(self.query)
        self.log.info(f"Insertion is now completed at the table - {self.table_name}")
