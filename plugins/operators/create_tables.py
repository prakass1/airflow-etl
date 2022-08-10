from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):
    """ This operator will create the necessary tables required for analysis"""

    ui_color = '#FFA219'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        

    def execute(self, context):
        self.log.info('CreateTablesOperator execution started')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Remove the example users table avoiding conflicts
        redshift.run("DROP TABLE public.users;")
        # Reading the sql file and creating the tables.
        create_sql_f = open('/home/workspace/airflow/create_tables.sql', 'r')
        sqls = create_sql_f.read().split(';')
        for sql in sqls:
            if "CREATE" not in sql:
                continue
            else:
                self.log.info(f"Creating table for query -- {sql}")
                redshift.run(sql)
        self.log.info("Creation of the tables is now completed")
