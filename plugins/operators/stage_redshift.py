from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os


class StageToRedshiftOperator(BaseOperator):
    """ This operator will use the copy command and stage it to Redshift"""
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS
        json 'auto ignorecase'
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 s3_loc,
                 aws_cred_id,
                 region,
                 *args, **kwargs):
        print(kwargs)
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_loc = s3_loc
        self.aws_cred_id = aws_cred_id
        self.region = region
        self.execution_date = kwargs.get('ds')

    def execute(self, context):
        self.log.info('StageToRedshiftOperator execution started')
        aws_hook = AwsHook(self.aws_cred_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Clearing the data from {self.table} before insertion")
        redshift_hook.run(f"DELETE FROM {self.table}")
        self.log.info("Copying data from S3 to Redshift")
        s3_path=self.s3_loc
        if self.execution_date:
            year=str(self.execution_date.strftime("%Y"))
            month=str(self.execution_date.strftime("%m"))
            s3_path=os.path.join(s3_path, year, month)
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region
        )
        redshift_hook.run(formatted_sql)
        self.log.info(f"The staging to the table - {self.table} is now completed")
