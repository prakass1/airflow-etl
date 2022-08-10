from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """ This operator will perform required data checks for insertions made"""

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 dq_checks: list,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info('DataQualityOperator has started execution')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if len(self.dq_checks) > 0:
            for dq in self.dq_checks:
                if isinstance(dq, dict):
                    records = redshift_hook.get_records(dq["sql_check"])
                    if len(records) < 1 or len(records[0]) < 1:
                        raise ValueError(f"Data quality check failed. {dq['table_name']} returned no results")
                    num_records = records[0][0]
                    if num_records < dq["min_expect"]:
                        raise ValueError(f"Data quality check failed. {dq['table_name']} contained 0 rows")
                    self.log.info(f"The dq check - {dq} for table - {dq['table_name']} passed with {records[0][0]} records")
