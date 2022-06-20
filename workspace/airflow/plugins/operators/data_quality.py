from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id,
        statement,
        *args,
        **kwargs,
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.statement = statement
        
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        expected_result = 0
        self.log.info("Starting Quality Check")
        
        records = redshift.get_records(self.statement)[0]
        if records[0] == 0 :
                self.log.info("PASS!")
        elif records[0] != exp_result:
                ValueError("FAIL!")
        else:
                self.log.info("FAIL!")
                
        self.log.info("QUALITY CHECK COMPETE")