from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 append_mode=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.append_mode = append_mode

    def execute(self, context):
        self.log.info('CONNECTING TO REDSHIFT')
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('DROP TABLE RECORDS IF TABLE IS NOT EMPTY')
        if not self.append_mode:
            redshift.run('DELETE FROM {table}'.format(table = self.table))
            
        insert_sql = self.select_sql
        
        self.log.info('INSERTING INTO {TABLE}'.format(TABLE = self.table))
        redshift.run(insert_sql)