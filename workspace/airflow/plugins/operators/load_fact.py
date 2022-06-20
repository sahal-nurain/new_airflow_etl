from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 append_mode=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.append_mode = append_mode

    def execute(self, context):
        self.log.info('Connect Redshift.')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('DROP RECORDS IF TABLE ALREADY POPULATED')
        if not self.append_mode:
            redshift.run('DELETE FROM {table}'.format(table = self.table))
            
        insert_sql = self.select_sql
        
        self.log.info('Run insert SQL statement.')
        redshift.run(insert_sql)