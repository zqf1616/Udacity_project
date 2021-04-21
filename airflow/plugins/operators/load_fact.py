from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    fact_table_insert_sql = """
    INSERT INTO {destination_table}
    {insert_sql}
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 destination_table="",
                 insert_sql = "",
                 *args, **kwargs):
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.insert_sql = insert_sql        

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        #self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        fact_sql = LoadFactOperator.fact_table_insert_sql.format(
            destination_table=self.destination_table,
            insert_sql = self.insert_sql)
        redshift.run(fact_sql)
        self.log.info('LoadFactOperator executed')        
