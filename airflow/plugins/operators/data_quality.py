from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

'''
tests: An array of tests that will be performed. 
The format of test should be something like the following:
[
{"table":"staging_events",
"sql":"SELECT COUNT(*) FROM staging_events WHERE sessionid is null",
"expected_result":0},

{"table":"staging_songs",
"sql":"SELECT COUNT(*) FROM staging_songs WHERE song_id is null",
"expected_result":0}
]

'''
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 tests = [],                 
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests
        
  

    def execute(self, context):
        # self.log.info('DataQualityOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for test in self.tests:
            table = test.get('table')
            sql = test.get('sql')
            expected_result = test.get('expected_result')
            result = redshift_hook.get_records(sql)[0][0]
            if expected_result != result:
                raise ValueError(f"Data quality for {table} failed")
            
        logging.info("Data quality checks completed successfully")