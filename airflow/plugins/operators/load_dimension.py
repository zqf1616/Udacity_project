from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    dimension_table_insert_sql = """
    INSERT INTO {destination_table}
    {insert_sql}
    """
    
#     dimension_table_drop_sql = """
#     DELETE FROM {destination_table}
#     """
        
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 destination_table="",
                 insert_sql = "",
                 append_only = False,
                 *args, **kwargs):
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.insert_sql = insert_sql 
        self.append_only = append_only

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        dimension_insert_sql = LoadFactOperator.dimension_table_insert_sql.format(
            destination_table=self.destination_table,
            insert_sql = self.insert_sql)
#         dimension_drop_sql = LoadFactOperator.dimension_table_drop_sql.format(
#             destination_table=self.destination_table)
        
        if self.append_only == False:
            #drop table
            redshift.run("DELETE FROM {}".format(self.table))
            redshift.run(dimension_insert_sql)
            self.log.info('LoadDimensionOperator executed. append_only = False')  
        else:
            redshift.run(dimension_insert_sql)
            self.log.info('LoadDimensionOperator executed. append_only = True')
            
