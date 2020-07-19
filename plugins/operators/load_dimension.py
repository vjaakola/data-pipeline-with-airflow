from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    
    insert_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql_stmt='',
                 table='',
                 truncate_table='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args,**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f'Truncating: {self.table}')
            redshift.run(LoadDimensionOperator.truncate_sql.format(self.table))

        self.log.info(f'Loading: {self.table}')
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql_stmt
        )
        
        self.log.info(f'Executing: {formatted_sql}')
        redshift.run(formatted_sql)

