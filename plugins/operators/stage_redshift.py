from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_json_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        JSON '{}'
        COMPUPDATE OFF
                   
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 region='us-west-2',                 
                 json_path='',
                 file_format='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_path=json_path
        self.file_format=file_format
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Copy & transfer data from S3 to Redshift staging {self.table} table')
        key = self.s3_key.format(**context)
        self.log.info(f'Key: {key}')
        s3_path = f's3://{self.s3_bucket}/{key}'


        formatted_sql = StageToRedshiftOperator.copy_json_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path
            
        )
        redshift.run(formatted_sql)
            
                      
        #self.log.info(f'Executing: copy from S3 {s3_path} to Redshift')
        
        #self.log.info('Copy to Redshift is done')





