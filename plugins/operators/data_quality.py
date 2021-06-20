from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 null_checks="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id   
        self.null_checks=null_checks
        self.tables=tables

    def execute(self, context):        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
                   
        for table in self.tables:
            no_of_records=redshift.get_first("""Select COUNT(*) from {}""".format(table))[0]
            
            if len(no_of_records) <1:
                raise ValueError('Number of records check failed. Table {} contained 0 records'.format(table))
        
        
        
        