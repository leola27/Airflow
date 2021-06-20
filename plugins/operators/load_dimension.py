from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql_statement="",
                 sql_headers="",
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id   
        self.sql_statement=sql_statement
        self.sql_headers=sql_headers
        self.append_only=append_only


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_only==False:   
            self.log.info('Deleting data from {}'.format(self.table))
            sql_delete_statement='''Delete from {}'''.format(self.table)
            redshift.run(sql_delete_statement)
            self.log.info('Inserting Dimension data for {} '.format(self.table))
            sql_insert_statement =  '''INSERT INTO {} {} {}'''.format(self.table,self.sql_headers,self.sql_statement)
            redshift.run(sql_insert_statement)
        
        else:        
            self.log.info('Inserting Dimension data for {} '.format(self.table))
            sql_insert_statement =  '''INSERT INTO {} {} {}'''.format(self.table,self.sql_headers,self.sql_statement)
            redshift.run(sql_insert_statement)
