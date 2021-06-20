from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook



class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)
    copy_sql ="""
       COPY {}
       FROM '{}'
       ACCESS_KEY_ID '{}'
       SECRET_ACCESS_KEY '{}'
       json '{}'
       """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 json_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id        
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region=region
        self.json_format=json_format

        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)


        self.log.info("Copying data from S3 to Redshift")
        s3_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, s3_key)
        self.log.info("s3_path is {}".format(s3_key))
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_format

        )
        redshift.run(formatted_sql)
        sql="""Select count(*) from {}""".format(self.table)
        sql1="""Select top 100 * from {}""".format(self.table)
        result = redshift.get_first(sql)[0]
        result1=redshift.get_records(sql1)
        self.log.info("Rows in {} {}".format(self.table,result))
        self.log.info(result1)

                      




