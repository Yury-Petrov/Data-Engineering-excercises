from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_conn_id="",
                 target_table="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.__redshift_conn_id = redshift_conn_id
        self.__aws_conn_id = aws_conn_id
        self.__target_table = target_table
        self.__s3_bucket = s3_bucket
        self.__s3_key = s3_key
        self.__region = region
        self.__execution_date = kwargs.get("execution_datetime")

    def execute(self, context):
        self.log.info('Running StageToRedshiftOperator')

        redshift = PostgresHook(postgres_conn_id=self.__redshift_conn_id)
        self.log.info("Clearing data from the target Redshift table")
        redshift.run("DELETE FROM {}".format(self.__target_table))
        self.log.info("Copying data from S3 to Redshift")

        aws_hook = AwsHook(self.__aws_conn_id)
        credentials = aws_hook.get_credentials()

        redshift.run(self.__get_copy_sql_query(access_key=credentials.access_key, secret_key=credentials.secret_key))

    ####################################################################################################################
    #                                                                                                                  #
    #                                                 Private functions                                                #
    #                                                                                                                  #
    ####################################################################################################################

    def __get_copy_sql_query(self,
                             access_key: str,
                             secret_key: str):
        s3_path = f"s3://{self.__s3_bucket}/{self.__s3_key}" + (
            f"/{self.__execution_date.strftime('%Y')}/{self.__execution_date.strftime('%d')}"
            if self.__execution_date
            else ""
        )
        return f"""
        COPY {self.__target_table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        REGION '{self.__region}'
        json 'auto'
    """
