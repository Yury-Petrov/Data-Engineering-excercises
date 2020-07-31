from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 target_table,
                 load_fact_sql_query,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.__redshift_conn_id = redshift_conn_id
        self.__target_table = target_table
        self.__load_fact_sql_query = load_fact_sql_query

    def execute(self, context):
        self.log.info('executing LoadFactOperator')
        redshift = PostgresHook(postgres_conn_id=self.__redshift_conn_id)
        redshift.run(self.__get_append_to_facts_table_query())

    ####################################################################################################################
    #                                                                                                                  #
    #                                                 Private functions                                                #
    #                                                                                                                  #
    ####################################################################################################################

    def __get_append_to_facts_table_query(self):
        return f"""
            INSERT INTO {self.__target_table}
            {self.__load_fact_sql_query};
        """
