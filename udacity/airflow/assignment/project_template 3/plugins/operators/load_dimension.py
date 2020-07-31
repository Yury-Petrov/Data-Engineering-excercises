from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 target_table,
                 load_dimension_sql_query,
                 is_reload_from_fact=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.__redshift_conn_id = redshift_conn_id
        self.__target_table = target_table
        self.__load_dimension_sql_query = load_dimension_sql_query
        self.__isReloadFromFact = is_reload_from_fact

    def execute(self, context):
        self.log.info('executing LoadDimensionOperator')
        self.__reload_target_table_from_fact_table() if self.__isReloadFromFact else self.__append_to_target_table()

    ####################################################################################################################
    #                                                                                                                  #
    #                                                 Private functions                                                #
    #                                                                                                                  #
    ####################################################################################################################

    def __append_to_target_table(self):
        self.log.info(f"appending data to table {self.__target_table}")
        redshift = PostgresHook(postgres_conn_id=self.__redshift_conn_id)
        redshift.run(self.__get_append_sql_query())

    def __reload_target_table_from_fact_table(self):
        self.log.info(f"reloading data to table {self.__target_table}")
        redshift = PostgresHook(postgres_conn_id=self.__redshift_conn_id)
        redshift.run(self.__get_truncate_sql_query())

    def __get_append_sql_query(self):
        return f"""
            INSERT INTO {self.__target_table}
            {self.__load_dimension_sql_query};
        """

    def __get_truncate_sql_query(self):
        return f"""
            TRUNCATE TABLE {self.__target_table};
            INSERT INTO {self.__target_table}
            {self.__load_dimension_sql_query};
        """
