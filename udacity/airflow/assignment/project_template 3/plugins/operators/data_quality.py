from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    TEST_NAME = "testName"
    TEST_QUERY = "testQuery"
    TEST_EXPECTED_RESULT = "testExpectedResult"
    TEST_ASSERTION_TYPE = 'assertionType'

    ASSERT_TRUE = '='
    ASSERT_FALSE = '!='
    ASSERT_LESS_THAN = '<'
    ASSERT_MORE_THAN = '>'


    __TEST_SUCCESS = 'Success'
    __TEST_MESSAGE = 'Message'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 test_cases,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.__redshift_conn_id = redshift_conn_id
        self.__test_cases = test_cases

        self.__assertions = {
                DataQualityOperator.ASSERT_TRUE: DataQualityOperator.__assert_true,
                DataQualityOperator.ASSERT_FALSE: DataQualityOperator.__assert_false,
                DataQualityOperator.ASSERT_LESS_THAN: DataQualityOperator.__assert_less_than,
                DataQualityOperator.ASSERT_MORE_THAN: DataQualityOperator.__assert_more_than
        }


    def execute(self, context):
        self.log.info('executing DataQualityOperator')
        redshift = PostgresHook(postgres_conn_id=self.__redshift_conn_id)
        test_results = []
        for test_case in self.__test_cases:
            test_results.append(self.__run_test(test_case, redshift))

        failed_tests = list(filter(lambda tr: tr[self.__TEST_SUCCESS] is False, test_results))
        successful_tests = list(filter(lambda tr: tr[self.__TEST_SUCCESS] is True, test_results))
        if len(failed_tests) > 0:
            self.log.info(successful_tests)
            raise ValueError(failed_tests)
        else:
            self.log.info(test_results)

    ####################################################################################################################
    #                                                                                                                  #
    #                                                 Private functions                                                #
    #                                                                                                                  #
    ####################################################################################################################

    def __run_test(self, test_case, redshift):
        result = redshift.get_records(test_case[DataQualityOperator.TEST_QUERY])

        return {
            DataQualityOperator.__TEST_MESSAGE: f"""
            Actual result {result[0][0]} should be {test_case[DataQualityOperator.TEST_ASSERTION_TYPE]} Expected result {test_case[DataQualityOperator.TEST_EXPECTED_RESULT]}.
        """,
            DataQualityOperator.__TEST_SUCCESS: self.__assertions[test_case[DataQualityOperator.TEST_ASSERTION_TYPE]](
                test_case[DataQualityOperator.TEST_EXPECTED_RESULT],
                result[0][0]
            )
        }

    @staticmethod
    def __assert_true(expected, actual):
        return expected == actual
    @staticmethod
    def __assert_false(expected, actual):
        return expected != actual

    @staticmethod
    def __assert_less_than(expected, actual):
        return actual < expected

    @staticmethod
    def __assert_more_than(expected, actual):
        return actual > expected
