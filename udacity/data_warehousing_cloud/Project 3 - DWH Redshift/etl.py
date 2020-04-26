import configparser
import psycopg2

from redshift_utils import get_redshift_connection_string
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    The function to load data from s3 to the staging tables.
    :param cur: cursor to execute the query with.
    :param conn: connection to commit transaction.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    The function to insert the data from the staging tables to the star schema tables.
    :param cur: cursor to execute the query with.
    :param conn: connection to commit transaction.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    The main method to run in order to populate the dwh tables for the sparkify system.
    This is done by running a set of queries described in sql_queries.py with connection details as per dwh.cfg.
    It is done in two steps":
    1. Loading data from s3 to the staging tables.
    2. Inserting data to the dwh tables from the staging tables. Any data found in the staging tables that is also
    present in the facts/dimension tables is removed to avoid duplicates or stale state.
    """
    print("Starting etl")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    connection_string = get_redshift_connection_string(config=config)
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

    print("Etl is finished")


if __name__ == "__main__":
    main()
