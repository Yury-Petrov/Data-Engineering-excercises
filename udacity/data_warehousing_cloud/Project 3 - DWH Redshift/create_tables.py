import configparser

import psycopg2

from redshift_utils import get_redshift_connection_string
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    The function to drop the tables for the sporkify system.
    :param cur: cursor to execute the query with.
    :param conn: connection to commit transaction.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    The function to create the tables for the sporkify system.
    :param cur: cursor to execute the query with.
    :param conn: connection to commit transaction.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    The main method to run in order to create tables for the sparkyfy system.
    This is done by running a set of queries described in sql_queries.py with connection details as per dwh.cfg.
    It is done in two steps":
    1. Dropping any tables that has been created for the task.
    2. Recreating the tables.
    """
    print("Creating tables")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    connection_string = get_redshift_connection_string(config=config)
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

    print("Tables have been successfully created")


if __name__ == "__main__":
    main()
