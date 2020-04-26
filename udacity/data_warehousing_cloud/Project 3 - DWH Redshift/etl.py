import configparser
import psycopg2

from redshift_utils import get_redshift_connection_string
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
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
