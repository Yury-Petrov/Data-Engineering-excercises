import configparser
from typing import Dict

import boto3
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from redshift_utils import get_redshift_connection_string


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def get_endpoint_info(region: str, key: str, secret: str, cluster_identifier: str) -> Dict:
    redshift = boto3.client(
        'redshift',
        region_name=region,
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )

    return redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]['Endpoint']


def main():
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
