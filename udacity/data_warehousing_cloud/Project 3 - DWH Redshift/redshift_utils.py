from configparser import ConfigParser
from typing import Dict

import boto3


def get_redshift_cluster_description(region: str, key: str, secret: str, cluster_identifier: str) -> Dict:
    """
    Convenience method to get description of the redshift cluster
    :param region: region where the cl;uster is created
    :param key: aws credentials key
    :param secret: aws credentials secret
    :param cluster_identifier: cluster name
    :return: cluster description as per
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html#Redshift.Client.describe_clusters
    """
    redshift = boto3.client(
        'redshift',
        region_name=region,
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )

    return redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]


def get_redshift_connection_string(config: ConfigParser) -> str:
    """
    Convenience method to get redhsift address and port as a part of the endpoint section of the cluster description and
    then generate a db connection string from it and the config (dwh.cfg)
    :param config: config with parameters for the cluster
    :return: connection string to use to get connected to the redshift cluster.
    """
    KEY = config.get("AWS", "KEY")
    SECRET = config.get("AWS", "SECRET")
    REGION = config.get("DWH", "DWH_REGION")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")

    endpoint_info = get_redshift_cluster_description(
        region=REGION,
        key=KEY,
        secret=SECRET,
        cluster_identifier=DWH_CLUSTER_IDENTIFIER)['Endpoint']
    return f"host={endpoint_info['Address']} dbname={DWH_DB} user={DWH_DB_USER} password={DWH_DB_PASSWORD} port={endpoint_info['Port']}"
