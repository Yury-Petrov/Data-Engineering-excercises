from configparser import ConfigParser
from typing import Dict

import boto3


def get_redshift_cluster_description(region: str, key: str, secret: str, cluster_identifier: str) -> Dict:
    redshift = boto3.client(
        'redshift',
        region_name=region,
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )

    return redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]


def get_redshift_connection_string(config: ConfigParser) -> str:
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
