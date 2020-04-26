import configparser
import json

from configparser import SectionProxy

from typing import List

import time
import boto3

from botocore.exceptions import ClientError


class InfrastructureManager:
    """
    This class is provides functionality for infrastructure creation operations required to set up a Redshift cluster.
    """
    def __init__(self, aws_config: SectionProxy, dwh_config: SectionProxy):
        KEY = aws_config.get('KEY')
        SECRET = aws_config.get('SECRET')
        REGION = dwh_config.get('DWH_REGION')

        self.__iam = boto3.client(
            'iam',
            region_name=REGION,
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET
        )
        self.__redshift = boto3.client(
            'redshift',
            region_name=REGION,
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET
        )
        self.__ec2 = boto3.resource(
            'ec2',
            region_name=REGION,
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET
        )

        self.dwh_config = dwh_config

        self.__redshift_cluster_creation_timeout = 600
        self.__redshift_cluster_creation_waiting_period = 60
        self.__redshift_cluster_failed_statuses = ['failed', 'unavailable', 'deleting', 'hardware-failure']
        self.__redshift_cluster_acceptable_statuses = ['available']

    def create_infrastructure(self):
        """
        This function runs several methods required to create infrastructure for Redshift cluster.
        The steps include:
        1. Create iam role for redshift.
        2. Attach s3 readonly policy to the role in step 1.
        3. Create redshift cluster with the role created in step 1.

        Please note that configuration that drives the infrastructure creation process should be provided in dwh.cfg
        """
        self.__create_iam_role(role_name=self.dwh_config.get('DWH_IAM_ROLE_NAME'))
        role_arn = self.__attach_s3_policy(role_name=self.dwh_config.get('DWH_IAM_ROLE_NAME'))
        self.__create_redshift_cluster(roleArns=[role_arn])
        self.__allow_ingress()

    def destroy_infrastructure(self):
        """
        This functions runs through several methods to destroy the infrastructure for redshift cluster.
        1. The cluster deletion request is sent.
        2. The policy is detached from the role.
        3. The role is deleted.
        """

        print(f"submitting request to to delete culster {self.dwh_config.get('DWH_CLUSTER_IDENTIFIER')}")
        self.__redshift.delete_cluster(ClusterIdentifier=self.dwh_config.get('DWH_CLUSTER_IDENTIFIER'),
                                       SkipFinalClusterSnapshot=True)
        print(f"Detaching s3 readonly policy from role {self.dwh_config.get('DWH_IAM_ROLE_NAME')}")
        self.__iam.detach_role_policy(RoleName=self.dwh_config.get('DWH_IAM_ROLE_NAME'),
                                      PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        print(f"Deleting role {self.dwh_config.get('DWH_IAM_ROLE_NAME')}")
        self.__iam.delete_role(RoleName=self.dwh_config.get('DWH_IAM_ROLE_NAME'))

    ####################################################################################################################
    #                                                                                                                  #
    #                                                  Private methods                                                 #
    #                                                                                                                  #
    ####################################################################################################################

    def __create_iam_role(self, role_name: str):
        """
        This function creates a new iam role for redshift cluster
        :param role_name: name of the role
        """
        print(f'Creating iam role {role_name}')
        try:
            self.__iam.create_role(
                Path='/',
                RoleName=role_name,
                Description="Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Action": [
                                "sts:AssumeRole"
                            ],
                            "Effect": "Allow",
                            'Principal': {'Service': 'redshift.amazonaws.com'}
                        }]
                })
            )
        except ClientError as ce:
            print(ce)
            if ce.response['Error']['Code'] == 'EntityAlreadyExists':
                print("Will continue execution using this role")
            else:
                raise
        except Exception as e:
            print(e)
            raise

    def __attach_s3_policy(self, role_name: str) -> str:
        """
        The function attaches the s3 readonly policy ti the provided role
        :param role_name: name of the role to attach the policy to.
        :return: role arn.
        """
        print(f'Attaching s3 readonly policy to role {role_name} ')
        policy_attach_response = self.__iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        print(f"Policy attach response status: {policy_attach_response['ResponseMetadata']['HTTPStatusCode']}")
        return self.__iam.get_role(RoleName=role_name)['Role']['Arn']

    def __create_redshift_cluster(self, roleArns: List[str]):
        """
        The function creates a new redshift cluster based on the configs provided in the class constructor.
        :param roleArns: arns for the roles that need to be passed to the cluster.
        """
        print('Creating Redshift cluster. It might take some time. Timeout: 10 minutes')
        try:
            self.__redshift.create_cluster(
                # adding parameters for hardware
                ClusterType=self.dwh_config.get('DWH_CLUSTER_TYPE'),
                Port=int(self.dwh_config.get('DWH_PORT')),
                NumberOfNodes=int(self.dwh_config.get('DWH_NUM_NODES')),
                NodeType=self.dwh_config.get('DWH_NODE_TYPE'),
                # adding parameters for identifiers & credentials
                DBName=self.dwh_config.get('DWH_DB'),
                ClusterIdentifier=self.dwh_config.get('DWH_CLUSTER_IDENTIFIER'),
                MasterUsername=self.dwh_config.get('DWH_DB_USER'),
                MasterUserPassword=self.dwh_config.get('DWH_DB_PASSWORD'),
                # adding parameter for role (to allow s3 access)
                IamRoles=roleArns
            )

            self.__wait_for_redshift_cluster_creation()

        except Exception as e:
            print(e)
            raise

    def __wait_for_redshift_cluster_creation(self):
        """
        The function makes sure that the redshift cluster is created by sending requests to aws and waiting for an
        acceptable status to be returned for the cluster. The request is sent with a certain configured interval
        (default is 60 sec) and has a timeout (default is 10 minutes)
        """
        cluster_creation_checks = 0
        print('Waiting for cluster to be created')
        while True:
            time.sleep(self.__redshift_cluster_creation_waiting_period)
            cluster_creation_checks = cluster_creation_checks + 1
            cluster_status = self.__redshift.describe_clusters(
                ClusterIdentifier=self.dwh_config.get('DWH_CLUSTER_IDENTIFIER'))['Clusters'][0]['ClusterStatus']
            if cluster_status in self.__redshift_cluster_failed_statuses:
                print(
                    f"What The failure: cluster {self.dwh_config.get('DWH_CLUSTER_IDENTIFIER')} status {cluster_status}")
                raise Exception(f"""
                                cluster {self.dwh_config.get('DWH_CLUSTER_IDENTIFIER')}
                                failed with the status {cluster_status}""")
            if cluster_status not in self.__redshift_cluster_acceptable_statuses:
                print(f'The cluster is in the {cluster_status} status. Waiting 60 seconds...')
                if (
                        self.__redshift_cluster_creation_waiting_period * cluster_creation_checks) >= self.__redshift_cluster_creation_timeout:
                    raise Exception(f"""
                                    Creation period for the {self.dwh_config.get('DWH_CLUSTER_IDENTIFIER')} 
                                    exceeded {self.__redshift_cluster_creation_timeout}.
                                    Please check the cluster status manually, there may be an issue.                            
                                """)
            else:
                print(f"cluster: {self.dwh_config.get('DWH_CLUSTER_IDENTIFIER')}. Status is: {cluster_status}")
                return

    def __allow_ingress(self):
        """
        The function creates a new ingress rule for the default security group to allow inbound connections to the
        redshift cluster.
        """
        try:
            cluster_vpc_id = self.__redshift.describe_clusters(
                ClusterIdentifier=self.dwh_config.get('DWH_CLUSTER_IDENTIFIER'))['Clusters'][0]['VpcId']

            vpc = self.__ec2.Vpc(id=cluster_vpc_id)
            default_sg = list(vpc.security_groups.all())[0]

            default_sg.authorize_ingress(
                GroupName='default',
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(self.dwh_config.get('DWH_PORT')),
                ToPort=int(self.dwh_config.get('DWH_PORT'))
            )
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'InvalidPermission.Duplicate':
                print("The rule for the TCP ingress for the provided peer already exists.")
        except Exception as e:
            print(e)
            raise


def main():
    """
    The main method to run in order to create Redshift cluster infrastructure.
    """
    print('Creating infrastructure')
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    infra_manager = InfrastructureManager(aws_config=config['AWS'], dwh_config=config['DWH'])
    infra_manager.create_infrastructure()
    print('Infrastructure created')


if __name__ == '__main__':
    main()
