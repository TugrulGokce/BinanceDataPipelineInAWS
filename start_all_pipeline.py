import io
import time
import zipfile
from datetime import datetime
from time import sleep
import json
from random import choice
import string
import sys
import boto3
from loguru import logger
import base64
import mysql.connector


def create_random_string():
    letters = string.ascii_lowercase
    return ''.join(choice(letters) for _ in range(10))


def zip_it_file(zip_name, file_name):
    zipped_file_name = f"{zip_name}_{create_random_string()}.zip"

    with zipfile.ZipFile(zipped_file_name, "w") as z:
        z.write(file_name)
        z.close()

    with open(zipped_file_name, 'rb') as f:
        zipped_code = f.read()

    return zipped_code


class BinanceDataPipeline:
    def __init__(self):
        self.iam_client = boto3.client('iam')
        self.s3_client = boto3.client('s3', region_name="eu-north-1")
        self.ec2_client = boto3.client('ec2', region_name='eu-north-1')
        self.ec2_resource = boto3.resource('ec2')
        self.lambda_client = boto3.client('lambda', region_name='eu-north-1')
        self.rds_client = boto3.client('rds', region_name="eu-north-1")
        self.sg_name = f'binance-sg-{create_random_string()}'
        self.db_username = f'user_{create_random_string()}'
        self.db_password = create_random_string()
        self.db_cluster_identifier = f'binance-cluster-{create_random_string()}'
        self.db_instance_identifier = f'binance-instance-{create_random_string()}'
        self.db_name = 'binance'

    def create_s3_bucket(self):
        bucket_name = f"binancedatabucket{create_random_string()}"
        response = self.s3_client.create_bucket(Bucket=bucket_name,
                                                CreateBucketConfiguration={
                                                    'LocationConstraint': 'eu-north-1'
                                                })

        logger.success(f"Created S3 Bucket named with '{bucket_name}' ")

        # Create a sub folder in bucket
        self.s3_client.put_object(Bucket=bucket_name, Key='data_finished/')
        logger.success(f"Created sub folder 'data_finished/' in '{bucket_name}'")

    def find_latest_created_s3_bucket(self):
        # List buckets
        buckets = self.s3_client.list_buckets()

        # Find latest created bucket
        latest_bucket_name = None
        latest_date = datetime.min.replace(tzinfo=None)
        for bucket in buckets['Buckets']:
            created_date = bucket['CreationDate'].replace(tzinfo=None)
            if created_date > latest_date:
                latest_date = created_date
                latest_bucket_name = bucket['Name']

        return latest_bucket_name

    def create_vpc(self):
        vpc = self.ec2_client.create_vpc(
            CidrBlock='10.0.0.0/16'
        )

        # Wait for the VPC to be available
        waiter = self.ec2_client.get_waiter('vpc_available')
        waiter.wait(VpcIds=[vpc['Vpc']['VpcId']])

        logger.info("Created VPC with ID: {}", vpc['Vpc']['VpcId'])

        self.ec2_client.create_tags(Resources=[vpc['Vpc']['VpcId']], Tags=[{'Key': 'Name', 'Value': 'sto_vpc'}])

        # Enable DNS support for the VPC
        self.ec2_client.modify_vpc_attribute(
            VpcId=vpc['Vpc']['VpcId'],
            EnableDnsSupport={'Value': True}
        )

        # Enable DNS hostnames for the VPC
        # Enable DNS hostnames for the VPC
        self.ec2_client.modify_vpc_attribute(
            VpcId=vpc['Vpc']['VpcId'],
            EnableDnsHostnames={'Value': True}
        )

        # Create an Internet Gateway
        igw = self.ec2_client.create_internet_gateway()

        logger.info("Created Internet Gateway with ID: {}", igw['InternetGateway']['InternetGatewayId'])

        # Attach the Internet Gateway to the VPC
        self.ec2_client.attach_internet_gateway(
            InternetGatewayId=igw['InternetGateway']['InternetGatewayId'],
            VpcId=vpc['Vpc']['VpcId']
        )

        # Create three subnets in the VPC
        subnet1 = self.ec2_client.create_subnet(
            VpcId=vpc['Vpc']['VpcId'],
            CidrBlock='10.0.0.0/24',
            AvailabilityZone='eu-north-1a'
        )

        waiter = self.ec2_client.get_waiter('subnet_available')
        waiter.wait(SubnetIds=[subnet1['Subnet']['SubnetId']])

        logger.info("Created Subnet 1 with ID: {}", subnet1['Subnet']['SubnetId'])

        subnet2 = self.ec2_client.create_subnet(
            VpcId=vpc['Vpc']['VpcId'],
            CidrBlock='10.0.1.0/24',
            AvailabilityZone='eu-north-1b'
        )

        waiter.wait(SubnetIds=[subnet2['Subnet']['SubnetId']])

        logger.info("Created Subnet 2 with ID: {}", subnet2['Subnet']['SubnetId'])

        subnet3 = self.ec2_client.create_subnet(
            VpcId=vpc['Vpc']['VpcId'],
            CidrBlock='10.0.2.0/24',
            AvailabilityZone='eu-north-1c'
        )

        waiter.wait(SubnetIds=[subnet3['Subnet']['SubnetId']])

        logger.info("Created Subnet 3 with ID: {}", subnet3['Subnet']['SubnetId'])

        # Create a route table for the VPC
        route_table = self.ec2_client.create_route_table(VpcId=vpc['Vpc']['VpcId'])

        s3_endpoint = self.ec2_client.create_vpc_endpoint(
            VpcEndpointType='Gateway',
            VpcId=vpc['Vpc']['VpcId'],
            ServiceName='com.amazonaws.eu-north-1.s3',
            RouteTableIds=[route_table['RouteTable']['RouteTableId']],
        )

        logger.info("Created S3 Endpoint Gateway with ID: {}", s3_endpoint['VpcEndpoint']['VpcEndpointId'])

        self.ec2_client.create_tags(
            Resources=[s3_endpoint['VpcEndpoint']['VpcEndpointId']],
            Tags=[{'Key': 'Name', 'Value': 's3_endpoint_gateway'}]
        )

        logger.info("Created Route Table with ID: {}", route_table['RouteTable']['RouteTableId'])

        # Create a route to the Internet Gateway in the route table
        self.ec2_client.create_route(
            DestinationCidrBlock='0.0.0.0/0',
            GatewayId=igw['InternetGateway']['InternetGatewayId'],
            RouteTableId=route_table['RouteTable']['RouteTableId']
        )

        logger.info("Created route to Internet Gateway in Route Table")

        # Associate the subnets with the route table
        self.ec2_client.associate_route_table(
            SubnetId=subnet1['Subnet']['SubnetId'],
            RouteTableId=route_table['RouteTable']['RouteTableId']
        )

        self.ec2_client.associate_route_table(
            SubnetId=subnet2['Subnet']['SubnetId'],
            RouteTableId=route_table['RouteTable']['RouteTableId']
        )

        self.ec2_client.associate_route_table(
            SubnetId=subnet3['Subnet']['SubnetId'],
            RouteTableId=route_table['RouteTable']['RouteTableId']
        )

        logger.info("Associated subnets with Route Table")

    def create_role_for_RDS_to_s3_full_access(self):
        # Set a random string RoleName start with 'RDS_to_S3_Role_'
        role_name = f"RDS_to_S3_Role_{create_random_string()}"

        # defining role policy
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": [
                            "rds.amazonaws.com"
                        ]
                    },
                    "Action": [
                        "sts:AssumeRole"
                    ]
                }
            ]
        }

        # creating role to role policy
        response = self.iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='Allows you to grant RDS access to additional resources on your behalf.',
        )

        role_arn_for_rds = response["Role"]["Arn"]

        self.iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess'
        )

        logger.success(f"Created new role for RDS : {role_arn_for_rds}")

        sleep(25)

        return role_arn_for_rds

    def create_role_for_lambda(self):
        role_name = f"LambdaBasicExecutionRole_{create_random_string()}"

        assume_role_policy_document = {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'lambda.amazonaws.com'
                    },
                    'Action': 'sts:AssumeRole'
                }
            ]
        }

        role_response = self.iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy_document),
            Description='Lambda role created by Boto3',
        )

        self.iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        )

        self.iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole'
        )

        self.iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess'
        )

        arn_for_lambda = role_response["Role"]["Arn"]
        logger.info(f"Created new role for Lambda Function = {arn_for_lambda}")

        sleep(20)

        return arn_for_lambda

    def create_role_for_EC2_to_S3_Full_Access(self):
        role_name = f'EC2-S3-Access-Role-{create_random_string()}'

        # Defining role policy
        assume_role_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "sts:AssumeRole"
                    ],
                    "Principal": {
                        "Service": [
                            "ec2.amazonaws.com"
                        ]
                    }
                }
            ]
        }

        # Creating role to role policy
        response = self.iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy),
            Description='Allows EC2 instances to call AWS services on your behalf.',
        )

        # Take role_arn
        ec2_role_arn = response['Role']['Arn']

        # EC2 rolüne S3FullAccess policy'sini ata
        self.iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess'
        )

        logger.info(f"Created new role for EC2 : {ec2_role_arn}")

        # Create instance profile
        instance_profile_name = f'EC2-S3-Access-Instance-Profile-{create_random_string()}'

        instance_profile = self.iam_client.create_instance_profile(
            InstanceProfileName=instance_profile_name,
            Tags=[
                {
                    'Key': 'Description',
                    'Value': 'EC2 instance profile description'
                },
            ]
        )

        # Add the role to instance profile
        self.iam_client.add_role_to_instance_profile(
            InstanceProfileName=instance_profile_name,
            RoleName=role_name
        )
        # Get the instance profile ARN
        instance_profile_arn = instance_profile['InstanceProfile']['Arn']

        logger.info(f"Instance profile '{instance_profile_arn}' associated it with '{ec2_role_arn}'")

        sleep(20)

        return instance_profile_arn

    def subnets_associated_with_VPC(self):
        # Find subnet associated with VPC
        subnet_a = ""
        subnet_b = ""
        all_subnets = self.ec2_client.describe_subnets()
        for subnet in all_subnets["Subnets"]:
            if subnet['CidrBlock'] == "10.0.0.0/24":
                subnet_a = subnet['SubnetId']

            if subnet['CidrBlock'] == "10.0.1.0/24":
                subnet_b = subnet['SubnetId']

        return subnet_a, subnet_b

    def create_new_security_group(self, vpc_id):

        waiter = self.ec2_client.get_waiter('security_group_exists')
        res = self.ec2_client.create_security_group(
            Description='For binance project',
            GroupName=self.sg_name,
            VpcId=vpc_id,
        )

        waiter.wait(GroupIds=[res['GroupId']])
        time.sleep(3)
        return res

    def modify_security_group_rules_for_rds_port(self, vpc_security_group_id):
        security_group = self.ec2_resource.SecurityGroup(vpc_security_group_id)

        ip_permissions = [
            {
                'IpProtocol': 'tcp',
                'FromPort': 63306,
                'ToPort': 63306,
                'IpRanges': [
                    {
                        'CidrIp': '0.0.0.0/0'
                    }
                ]
            },
            # { IF SSH NEEDED
            #     'IpProtocol': 'tcp',
            #     'FromPort': 22,
            #     'ToPort': 22,
            #     'IpRanges': [
            #         {
            #             'CidrIp': '0.0.0.0/0'
            #         }
            #     ]
            # }
        ]

        res = self.ec2_client.authorize_security_group_ingress(
            GroupId=vpc_security_group_id,
            IpPermissions=ip_permissions

        )

        # return security_group.authorize_ingress(IpPermissions=ip_permissions)["SecurityGroupRules"]

    def get_vpc_id(self):
        """
        This function create a new ec2 client and return VpcId
        In this project scope, we are working on just one VPC (VpcName: sto_vpc, IPv4 CIDR Blocks: 10.0.0.0/16)
        !! If you are more than one VPC, you need to configure this function. !!
        :return: Return VpcId
        """
        all_vpcs = self.ec2_client.describe_vpcs()
        return all_vpcs["Vpcs"][0]["VpcId"]

    def find_binance_security_group_id(self):
        parameter_groups_response = self.ec2_client.describe_security_groups(
            Filters=[
                {
                    'Name': 'group-name',
                    'Values': [
                        self.sg_name,
                    ]
                },
            ]
        )

        return parameter_groups_response['SecurityGroups'][0]['GroupId']

    def create_s3_put_trigger(self, bucket_name, prefix, suffix, lambda_arn):

        self.lambda_client.add_permission(
            FunctionName=lambda_arn,
            StatementId=f'lambda-{create_random_string()}',
            Action='lambda:InvokeFunction',
            Principal='s3.amazonaws.com',
            SourceArn=f'arn:aws:s3:::{bucket_name}'
        )

        return self.s3_client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                'LambdaFunctionConfigurations': [
                    {
                        'LambdaFunctionArn': lambda_arn,
                        'Events': ['s3:ObjectCreated:Put'],
                        'Filter': {
                            'Key': {
                                'FilterRules': [
                                    {'Name': 'prefix', 'Value': prefix},
                                    {'Name': 'suffix', 'Value': suffix}
                                ]
                            }
                        }
                    }
                ]
            }
        )

    def create_table_on_rds(self, host_name):
        cnx = mysql.connector.connect(host=host_name, user=self.db_username,
                                      passwd=self.db_password,
                                      database=self.db_name, port="63306")

        create_sql = "create table BTCUSDT ( bid bigint null, parameter char(7) null, price float(7,2) null, " \
                     "quantity float(7,5) null, time datetime null, maker tinyint null ); "

        cur = cnx.cursor()
        cur.execute(create_sql)
        logger.info("Create sql script executed.")
        cnx.commit()
        cnx.close()

        logger.info("Table BTCUSDT created.")

    def create_aurora_mysql_rds(self):
        # Get created VPCId
        vpc_id = self.get_vpc_id()

        # Fetch subnet_ids -> subnet_a (IPv4 CIDR : 10.0.0.0/24), subnet_b(IPv4 CIDR : 10.0.1.0/24)
        subnet_a, subnet_b = self.subnets_associated_with_VPC()

        # Create a new SecurityGroup in VPC
        response_for_new_security_group = self.create_new_security_group(vpc_id)

        # Modify SecurityGroup Inbound Rules for RDS
        self.modify_security_group_rules_for_rds_port(response_for_new_security_group['GroupId'])

        db_subnet_group_name = f"binance-db-subnet-group-{create_random_string()}"
        self.rds_client.create_db_subnet_group(
            DBSubnetGroupName=db_subnet_group_name,
            DBSubnetGroupDescription='binance db subnet group',
            SubnetIds=[
                subnet_a, subnet_b
            ],
            Tags=[
                {
                    'Key': 'Name',
                    'Value': 'binance-db-subnet-group',
                },
            ],
        )

        logger.success(f"DB Subnet Group successfully created : {db_subnet_group_name}")

        cluster_db_response = self.rds_client.create_db_cluster(
            AvailabilityZones=["eu-north-1a", "eu-north-1b"],  # at least 2 AZs
            DatabaseName=self.db_name,
            DBClusterIdentifier=self.db_cluster_identifier,
            VpcSecurityGroupIds=[response_for_new_security_group['GroupId']],
            Engine="aurora-mysql",
            EngineVersion="5.7.mysql_aurora.2.11.1",
            Port=63306,
            MasterUsername=self.db_username,
            MasterUserPassword=self.db_password,
            DBSubnetGroupName=db_subnet_group_name,
            StorageEncrypted=False,
            DeletionProtection=False,
            EnableCloudwatchLogsExports=[
                "audit", "error", "general", "slowquery"
            ]
        )

        waiter = self.rds_client.get_waiter('db_cluster_available')
        waiter.wait(DBClusterIdentifier=self.db_cluster_identifier)
        logger.success(f"RDS Cluster created successfully. {cluster_db_response}")
        db_instance_response = self.rds_client.create_db_instance(
            DBClusterIdentifier=self.db_cluster_identifier,
            DBInstanceIdentifier=self.db_instance_identifier,
            DBInstanceClass="db.t3.small",
            Engine='aurora-mysql',
            EngineVersion='5.7.mysql_aurora.2.11.1',
            DBSubnetGroupName=db_subnet_group_name,
            StorageEncrypted=False,
            PubliclyAccessible=True,
        )

        waiter = self.rds_client.get_waiter('db_instance_available')
        waiter.wait(DBInstanceIdentifier=self.db_instance_identifier)
        logger.success(f"RDS Instance created successfully. {db_instance_response}")

        s3_rds_arn = self.create_role_for_RDS_to_s3_full_access()

        logger.info(f"{s3_rds_arn=}")

        # Adding IAM 'RDS-S3FullAccess' Role to RDS
        self.rds_client.add_role_to_db_cluster(
            DBClusterIdentifier=self.db_cluster_identifier,
            RoleArn=s3_rds_arn,
        )

        logger.info('DBInstance is available now!')

        logger.success(f"Role : {s3_rds_arn} successfully attached to binance RDS Cluster.")

        # Create new cluster parameter group Random olsun
        parameter_group_name = f"binance-cluster-parameter-group-{create_random_string()}"
        self.rds_client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=parameter_group_name,
            DBParameterGroupFamily='aurora-mysql5.7',
            Description='A new cluster parameter group for binance RDS Cluster (S3 Access)'
        )

        logger.info(f"The Cluster Parameter Group named '{parameter_group_name}' has been successfully created.")

        # Modify created ClusterParameterGroup for S3 Access
        self.rds_client.modify_db_cluster_parameter_group(
            DBClusterParameterGroupName=parameter_group_name,
            Parameters=[
                {
                    'ParameterName': 'aurora_load_from_s3_role',
                    'ParameterValue': s3_rds_arn,
                    'ApplyMethod': 'immediate'
                },
                {
                    'ParameterName': 'aurora_select_into_s3_role',
                    'ParameterValue': s3_rds_arn,
                    'ApplyMethod': 'immediate'
                },
                {
                    'ParameterName': 'aws_default_s3_role',
                    'ParameterValue': s3_rds_arn,
                    'ApplyMethod': 'immediate'
                }
            ]
        )

        logger.info(f"'{parameter_group_name}' is modified.")

        # Apply changes.
        self.rds_client.modify_db_cluster(
            DBClusterIdentifier=self.db_cluster_identifier,
            ApplyImmediately=True,
            DBClusterParameterGroupName=parameter_group_name
        )

        sleep(15)

        logger.info(f"The default Parameter Group changed with '{parameter_group_name}'")

        self.rds_client.reboot_db_instance(DBInstanceIdentifier=self.db_instance_identifier)
        db_instance_status = 'rebooting'
        while db_instance_status != 'available':
            response = self.rds_client.describe_db_instances(DBInstanceIdentifier=self.db_instance_identifier)
            db_instance_status = response['DBInstances'][0]['DBInstanceStatus']
            logger.info(f'db_instance_status: {db_instance_status}')
            sleep(10)

        logger.info("DB Instance available now !!!")

        self.create_table_on_rds(cluster_db_response["DBCluster"]['Endpoint'])

        logger.info("RDS configuration done.")

        return cluster_db_response["DBCluster"]

    def create_lambda_on_vpc(self, rds_cluster_response):
        # Fetch subnet_ids -> subnet_a (IPv4 CIDR : 10.0.0.0/24), subnet_b(IPv4 CIDR : 10.0.1.0/24)
        subnet_a, subnet_b = self.subnets_associated_with_VPC()

        # SecurityGroupId of VPC
        security_group_id = self.find_binance_security_group_id()

        # Create a random lambda funtion name
        lambda_funtion_name = f"Lambda_S3_PUT_Trigger_{create_random_string()}"

        # get Lambda Role ARN
        arn_for_lambda = self.create_role_for_lambda()

        response_created_lambda = self.lambda_client.create_function(
            FunctionName=lambda_funtion_name,
            Runtime="python3.9",
            Role=arn_for_lambda,
            Handler='lambda_function.lambda_handler',
            Description='S3 Bucket PUT Trigger to RDS',
            Timeout=30,
            VpcConfig={
                'SubnetIds': [
                    subnet_a, subnet_b
                ],
                'SecurityGroupIds': [
                    security_group_id,
                ]
            },
            Code=dict(ZipFile=zip_it_file(zip_name="lambda_handler", file_name="lambda_function.py")),
            Architectures=['x86_64']
        )

        waiter = self.lambda_client.get_waiter('function_active').wait(FunctionName=lambda_funtion_name)
        logger.success(f"Lambda Function : {lambda_funtion_name} created successfully.")

        lambda_arn = response_created_lambda['FunctionArn']
        # upload the zip file(for lambda function layer -> mysql-connector) and read content
        with open('python.zip', 'rb') as f:
            zip_bytes = io.BytesIO(f.read())

        # Create Lambda Layer
        response_published_layer = self.lambda_client.publish_layer_version(
            LayerName=f'mysql-connector_for_{lambda_funtion_name}',
            Content={
                'ZipFile': zip_bytes.getvalue(),
            },
            CompatibleRuntimes=[
                'python3.9',
            ],
        )

        # Add layer to Lambda Function
        response = self.lambda_client.update_function_configuration(
            FunctionName=lambda_funtion_name,
            Layers=[
                response_published_layer['LayerVersionArn'],
            ],
        )
        waiter_updated = self.lambda_client.get_waiter('function_updated').wait(FunctionName=lambda_funtion_name)

        bucket_name = self.find_latest_created_s3_bucket()

        # Add S3 PUT Trigger to Lambda Function
        put_trigger_response = self.create_s3_put_trigger(bucket_name=bucket_name, prefix="data_1_min/", suffix=".tsv",
                                                          lambda_arn=response_created_lambda['FunctionArn'])
        logger.info("S3 PUT Trigger succesfully adedd.")

        self.lambda_client.update_function_configuration(
            FunctionName=lambda_funtion_name,
            Environment={
                'Variables': {
                    'RDS_HOSTNAME': rds_cluster_response['Endpoint'],
                    'RDS_USERNAME': self.db_username,
                    'RDS_PASSWORD': self.db_password,
                    'RDS_DB_NAME': self.db_name,
                    'RDS_PORT': str(rds_cluster_response['Port']),
                }
            }
        )

        logger.info("Added new Environment Variables to Lambda Function.")
        logger.info("Lambda configuration done.")

    def create_ec2_instance(self):
        # Fetch subnet_ids -> subnet_a (IPv4 CIDR : 10.0.0.0/24), subnet_b(IPv4 CIDR : 10.0.1.0/24)
        subnet_a, _ = self.subnets_associated_with_VPC()

        # SecurityGroupId of VPC
        security_group_id = self.find_binance_security_group_id()

        # Create a new Key Pair
        key_pair_name = f"binance_ec2_{create_random_string()}"
        key_pair = self.ec2_client.create_key_pair(KeyName=key_pair_name)

        # Create special key file (.pem). It will only be displayed once.
        with open(f'{key_pair_name}.pem', 'w') as f:
            f.write(key_pair['KeyMaterial'])

        # Define User Data
        ec2_user_data_text = "#!/bin/bash\ncd /home/ec2-user && curl -o run -L " \
                             "https://raw.githubusercontent.com/sinanartun/binance_4/main/run38.sh && sh run "
        # Convert to byte object
        ec2_user_data_bytes = ec2_user_data_text.encode('utf-8')
        # Convert byte object to base64-encoded format
        ec2_user_data_base_64 = base64.b64encode(ec2_user_data_bytes).decode('utf-8')

        # Block Device Mappings
        block_device_mappings = [
            {
                'DeviceName': '/dev/sda1',  # for t3.micro ınstance type
                'Ebs': {
                    'VolumeSize': 10,
                    'VolumeType': 'gp2'  # Its free tier option
                }
            }
        ]

        instance_profile_arn = self.create_role_for_EC2_to_S3_Full_Access()

        instance_response = self.ec2_client.run_instances(
            ImageId='ami-02d0b04e8c50472ce',  # Indicates Amazon Linux 2 AMI - Kernel 5.10, SSD Volume Type (Free Tier)
            InstanceType='t3.micro',  # Indicates Family:t3, 2vCPU 1GiB Memory (Free Tier)
            MinCount=1,
            MaxCount=1,
            NetworkInterfaces=[
                {
                    'DeviceIndex': 0,
                    'SubnetId': subnet_a,
                    'Groups': [security_group_id],
                    'AssociatePublicIpAddress': True,
                    'DeleteOnTermination': True,
                }
            ],
            BlockDeviceMappings=block_device_mappings,
            KeyName=key_pair_name,
            UserData=ec2_user_data_base_64,
            IamInstanceProfile={'Arn': instance_profile_arn}
        )

        waiter = self.ec2_client.get_waiter('instance_exists').wait(WaiterConfig={'Delay': 5, 'MaxAttempts': 40})

        logger.success("EC2 Instance created successfully.")

        # Instance'a etiket ekleme
        instance_id = instance_response['Instances'][0]['InstanceId']
        response = self.ec2_client.create_tags(
            Resources=[instance_id],
            Tags=[
                {
                    'Key': 'Name',
                    'Value': 'Binance_EC2'
                },
            ]
        )

        logger.info("Added tag to EC2 Instance : Binance_EC2")
        logger.info("EC2 Configuration done.")


if __name__ == '__main__':
    logger.remove()
    logger.add(
        sink=sys.stdout,
        format="<level>{time:HH:mm:ss}</level> | <level>{level: <8}</level> | <level>{message}</level>",
        level="DEBUG"
    )
    logger.info(f"boto3 version: {boto3.__version__}")
    logger.info("Creating Resources")

    binance_object = BinanceDataPipeline()
    binance_object.create_vpc()
    binance_object.create_s3_bucket()
    rds_response = binance_object.create_aurora_mysql_rds()
    binance_object.create_lambda_on_vpc(rds_response)
    binance_object.create_ec2_instance()
    print(f'db_username:{binance_object.db_username}')
    print(f'db_password:{binance_object.db_password}')
