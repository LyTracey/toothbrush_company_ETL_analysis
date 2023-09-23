import boto3
from configparser import ConfigParser

def lambda_handler(event, context):
    
    # Parse config data
    parser = ConfigParser()
    parser.read('ec2_config.ini')
    
    # Configure and start ec2 instances - sftp server and s3 uploader
    ec2_params = parser['ec2']
    
    ec2 = boto3.client(
        'ec2', 
        aws_access_key_id=ec2_params['aws_access_key_id'],
        aws_secret_access_key=ec2_params['aws_secret_access_key'],
        region_name=ec2_params['region_name'])
    
    # Start ec2 instance
    ec2.stop_instances(InstanceIds=["i-04fb6ed09cf5848db", "i-01a25ceeb1d29a741"])
    
    # Configure and start rds instance
    rds = boto3.client(
        'rds',
        aws_access_key_id=ec2_params['aws_access_key_id'],
        aws_secret_access_key=ec2_params['aws_secret_access_key'],
        region_name=ec2_params['region_name'])
    
    rds.stop_db_instance(DBInstanceIdentifier = "my-first-db")
    
    return {
        'statusCode': 200
    }


