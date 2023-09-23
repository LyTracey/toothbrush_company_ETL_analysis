import urllib.parse
import boto3
from configparser import ConfigParser
import pymysql

def lambda_handler(event, context):
    
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']

    # Get name of uploaded s3 file
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    # Check if it's a Orders_data_clean csv file
    if not str(key).startswith("Orders_data_clean"):
        return {
            "error": "Unexpected file"
        }

    # Parse config details
    parser = ConfigParser()
    parser.read("ec2_config.ini")
    ec2_params = parser["ec2"]

    # Connect to s3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=ec2_params["aws_access_key_id"],
        aws_secret_access_key=ec2_params["aws_secret_access_key"],
        region_name=ec2_params["region_name"]
    )
    
    s3_params = parser["s3"]
    bucket = s3_params["bucket"]
    
    # Connect to rds
    db_params = parser["mysql"]
    
    db_connector = pymysql.connect(
        host=db_params["host"],
        user=db_params["user"],
        password=db_params["password"],
        database=db_params["database"],
        autocommit=True
    )
    
    # Open cursor
    cursor = db_connector.cursor()
    
    # Check if files processed already exists in queue. Check status if yes.
    try:
        cursor.execute("SELECT file_id, status_id FROM Download_to_s3_queue WHERE file_name = %s;", (key))
        response = cursor.fetchone()
        file_id = response[0]
        current_status = response[1]

        print("Checking status")
        if current_status == 4:
            print("Preparing to load into rds...")
        elif current_status == 5:
            print("File already loaded to RDS.")
        else:
            print("File is not uploaded.")
    except:
        print("File does not exist. Creating new log.")
        cursor.execute("CALL insert_log(%s, %s);", [key, 4])
        file_id = cursor.fetchone()[0]
        

    # Get file from s3
    try:
        file = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8').splitlines()
        data = [x.split(",") for x in file]
        data = [list(map(lambda x: None if x=="" else x, row)) for row in data]
    except:
        print("File not found in s3 bucket")
    
    # Create orders table if not exists
    cursor.execute("CALL create_table;")
    
    # Check columns of csv file match
    csv_cols = data[0]
    cursor.execute("CALL get_cols(%s);", ["Orders"])
    table_cols = [x[0] for x in cursor.fetchall()]
    if csv_cols != list(table_cols):
        print("Columns don't match")
        return {
        "error": 400
        }
    
    # Upload rows to mysql
    cursor.executemany("CALL insert_row({}%s);".format("%s,"*11), data[1:])
    
    # Change file status in queue
    cursor.execute("CALL update_log(%s, %s);", [file_id, 5])
    
    # Close connections
    cursor.close()
    db_connector.close()
    
    return {
        "success_code": 200
    }

