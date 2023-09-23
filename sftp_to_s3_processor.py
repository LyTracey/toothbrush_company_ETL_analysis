#!/usr/bin/python3
import boto3
from configparser import ConfigParser
import pymysql
import os
import paramiko
import sys
import pandas as pd
import re
from datetime import datetime

def main():

    # Open connectors
    db_connector, s3, ssh_client, bucket = open_connectors()

    # Download new order files on sftp server
    get_new_sftp_files(db_connector, s3, ssh_client)

    # Clean data
    orders_files, clean_file = data_cleansing()
    
    # Upload files to S3 bucket and clear local environment
    upload_files(orders_files, clean_file, s3, db_connector, bucket)

    # Close connectors
    ssh_client.close()
    db_connector.close()
    s3.close()


def open_connectors():
    # Set up config parser
    parser = ConfigParser()
    parser.read("ec2_config.ini")

    # Set ec2 params
    ec2_params = parser["ec2"]

    # Connect to rds
    db_params = parser["mysql"]

    db_connector = pymysql.connect(
        host=db_params["host"],
        user=db_params["user"],
        password=db_params["password"],
        database=db_params["database"],
        autocommit=True,
    )

    # Connect to S3 bucket
    s3 = boto3.client(
        "s3",
        aws_access_key_id=ec2_params["aws_access_key_id"],
        aws_secret_access_key=ec2_params["aws_secret_access_key"],
    )
    s3_params = parser["s3"]
    bucket = s3_params["bucket"]

    # Connect to SFTP server
    sftp_params = parser["sftp"]
    sftp_hostname = sys.argv[1]

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(
        hostname=sftp_hostname,
        username=sftp_params["user"],
        password=sftp_params["password"],
    )

    print("Connections successfully established")
    
    # Return connection objects
    return db_connector, s3, ssh_client, bucket


def get_new_sftp_files(db_connector, s3, ssh_client):

    cursor = db_connector.cursor()
    sftp = ssh_client.open_sftp()
    sftp.chdir("./")

    # Get sftp files
    sftp_files = sftp.listdir()
    
    # Get files already loaded into db Orders table from download_to_s3_queue db table
    cursor.execute("SELECT file_name FROM Download_to_s3_queue;")
    processed_files = cursor.fetchall()
    processed_files = [x[0] for x in processed_files]

    # Store local list of files currently being processed
    unprocessed_files = []

    # Iterate over sftp server files and download locally if not in download_to_s3_queue db table
    for file in sftp_files:
        
        extension = file.split(".")[-1]

        # Get files already loaded to the db
        if (
            (file not in processed_files)
            and (extension == "csv")
            and (file.startswith("order"))
        ):
            try:
                cursor.execute("CALL insert_log(%s, %s);", [file, 1])
                file_id = cursor.fetchone()[0]

                # Downloading from SFTP
                print(f"Downloading {file} from the SFTP server...")
                filepath = "./tmp/" + file
                sftp.get(file, filepath)

                # Update log on database
                cursor.execute("CALL update_log(%s, %s);", [file_id, 2])
                
                # Append file name to unprocessed files
                unprocessed_files.append((file, file_id))

            except:
                # If download is unsuccessful, update log to status_id 6 (Error)
                cursor.execute("CALL update_log(%s, %s);", [6, file_id])
                print(f"ERROR. {file} failed to download.")
        else:
            print(f"ERROR {file} already in S3 bucket or is not an orders file. Skipping...")
    
    cursor.close()
    
    return "New files downloaded from SFTP server."

def postcode_formatter(string):
    pattern = r"^([A-Z0-9]+) ([A-Z0-9]{3})$"
    if not re.match(pattern, string):
        postcode = re.sub(r" +", "", string).upper()
        return f"{postcode[:-3]} {postcode[-3:]}"
    return string

def data_cleansing():

    # Collate current orders data files not currently processed
    orders_files = [
        file for file in os.listdir("./tmp/") if file.startswith("order")
    ]

    if orders_files:
        Orders = pd.concat(
            map(lambda x: pd.read_csv(f"./tmp/{x}", na_values=["NaT", "NaN"]), orders_files),
            ignore_index=True,
        )

        # Remove rows with nans
        Orders.dropna(inplace=True, how="any")

        # Set index
        Orders.set_index("Order Number", inplace=True)

        # Change %20 to space
        Orders["Delivery Postcode"] = Orders["Delivery Postcode"].str.replace("%20", " ")
        Orders["Billing Postcode"] = Orders["Billing Postcode"].str.replace("%20", " ")

        # Format Delivery Postcodes
        Orders["Delivery Postcode"] = Orders["Delivery Postcode"].apply(postcode_formatter)
        Orders["Billing Postcode"] = Orders["Billing Postcode"].apply(postcode_formatter)

        # Change date cols to datetime format
        date_cols = ["Order Date", "Dispatched Date", "Delivery Date"]
        Orders[date_cols] = Orders[date_cols].apply(pd.to_datetime, errors="coerce")

        # Remove rows where customer age < 11
        Orders = Orders[(Orders["Customer Age"] >= 11) & (Orders["Customer Age"] <= 120)]

        # Write clean df to csv
        now = datetime.now().strftime(format="%Y%m%d_%H%M")
        clean_file = f"Orders_data_clean_{now}.csv"
        Orders.to_csv("./tmp/" + clean_file)
    
    return orders_files, clean_file

def upload_files(orders_files, clean_file, s3, db_connector, bucket):
    
    cursor = db_connector.cursor()

    # Upload raw data to raw_data file in S3 bucket
    for file in orders_files:
        s3.upload_file(f"./tmp/{file}", Bucket=bucket, Key=f"raw/{file}")

    # Upload clean data to S3
    print("Uploading {clean_file} to S3 bucket {bucket}...")
    s3.upload_file(f"./tmp/{clean_file}", Bucket=bucket, Key=clean_file)

    # Update log
    cursor.execute("CALL insert_log(%s, %s);", [clean_file, 4])

    # Clean local evironment
    for file in [*orders_files, clean_file]:

        # Deleting local files
        print(f"Removing {file} from the local machine...")
        os.remove("./tmp/" + file)

        # Complete 
        print(
            f"Complete. {file} was successfully removed from the local environment."
        )


if __name__ == "__main__":
    
    print(datetime.now())

    # Check if lock file present
    if not os.path.exists("./tmp/lock_file.txt"):

        print("Lock file doesn't exist. Creating lock file and running script...")

        # Create lock file
        open("./tmp/lock_file.txt", "w").close()

        # Run script
        main()

        # Remove lock file
        os.remove("./tmp/lock_file.txt")

    else:
        print("Lock file exists. Terminating script...")

    
