# Toothbrush Sales Project

This project uses fictitious data of a toothbrush company to create an ETL pipeline on AWS to automate the preparation of raw data into a clean format in an RDS database ready for analysis. Data analysis is also performed on the data to reveal insights about the client's sales, customer demographics and customer purchasing habits.

**Note**: This is a case study based on fictitious data.

## Contents
- [Overview](#overview)
- [Dependencies](#dependencies)
- [Set Up](#set-up)
- [How to run](#how-to-run)
- [Other Notes](#other-notes)


## Overview

1. To emulate a client's regular stream of customer data, an SFTP server is set up on an EC2 instance. This runs a CRON job which runs a `.py` script to generate the fictitious data daily.
2. A separate EC2 instance (representing a non-client environment) is used to run a CRON job to retrieve, process, and upload the client's data to an S3 bucket.
3. A lambda function with an S3 trigger automatically loads any new data to an RDS instance with a MySQL database.
4. Data analysis is performed in a Jupyter Notebook to reveal orders and customer insights.

Further information can be found in the `toothbrush_sales.pptx` powerpoint.


## Dependencies

- This project uses a MySQL Community 5.7.33 db engine version. 
- The SFTP server EC2 instance uses an Ubuntu distribution. 
- The processing EC2 instance uses a Linux distribution.

## Set Up
1. Run the stored procedures in `stored_procedures.sql` separately in MySQL Workbench.
2. Set up an ec2 instance as a SFTP server and copy the data folder and 
3. Add the appropiate connection details in the ec2_config.ini file if running seaparately.
4. Have an S3 bucket to get and upload files into.
5. Deploy the lambda functions `lambda_start_instances.py` and `lambda_stop_instances.py` separately.
6. Deploy `lambda_s3_trigger.py` with an trigger for your designated S3 bucket.
7. Add the `triggers.sql` trigger to MySQL Workbench.

## How to Run

1. Create an Amazon EventBridge rule that triggers the `lambda_start_instances.py` lambda function on a time schedule
2. Create an Amazon EventBridge rule that triggers the `lambda_stop_instances.py` lambda function scheduled at least 30 mins after the previous rule
3. Create the following CRON job on the SFTP server EC2 instance, replacing the time to any appropiate time frame:
    ```
    5 17 * * * cd /var/sftp/myfolder; sudo python3 generate_toothbrush_data.py
    ```
    Make sure the data folder is in the same directory as `generate_toothbrush_data.py`
4. Create the following CRON job on the processing EC2 instance, replacing the time to any appropiate time frame:
    ```
    20 17 * * * aws ec2 describe-instances --instance-ids i-04fb6ed09cf5848db --query 'Reservations[*].Instances[*].PublicIpAddress' --output text | xargs python3 sftp_to_s3_processor.py >> log.txt
    ```
    `sftp_to_s3_processor.py` can also be run manually.

## Other Notes
I have made some modifications to the generate_toothbrush_data.py we were provided with as there were a few errors that were raised when changing `full_dumps=False` after running the script with `full_dumps=True` once. These modifications are indicated as comments in `generate_toothbrush_data_modified.py` that start with `#tracey:`.

The data anlysis is done locally by grabbing the data from the MySQL RDS database. Make sure the instance is running when doing this. 
