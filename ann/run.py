import sys
import time
sys.path.append('/home/ec2-user/mpcs-cc/gas/ann/anntools')
import driver
import boto3
import os
from configparser import ConfigParser
from botocore.exceptions import BotoCoreError, ClientError
import json


# Load configuration
config = ConfigParser()
config.read('/home/ec2-user/mpcs-cc/gas/ann/ann_config.ini') 


# Bucket and DynamoDB table names from config file
RESULTS_BUCKET_NAME = config.get('s3', 'results_bucket_name')
TABLE_NAME = config.get('dynamodb', 'table_name')
CNETID = config.get('paths', 'cnetid')
SNS_TOPIC_ARN = config.get('sns', 'topic_arn')


class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")

if __name__ == '__main__':
    # Check if a VCF file name is provided
    if len(sys.argv) > 1:
        # Extract the job_id path removing '.vcf'
        job_id = sys.argv[1][0: -4]
        id = job_id.split('/')[-2]
        fullfilename = sys.argv[7]
        filename = fullfilename.split('.')[0]
        username = sys.argv[5]
        email = sys.argv[9]
        user_status = sys.argv[11]


        with Timer():
            try:
            # Run the AnnTools driver
                driver.run(sys.argv[1], 'vcf')
            except Exception as e:
                print(f"Error processing VCF file: {e}")
                sys.exit(1)

        
        # Initialize S3 client
        s3 = boto3.client('s3')
        # DynamoDB client setup
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(TABLE_NAME)
            
        results_file = f"{job_id}.annot.vcf"
        log_file = f"{job_id}.vcf.count.log"

        results_file_key = f"{CNETID}/{username}/{id}/{filename}.annot.vcf"
        log_file_key = f"{CNETID}/{username}/{id}/{filename}.vcf.count.log"

        try:
            # Upload file to s3
            s3.upload_file(results_file, RESULTS_BUCKET_NAME, results_file_key)
            s3.upload_file(log_file, RESULTS_BUCKET_NAME, log_file_key)
        except (BotoCoreError, ClientError) as error:
                print(f"Error uploading files to S3: {error}")
                sys.exit(1)

        try:
            # Update DynamoDB table
            #https://docs.python.org/3/library/time.html
            completion_time = int(time.time()) 
            table.update_item(
                Key={'job_id': id},
                UpdateExpression='SET s3_key_result_file = :res, s3_results_bucket = :rb, s3_key_log_file = :log, complete_time = :comp, job_status = :status',
                ExpressionAttributeValues={
                    ':res': results_file_key,
                    ':rb': RESULTS_BUCKET_NAME,
                    ':log': log_file_key,
                    ':comp': completion_time,
                    ':status': 'COMPLETED'
                }
            )
        except (BotoCoreError, ClientError) as error:
                print(f"Error Update DynamoDB table: {error}")
                sys.exit(1)

        #publishes a notification to sns when the job is complete
        #The result queue is handled by the lambda function to send out email
        try:
            sns = boto3.client('sns')
            sns_message = {
                 "job_id": id,
                 "email": email,
                 "message": "Your job has completed successfully."
            }
            sns.publish(TopicArn=SNS_TOPIC_ARN, Message=json.dumps(sns_message))
        except (BotoCoreError, ClientError) as error:
                print(f"Error sending SNS notification: {error}")
                sys.exit(1)

        #sends notification to archive queue
        if (user_status =='free_user'):
            sns = boto3.client('sns')
            data_glacier = {
                    'user_id': username, 
                    'job_id': id, 
                    's3_key_result_file': results_file_key
                    }
            
            response = sns.publish(
                    TopicArn=config.get('sns', 'archive_arn'),
                    Message=json.dumps(data_glacier))

        # Delete local files
        os.remove(results_file)
        os.remove(log_file)
      
                
    else:
        print("A valid .vcf file must be provided as input to this program.")