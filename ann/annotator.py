import subprocess
import uuid
import os
import json
import boto3
from botocore.exceptions import ClientError
from configparser import ConfigParser, NoSectionError, NoOptionError

config = ConfigParser()
try:
    config_file_path = '/home/ec2-user/mpcs-cc/gas/ann/ann_config.ini'
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"Configuration file '{config_file_path}' not found.")
    
    config.read(config_file_path)
    
    # SQS and DynamoDB configuration
    QUEUE_NAME = config.get('sqs', 'queue_name')
    TABLE_NAME = config.get('dynamodb', 'table_name')

    # Directory for storing job data and logs
    JOB_DATA_DIR = config.get('paths', 'job_data_dir')
    os.makedirs(JOB_DATA_DIR, exist_ok=True)

except (FileNotFoundError, NoSectionError, NoOptionError) as e:
    print(f"Error reading configuration: {e}")
    exit(1)

try:
    # DynamoDB client setup
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_NAME)

    # Setup AWS SQS and S3 clients
    sqs = boto3.client('sqs')
    s3 = boto3.client('s3')
except ClientError as e:
    print(f"Error setting up AWS clients: {e}")
    exit(1)


def get_queue_url(queue_name):
    #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/get_queue_url.html
    response = sqs.get_queue_url(QueueName=queue_name)
    return response['QueueUrl']

def get_job_dir(job_id):
    """Return the directory path for a given job_id."""
    return os.path.join(JOB_DATA_DIR, job_id)

def ensure_job_dir_exists(job_id):
    """Ensure that a directory exists for the given job_id."""
    job_dir = get_job_dir(job_id)
    os.makedirs(job_dir, exist_ok=True)
    return job_dir

def get_job_file_path(job_id, file_extension):
    """Return the file path within the job directory for a given file extension."""
    job_dir = ensure_job_dir_exists(job_id)
    return os.path.join(job_dir, f"{job_id}{file_extension}")

queue_url = get_queue_url(QUEUE_NAME)

# Poll the message queue in a loop
#https://stackoverflow.com/questions/63601696/constantly-polling-sqs-queue-using-infinite-loop
while True:
    messages = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20  # Enable long polling
    )

    if 'Messages' in messages:
        message = messages['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        body = json.loads(message['Body'])

        try:
            if 'Message' in body:
                nested_message = json.loads(body['Message'])
                job_id = nested_message['job_id']
                s3_inputs_bucket = nested_message['s3_inputs_bucket']
                s3_key_input_file = nested_message['s3_key_input_file']
                input_file_name = nested_message['input_file_name']
                user_id = nested_message['user_id']
                email = nested_message['email']
                user_status = nested_message['user_status']
            else:
                job_id = body['job_id']
                s3_inputs_bucket = body['s3_inputs_bucket']
                s3_key_input_file = body['s3_key_input_file']
                input_file_name = body['input_file_name']
                user_id = body['user_id']
                email = body['email']
                user_status = body['user_status']

            local_filename = get_job_file_path(job_id, '.vcf')
            s3.download_file(s3_inputs_bucket, s3_key_input_file, local_filename)

            try:
                # Launch annotation job as a background process
                subprocess.Popen(['python', './run.py', local_filename, '--job_id', job_id, 
                                  '--user_id', user_id, '--input_file_name', input_file_name, 
                                  '--email', email, '--user_status', user_status],
                                stdout=open(get_job_file_path(job_id, '.vcf.log'), 'w'),
                                stderr=subprocess.STDOUT)
            except Exception as e:
                print("An error occurred:", str(e))

            try:
                # Update job status to RUNNING in the DynamoDB
                table.update_item(
                    Key={'job_id': job_id},
                    UpdateExpression='SET job_status = :new_status',
                    ConditionExpression='job_status = :expected_status',
                    ExpressionAttributeValues={
                        ':new_status': 'RUNNING',
                        ':expected_status': 'PENDING'
                    }
                )
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    print("Condition check failed:", e.response['Error']['Message'])
                else:
                    print("DynamoDB ClientError:", e.response['Error']['Message'])
            except Exception as e:
                print("An unexpected error occurred:", str(e))

            try:
                # Delete the message from the queue
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            except ClientError as e:
                print("Failed to delete message:", e.response['Error']['Message'])
            except Exception as e:
                print("An unexpected error occurred:", str(e))

        except KeyError as e:
            print(f"Key error: {e} - possibly due to missing data in message")
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e} - check the format of the 'Message' string")