# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import boto3
import ast
from botocore import exceptions
from configparser import ConfigParser
import json

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
CONFIG_FILE = '/home/ec2-user/mpcs-cc/gas/util/archive/archive_config.ini'
config = ConfigParser()
config.read_file(open(CONFIG_FILE))

s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')
sqs = boto3.client('sqs')
glacier = boto3.client('glacier')
# Add utility code here

def main(): 
    print('... checking for files to move to glacier ...')
    queue_url = config.get('aws', 'SQSArchiveQueueUrl')

    while True:
        try:
            msgs = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                MessageAttributeNames=['All']
            )
            
            if 'Messages' not in msgs:
                continue
        
            message = msgs['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            body = message.get('Body', "{}")
            
            try:
                # First decode to get the outer SNS-to-SQS JSON
                sns_message = json.loads(body)
                # Now decode the 'Message' field to get your original JSON
                inner_message = json.loads(sns_message['Message'])
            except json.JSONDecodeError as e:
                print(f"Failed to decode JSON from the message body: {body} with error {e}")
                delete_message(receipt_handle)  # Optionally delete the message to prevent reprocessing
                continue

            job_id = inner_message.get('job_id')
            user_id = inner_message.get('user_id')
            s3_key_result_file = inner_message.get('s3_key_result_file')

            print(f"Processing job ID: {job_id}")
            print(f"Processing user ID: {user_id}")
            print(f"Processing s3_key_result_file: {s3_key_result_file}")
    
            _, _, _, _, role, _, _ = helpers.get_user_profile(id=user_id) 
            print(role)
            if role == 'premium_user': 
                delete_message(receipt_handle)
                continue

         # Get S3 results file
            response = s3.get_object(
                Bucket=config.get('aws', 'ResultsBucket'),
                Key=s3_key_result_file)
            obj = response['Body'].read()


            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.upload_archive
            response = glacier.upload_archive(
                vaultName=config.get('aws', 'S3GlacierBucketName'),
                body = obj)
            location, archive_id = response['location'], response['archiveId']

        # Update dynamodb with Glacier Archive Id                 
            dynamodb.update_item(
                TableName=config.get('aws', 'DynamoDBTable'), 
                Key={'job_id': {'S': job_id}}, 
                ExpressionAttributeValues={
                    ':id': {'S': archive_id}
                }, 
                UpdateExpression='SET results_file_archive_id = :id REMOVE s3_key_result_file'
            )
        
        
        # Remove results file from S3
            s3.delete_object(
                Bucket=config.get('aws', 'ResultsBucket'),
                Key=s3_key_result_file)
        
            delete_message(receipt_handle)

        except KeyError as e:
                print(f"Key error: {e} - possibly due to missing data in message")
        except exceptions.ClientError as e:
                print(f"AWS service error: {e}")
        except Exception as e:
                print(f"Unhandled exception: {e}")


def delete_message(receipt_handle): 
    try: # Delete message from SQS queue
        response = sqs.delete_message(
            QueueUrl=config.get('aws', 'SQSArchiveQueueUrl'), 
            ReceiptHandle=receipt_handle)
    except exceptions.ClientError as e: 
        code = e.response['Error']['Code']
        if code == 'ReceiptHandleIsInvalid': 
            print({
                'code': 500, 
                'status': 'Server Error', 
                'message': f'Receipt Handle Is Invalid: {e}',
            }) 
        else: 
            print({
                'code': 500, 
                'status': 'Server Error', 
                'message': f'An error occurred: {e}',
            }) 

if __name__ == '__main__': 
    main()
### EOF