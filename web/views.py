# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile
import logging
import io

dynamodb = boto3.client('dynamodb')
sqs = boto3.client('sqs')
s3 = boto3.client('s3')


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  logging.basicConfig(level=logging.DEBUG)

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))
  logging.debug(f"Received bucket: {bucket_name}, key: {s3_key}")

  # Extract the job ID from the S3 key
  #{txin}/{user_id}/{job_id}~{filename}
  parts = s3_key.split('/')
  #{job_id}~{filename}
  unique_filename = parts[2]
  job_id, input_file_name = unique_filename.split('~')
  user_id = session['primary_identity']
  logging.debug(f"Extracted job_id: {job_id}, input_file_name: {input_file_name}, user_id: {user_id}")
  profile = get_profile(identity_id=session.get('primary_identity'))
  user_email = profile.email
  print(user_email)

  # Persist job to database
  # Move your code here...
  dynamodb = boto3.resource('dynamodb')
  table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
  table = dynamodb.Table(table_name)

  item = {
        "job_id": job_id,
        "user_id": user_id, 
        'input_file_name': input_file_name, 
        's3_inputs_bucket': bucket_name, 
        's3_key_input_file': s3_key, 
        'submit_time': int(time.time()), 
        'job_status': 'PENDING',
        'email': user_email,
        "user_status": session['role']
    }
  
  table.put_item(Item=item)
  

  # Send message to request queue
  # Move your code here...
  sns = boto3.client('sns')
  try:

      sns.publish(
            TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'],
            Message=json.dumps(item),
            Subject='New Annotation Job'
        )
        
  except ClientError as e: # Topic not found
        code = e.response['Error']['Code']
        if code == 'NotFound': 
            abort(404) 
        else: 
            abort(500)       

  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():

  # Get list of annotations to display
  # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SQLtoNoSQL.ReadData.Query.html#SQLtoNoSQL.ReadData.Query.DynamoDB
  table = boto3.resource('dynamodb').Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

  try:
        response = table.query(
            IndexName='user_id-index',
            KeyConditionExpression="user_id = :u",
            ExpressionAttributeValues={":u": session['primary_identity']}
        )
        jobs = response.get('Items', [])
  except ClientError as e:
        app.logger.error(f"Unable to query jobs: {e}")
        return abort(500)

   
  formatted_jobs = []
  for job in jobs:
        submit_time = job.get('submit_time', None)
        if submit_time is not None:
           submit_time = datetime.fromtimestamp(submit_time).strftime('%Y-%m-%d %H:%M:%S')
        else:
            submit_time = 'Not Available'

        formatted_jobs.append({
            'job_id': job['job_id'],
            'submit_time': submit_time,
            'input_file_name': job['input_file_name'],
            'job_status': job['job_status']
        })

  
  return render_template('annotations.html', annotations=formatted_jobs)

def create_presigned_url(bucket_name, object_name,
                         expiration=app.config['AWS_SIGNED_REQUEST_EXPIRATION']):

    """Creates a presigned url given the bucket and object names
       https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html"""

    s3 = boto3.client('s3')
    try:
        response = s3.generate_presigned_url('get_object',
                                             Params={'Bucket': bucket_name,
                                                     'Key': object_name},
                                             ExpiresIn=expiration)
        return response

    except ClientError as e:
        logging.error(e)
        return None

   
"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  key = app.config["AWS_DYNAMODB_PRIMARY_KEY"]
  table = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]
  expired = False 
  restore_message = False

  try:
        db = boto3.resource('dynamodb')
        table = db.Table(table)
        results = table.query(
            KeyConditionExpression=Key(key).eq(id)
        )
  except ClientError as e:
        logging.error(e)
        code = e.response['ResponseMetadata']['HTTPStatusCode']
        abort(code)
  
  try:
        info = results["Items"][0]
  except IndexError:
        # No items were returned
        abort(404)

  # Check that this user is authorized
  if info["user_id"] != session["primary_identity"]:
        abort(403)
  
  # Generate Presigned URL so user can download input and results file

  try:
          info['input_file_url'] = create_presigned_url(info['s3_inputs_bucket'],
                                                        info['s3_key_input_file'])
  except ClientError as e:
          code = e.response['ResponseMetadata']['HTTPStatusCode']
          abort(code)
  
  # Handle result file URL based on file status
  if 's3_key_result_file' in info:
        try:

          if info['job_status'] == "COMPLETED":
              info['result_file_url'] = create_presigned_url(info['s3_results_bucket'],
                                                            info['s3_key_result_file'])
        except ClientError as e:
            abort(e.response['ResponseMetadata']['HTTPStatusCode'])
  elif 's3_key_result_file' not in info and session['role'] == "premium_user":
      restore_message = True
      info['restore_message'] = "This file is currently being unarchived and should be available within several hours. Please check back later."
    
          

  # Convert to local time
  info["submit_time"] = time.strftime('%Y-%m-%d %H:%M',
                                        time.localtime(info["submit_time"]))

  if info['job_status'] == "COMPLETED":
        info["complete_time"] = time.strftime('%Y-%m-%d %H:%M',
                                              time.localtime(info["complete_time"]))

  # Check user's status and whether they can view file
  
  if info['job_status'] == "COMPLETED":
        # Convert complete_time from string to Unix timestamp
        complete_time_dt = datetime.strptime(info["complete_time"], '%Y-%m-%d %H:%M')
        complete_time_timestamp = time.mktime(complete_time_dt.timetuple())

        if session['role'] == "free_user" and ((time.time()) - complete_time_timestamp >= app.config['FREE_USER_DATA_RETENTION']):
            expired = True

  # Render HTML page
  return render_template('annotation_details.html', annotation=info, free_access_expired=expired)

"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  key = app.config["AWS_DYNAMODB_PRIMARY_KEY"]
  table = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]

  try:
      db = boto3.resource('dynamodb')
      table = db.Table(table)
      results = table.query(
            KeyConditionExpression=Key(key).eq(id)
        )
  except ClientError as e:
        logging.error(e)
        code = e.response['ResponseMetadata']['HTTPStatusCode']
        abort(code)

  try:
      info = results["Items"][0]
          
  except IndexError:
     abort(404)
  
  # Check that this user is authorized
  if info["user_id"] != session["primary_identity"]:
        abort(403)

  try:
        # Read the log file directly from S3 and load it into memory
        # https://stackoverflow.com/a/48696641
        s3 = boto3.resource('s3')
        s3_results_bucket = s3.Bucket(app.config["AWS_S3_RESULTS_BUCKET"])
        log_file = s3_results_bucket.Object(info['s3_key_log_file'])
        # Read it into a ByteIO object
        raw_input = io.BytesIO(log_file.get()['Body'].read())

  except ClientError as e:
        code = e.response['ResponseMetadata']['HTTPStatusCode']
        abort(code)
  
  # Render the template with decoded text
    # https://docs.python.org/3/library/stdtypes.html#bytes.decode
  return render_template('view_log.html', job_id=id,
                           log_file_contents=raw_input.read().decode("utf-8"))
  


"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!
    sqs.send_message( 
            QueueUrl=app.config['AWS_SQS_RESTORE_QUEUE_URL'], 
            MessageBody=str({
                'user_id': session['primary_identity']})
        )

    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF
