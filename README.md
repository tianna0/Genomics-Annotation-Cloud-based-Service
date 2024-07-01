# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

# Archive Process
The archive process is as follows:
- In `ann/run.py` after running the annotation job, as its last step the system checks to see if the user is a `free_user`. If so, then it uses SQS to send a message to the SQS archive queue, otherwise nothing more happens. This queue has a default delay of 5 minutes, so no message will be delivered to it before that time has elapsed.
- The `util/archive/archive.py` script is continuously running and checking the archive queue for any jobs that should be archived. If it receives a message, it checks what type of user does this `user_id` belong to.
  - If the user is a `premium_user` according to the database, the script deletes the message and nothing happens, which accounts for the edge case where a user upgrades from free to premium after they submit an annotation job and before their results are archived.
  - Otherwise the user is a `free_user`, and the script performs the following actions:
    - Get the results file from S3
    - Upload that bytes-file archive to Glacier
    - Update the dynamodb entry for this job to add `results_file_archive_id` and `delete s3_key_result_file`
    - Delete the results file from S3
    - Delete the message from the archive queue

# Restore Process
The restore process is as follows:
- In `web/views.py`, the endpoint `/subscribe` will send a POST request to update the user's profile, which triggers SQS to send a message to the restore queue.
- `util/restore/restore.py` continuously waits to receive messages from this queue, and when it receives one it:
  - If the user is a `free_user`, the message is deleted from the restore queue.
  - If the user is a `premium_user`, then it uses `dynamodb.query()` to get every annotation job record that the user has submitted and that has been archived to glacier. Specifically, these records will not have an `s3_key_result_file` listed, and have a `results_file_archive_id` listed.
  - For each of these `results_file_archive_ids`, we will initiate a job for `archive_retrieval` on glacier.
  - The message is deleted from the restore queue.
- The script `thaw.py` is waiting with the thaw queue which is subscribed to the thaw SNS topic. When it receives a message that a job has been completed, it will:
  - Use the glacier JobId included in the message to receive the bytes-file that was un-archived.
  - Use `dynamodb.scan()` to locate the record where `results_file_archive_id = ArchiveId`, gather several fields from that observation, and properly concatenate them to form the `s3_key_result_file` name.
  - Puts the bytes-file back up to S3 with the `s3_key_result_file name`
  - Updates the corresponding dynamodb record to delete the `results_file_archive_id` field and add the `s3_key_results_file field`.
  - Deletes the message from the thaw queue.
