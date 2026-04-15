import boto3


GLUE_JOB_NAME = "process_reviews_job"

def lambda_handler(event, context):
    """
    This Lambda function is triggered by an S3 event and starts a Glue ETL job.
    """
    glue_client = boto3.client('glue')

    try:
        print(f"Starting AWS Glue job: {process_reviews_job}")
        response = glue_client.start_job_run(JobName=process_reviews_job)
        print(f"Successfully started job run. Run ID: {response['JobRunId']}")
        return {
            'statusCode': 200,
            'body': f"Glue job {process_reviews_job} started successfully."
        }
    except Exception as e:
        print(f"Error starting Glue job: {e}")
        raise e

lambda function
