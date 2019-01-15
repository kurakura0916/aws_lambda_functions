import logging
import boto3

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

ARN = "arn:aws:batch:ap-northeast-1:hogehoge"


def lambda_handler(event, _context):
    LOGGER.info(event)

    client = boto3.client("batch")
    print("event", event)

    params = get_job_params(event, client)

    client.submit_job(
        jobName=params["JOB_NAME"],
        jobQueue=params["JOB_QUEUE"],
        jobDefinition=params["JOB_DEFINITION"]
    )


def get_revision(client, job_definition_name):
    job_definitions = \
        client.describe_job_definitions()['jobDefinitions']

    revision_num = 1

    for job in job_definitions:
        if job["jobDefinitionName"] == job_definition_name:
            if job["revision"] > revision_num:
                revision_num = job["revision"]

    return revision_num


def get_job_params(event, client):
    params = {}
    job_definition_name = "test-batch-job"
    revision_num = get_revision(client, job_definition_name)

    params["JOB_NAME"] = "test-job"
    params["JOB_QUEUE"] = ARN + ":" + "job-queue/s3_get_bucket_list"
    params["JOB_DEFINITION"] = "test-batch-job" + ":" + str(revision_num)

    return params

