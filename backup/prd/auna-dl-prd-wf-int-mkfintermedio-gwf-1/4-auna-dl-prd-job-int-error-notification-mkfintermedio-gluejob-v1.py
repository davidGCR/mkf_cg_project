import sys
from datetime import datetime
from pprint import pprint
import boto3
from awsglue.utils import getResolvedOptions

sns = boto3.client('sns')
glue = boto3.client('glue')

args = getResolvedOptions(sys.argv, ['topic_arn','WORKFLOW_NAME'])

topic_arn = args['topic_arn']
workflow_name = args['WORKFLOW_NAME']

wf_response = glue.get_workflow(
    Name=workflow_name,
    IncludeGraph=True,
)

last_run = wf_response['Workflow']['LastRun']
nodes = last_run['Graph']['Nodes']
jobs = [node for node in nodes if node['Type'] == 'JOB']

job_data = []
for job in jobs:
    job_details = job['JobDetails']
    name = job['Name']

    if name != "auna-dl-prd-job-int-error-notification-mkfintermedio-gluejob-v1":
        if 'JobRuns' in job_details:
            last_job_run = job_details['JobRuns'][0]

            error_message = last_job_run.get('ErrorMessage', "")
            job_state = last_job_run.get('JobRunState', "NONE")
            started_on = last_job_run.get('StartedOn', "")

            if job_state == "FAILED":
                job_data.append({
                    "name": name,
                    "state": job_state,
                    "error": error_message,
                    "started_on": started_on,
                })

def pretty_job_run(name, state, error, started_on):
    return "\n".join([f"Job: {name}", f"Estado: {state}", f"Mensaje Error: {error}", f"Iniciado a las: {started_on}"])
    
response = sns.publish(
    TopicArn=topic_arn,
    Message="\n\n".join(["Se detectó un error en el flujo de carga del Modelo MarketForce Intermedio:"] + [pretty_job_run(j['name'], j['state'], j['error'], j['started_on']) for j in job_data])
)