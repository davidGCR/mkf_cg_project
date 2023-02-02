#!/bin/bash
JOB_NAME=4-auna-dlake-qas-job-int-analytics-mkfintermedio-gluejob-v2-executeloadanalytics-4

echo "---> (1) Running: ${JOB_NAME}"
aws glue start-job-run --job-name $JOB_NAME

while true; do
    sleep 5
    STATUS=$(aws glue get-job-runs --job-name $JOB_NAME --query 'JobRuns[0].JobRunState' --output text)
    echo "Status: ${STATUS}..."
    if [[ "$STATUS" == "SUCCEEDED" ]]; then
        break
    fi
done

JOB_ID=$(aws glue get-job-runs --job-name $JOB_NAME --query 'JobRuns[0].Id' --output text)
echo "---> Job Id: ${JOB_ID}"

if [[ "$STATUS" == "FAILED" ]]; then
    echo "---> Getting ErrorLogs..."
    rm logs.json
    echo "---> Saving to error_logs.json"
    # MSYS_NO_PATHCONV=1 aws logs filter-log-events --log-group-name /aws-glue/jobs/error --log-stream-names $JOB_ID --filter-pattern ERROR | tr '\n\t\' '\n' > logs.json
    MSYS_NO_PATHCONV=1 aws logs filter-log-events --log-group-name /aws-glue/jobs/error --log-stream-names $JOB_ID --filter-pattern ERROR > error_logs.json
    echo "---> Opening error_logs.json"
    code -r error_logs.json
elif [[ "$STATUS" == "SUCCEEDED" ]]; then
    echo "---> Getting Logs..."
    MSYS_NO_PATHCONV=1 aws logs filter-log-events --log-group-name /aws-glue/jobs/error --log-stream-names $JOB_ID > logs.json
    code -r logs.json
fi
