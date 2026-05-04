#!/bin/bash
set -euo pipefail

# Usage: ./run-snapshot-load.sh 2026-04-03 [strict|warn|skip]

SNAPSHOT_DATE="${1:?Usage: $0 <YYYY-MM-DD> [schema_check_mode]}"
SCHEMA_MODE="${2:-strict}"

JOB_NAME="data-snapshot-loader"

echo "Starting snapshot load for $SNAPSHOT_DATE (schema mode: $SCHEMA_MODE)"

RUN_ID=$(aws glue start-job-run \
    --job-name "$JOB_NAME" \
    --arguments "{
        \"--source_bucket\": \"datalabs-audienceacuity\",
        \"--source_prefix\": \"Downloads/consumer-email\",
        \"--target_bucket\": \"blue-glue-tables\",
        \"--target_prefix\": \"consumer/consumer_email\",
        \"--snapshot_date\": \"$SNAPSHOT_DATE\",
        \"--database_name\": \"consumer\",
        \"--table_name\": \"consumer_email\",
        \"--schema_check_mode\": \"$SCHEMA_MODE\"
    }" \
    --query 'JobRunId' --output text)

echo "Job run started: $RUN_ID"
echo "Streaming status..."

while true; do
    STATE=$(aws glue get-job-run --job-name "$JOB_NAME" --run-id "$RUN_ID" \
        --query 'JobRun.JobRunState' --output text)
    echo "  [$( date '+%H:%M:%S')] $STATE"

    case "$STATE" in
        SUCCEEDED)
            echo "✓ Load complete for $SNAPSHOT_DATE"
            exit 0
            ;;
        FAILED|TIMEOUT|STOPPED)
            echo "✗ Job ended in state: $STATE"
            aws glue get-job-run --job-name "$JOB_NAME" --run-id "$RUN_ID" \
                --query 'JobRun.ErrorMessage' --output text
            exit 1
            ;;
    esac
    sleep 15
done