#!/bin/bash
set -euo pipefail

# Usage: ./run-snapshot-load.sh <YYYY-MM-DD> [schema_check_mode] [suffix]
# suffix defaults to data; paths/table names ending in -data / _data get -suffix / _suffix.

SNAPSHOT_DATE="${1:?Usage: $0 <YYYY-MM-DD> [schema_check_mode] [suffix]}"
SCHEMA_MODE="${2:-strict}"
SUFFIX="${3:-data}"

# Replace trailing -data / _data only (avoids touching e.g. consumer_datamodel).
apply_data_suffix() {
    local v="$1"
    case "$v" in
        *_data) v="${v%_data}_${SUFFIX}" ;;
    esac
    case "$v" in
        *-data) v="${v%-data}-${SUFFIX}" ;;
    esac
    printf '%s' "$v"
}

SOURCE_PREFIX_TEMPLATE="Downloads/consumer-data"
TARGET_PREFIX_TEMPLATE="consumer/consumer_data"
TABLE_NAME_TEMPLATE="consumer_data"

SOURCE_PREFIX="$(apply_data_suffix "$SOURCE_PREFIX_TEMPLATE")"
TARGET_PREFIX="$(apply_data_suffix "$TARGET_PREFIX_TEMPLATE")"
TABLE_NAME="$(apply_data_suffix "$TABLE_NAME_TEMPLATE")"

JOB_NAME="data-snapshot-loader"

echo "Starting snapshot load for $SNAPSHOT_DATE (schema mode: $SCHEMA_MODE, suffix: $SUFFIX)"
echo "  source_prefix=$SOURCE_PREFIX target_prefix=$TARGET_PREFIX table_name=$TABLE_NAME"

RUN_ID=$(aws glue start-job-run \
    --job-name "$JOB_NAME" \
    --arguments "{
        \"--source_bucket\": \"datalabs-audienceacuity\",
        \"--source_prefix\": \"$SOURCE_PREFIX\",
        \"--target_bucket\": \"blue-glue-tables\",
        \"--target_prefix\": \"$TARGET_PREFIX\",
        \"--snapshot_date\": \"$SNAPSHOT_DATE\",
        \"--database_name\": \"consumer\",
        \"--table_name\": \"$TABLE_NAME\",
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