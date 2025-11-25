# Start a list import
aws glue start-job-run \
  --job-name ListImport \
  --arguments '{
    "--INPUT_S3_PATH":"s3://lsc-imports/<my-import-folder>/",
    "--OUTPUT_S3_PATH":"s3://lsc-databases/clients/<my-new-table-location>/",
    "--OUTPUT_DATABASE":"<target-database>",
    "--OUTPUT_TABLE":"<target-table>"
  }'

# Start a consumer match job
# The INPUT_COLUMN_MAPPING argument maps the standard input columns to the actual column names in your table
# The key names on the left are the standard names expected by the job
# The values on the right are the actual column names in your input table

aws glue start-job-run \
  --job-name consumer-match-job \
  --arguments '{
      "--INPUT_TABLE": "database_name.table_name",
      "--MATCH_TABLE": "database_name.table_name_to_match_against",
      "--OUTPUT_PATH": "s3://lsc-databases/clients/<my-new-table-location>/",
      "--OUTPUT_TABLE": "<target-database>.<target-table>",
      "--MATCH_THRESHOLD": "92",
      "--INPUT_COLUMN_MAPPING": "{\"first_name\":\"firstname\",\"last_name\":\"lastname\",\"address\":\"street\",\"city\":\"city\",\"state\":\"state\",\"zip\":\"zipcode\",\"zip4\":\"plus4\"}"
  }'