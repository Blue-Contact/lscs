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
      "--MATCH_TABLE": "source_a.consumer",
      "--OUTPUT_PATH": "s3://lsc-databases/clients/<my-new-table-location>/",
      "--OUTPUT_TABLE": "<target-database>.<target-table>",
      "--MATCH_THRESHOLD": "92",
      "--INPUT_COLUMN_MAPPING": "{\"first_name\":\"FIRSTNAME\",\"last_name\":\"LASTNAME\",\"address\":\"bc_std_street\",\"city\":\"bc_std_city\",\"state\":\"bc_std_state\",\"zip\":\"bc_std_zip\",\"zip4\":\"bc_std_zip_4\",\"email\":\"BUSINESSEMAIL,PERSONALEMAIL\",\"phone\":\"BUSINESSPHONE1,BUSINESSPHONE2,MOBILEPHONE,HOMEPHONE\"}"
  }'

# Start a list import + key match job (email/phone/address)
aws glue start-job-run \
  --job-name match-consumer-key \
  --arguments '{
    "--INPUT_S3_PATH": "s3://bluecontact-sftp2/linqd/match-stage/output/2026-02-24/",
    "--MATCH_TABLE": "bluecontact.consumer_key_a",
    "--OUTPUT_PATH": "s3://blue-datasets/tables/linqd_202602_matched/",
    "--OUTPUT_TABLE": "my_tables.linqd_202602_matched",
    "--MATCH_THRESHOLD": "92",
    "--INPUT_COLUMN_MAPPING": "{\"first_name\":\"FIRSTNAME\",\"last_name\":\"LASTNAME\",\"address\":\"bc_std_street\",\"city\":\"bc_std_city\",\"state\":\"bc_std_state\",\"zip\":\"bc_std_zip\",\"zip4\":\"bc_std_zip_4\",\"email\":\"BUSINESSEMAIL,PERSONALEMAIL\",\"phone\":\"BUSINESSPHONE1,BUSINESSPHONE2,MOBILEPHONE,HOMEPHONE\"}"
  }'