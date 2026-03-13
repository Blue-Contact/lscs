# Start a list import
aws glue start-job-run \
  --job-name ListImport \
  --arguments '{
    "--INPUT_S3_PATH":"s3://blue-imports/consumer_property_202512/",
    "--OUTPUT_S3_PATH":"s3://blue-glue-tables/source_a/consumer_property/",
    "--OUTPUT_DATABASE":"source_a",
    "--OUTPUT_TABLE":"consumer_property",
    "--INPUT_DELIMITER":"pipe"
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
    "--INPUT_S3_PATH": "s3://blue-imports/altair/202603/email_append/20260224_Altair_VU_File_For_Email_Append.csv",
    "--MATCH_TABLE": "bluecontact.consumer_key_a",
    "--OUTPUT_PATH": "s3://blue-datasets/projects/altair_email_append_202602/",
    "--OUTPUT_TABLE": "projects.altair_email_append_202602",
    "--MATCH_THRESHOLD": "92",
    "--INPUT_COLUMN_MAPPING": "{\"first_name\":\"first_name\",\"last_name\":\"last_name\",\"address\":\"address\",\"city\":\"city`\",\"state\":\"state\",\"zip\":\"zip\",\"zip4\":\"zip4\", \"email\":\"email,email_2\",\"phone\":\"phone,phone_2\"}"
  }'

  # Start a list import + key match job (email/phone/address)
aws glue start-job-run \
  --job-name match-consumer-key \
  --arguments '{
    "--INPUT_S3_PATH": "s3://bluecontact-sftp2/linqd/match-stage/output/2026-02-24/",
    "--MATCH_TABLE": "bluecontact.consumer_key_a",
    "--OUTPUT_PATH": "s3://blue-glue-tables/linqd/match_2026/",
    "--OUTPUT_TABLE": "linqd.match_2026",
    "--MATCH_THRESHOLD": "92",
    "--INPUT_COLUMN_MAPPING": "{\"first_name\":\"FIRSTNAME\",\"last_name\":\"LASTNAME\",\"address\":\"bc_std_street\",\"city\":\"bc_std_city\",\"state\":\"bc_std_state\",\"zip\":\"bc_std_zip\",\"zip4\":\"bc_std_zip_4\",\"email\":\"BUSINESSEMAIL,PERSONALEMAIL\",\"phone\":\"BUSINESSPHONE1,BUSINESSPHONE2,MOBILEPHONE,HOMEPHONE\"}"
  }'

  aws glue start-job-run \
  --job-name match-consumer-key \
  --arguments '{
    "--INPUT_S3_PATH": "s3://bluecontact-sftp2/linqd/match-stage/output/2026-02-24/",
    "--MATCH_TABLE": "bluecontact.consumer_key_a",
    "--OUTPUT_PATH": "s3://blue-glue-tables/linqd/match_2026/",
    "--OUTPUT_TABLE": "linqd.match_2026",
    "--MATCH_THRESHOLD": "92",
    "--INPUT_COLUMN_MAPPING": "{\"first_name\":\"FIRSTNAME\",\"last_name\":\"LASTNAME\",\"address\":\"bc_std_street\",\"city\":\"bc_std_city\",\"state\":\"bc_std_state\",\"zip\":\"bc_std_zip\",\"zip4\":\"bc_std_zip_4\",\"email\":\"BUSINESSEMAIL,PERSONALEMAIL\",\"phone\":\"BUSINESSPHONE1,BUSINESSPHONE2,MOBILEPHONE,HOMEPHONE\"}"
  }'

    aws glue start-job-run \
    --job-name match-consumer-key \
    --arguments '{
      "--INPUT_S3_PATH": "s3://blue-imports/lsc/financial_advisors_tal/",
      "--MATCH_TABLE": "bluecontact.consumer_key_a",
      "--OUTPUT_PATH": "s3://blue-glue-tables/lsc/financial_advisors_tal/",
      "--OUTPUT_TABLE": "lsc.financial_advisors_tal",
      "--MATCH_THRESHOLD": "92",
      "--INPUT_COLUMN_MAPPING": "{\"first_name\":\"cFirstName\",\"last_name\":\"cLastName\",\"email\":\"cEmail\",\"phone\":\"cPhone\"}"
    }'

# Convert D&B fixed-width file to CSV
aws glue start-job-run \
  --job-name dnb-fixed-width-to-csv \
  --arguments '{
    "--INPUT_S3_PATH": "s3://blue-imports/dnb/202603/",
    "--OUTPUT_S3_PATH": "s3://blue-datasets/dnb/202603_csv/"
  }'

# With optional state filter and output partition control
aws glue start-job-run \
  --job-name dnb-fixed-width-to-csv \
  --arguments '{
    "--INPUT_S3_PATH": "s3://blue-imports/dnb/202603/",
    "--OUTPUT_S3_PATH": "s3://blue-datasets/dnb/202603_csv_tx/",
    "--STATE_FILTER": "TX",
    "--COALESCE": "1"
  }'