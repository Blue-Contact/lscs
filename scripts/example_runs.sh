# Start a list import
aws glue start-job-run \
  --job-name ListImport \
  --arguments '{
    "--INPUT_S3_PATH":"s3://lsc-imports/dnb/DNB_Texas_100K_csv/",
    "--OUTPUT_S3_PATH":"s3://lsc-databases/lsc/DNB_Texas_100K/",
    "--OUTPUT_DATABASE":"lsc",
    "--OUTPUT_TABLE":"DNB_Texas_100K"
  }'

# Start a consumer match job
# The INPUT_COLUMN_MAPPING argument maps the standard input columns to the actual column names in your table
# The key names on the left are the standard names expected by the job
# The values on the right are the actual column names in your input table

aws glue start-job-run \
  --job-name consumer-match-job \
  --arguments '{
      "--INPUT_TABLE": "source_b.b2c",
      "--MATCH_TABLE": "source_a.consumer_data",
      "--OUTPUT_PATH": "s3://lsc-databases/source_b/b2c_matched/",
      "--OUTPUT_TABLE": "source_b.b2c_matched",
      "--MATCH_THRESHOLD": "92",
      "--INPUT_COLUMN_MAPPING": "{\"first_name\":\"first\",\"last_name\":\"last\",\"address\":\"std_address\",\"city\":\"std_city\",\"state\":\"std_state\",\"zip\":\"std_zip\",\"zip4\":\"std_zip4\"}"
  }'

aws glue start-job-run \
  --job-name consumer-match-job \
  --arguments '{
    "--INPUT_TABLE": "clients.ct0310b_sabel",
    "--MATCH_TABLE": "source_b.b2c",
    "--OUTPUT_PATH": "s3://lsc-databases/clients/ct0310b_sabel_matched/",
    "--OUTPUT_TABLE": "clients.ct0310b_sabel_matched",
    "--MATCH_THRESHOLD": "92",
    "--INPUT_COLUMN_MAPPING": "{\"first_name\": \"first_name\", \"last_name\": \"last_name\", \"address\": \"address\", \"city\": \"city\", \"state\": \"state\", \"zip\": \"zip\", \"zip4\": \"zip4\"}",
    "--MATCH_COLUMN_MAPPING": "{\"id\": \"pid\", \"first_name\": \"first\", \"last_name\": \"last\", \"address\": \"std_address\", \"state\": \"std_state\", \"zip\": \"std_zip\", \"zip4\": \"std_zip4\"}"
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
  --job-name lsc-fixed-width-to-csv \
  --arguments '{
    "--INPUT_S3_PATH": "s3://lsc-imports/dnb/DNB_Texas_100K.txt",
    "--OUTPUT_S3_PATH": "s3://lsc-imports/dnb/DNB_Texas_100K_csv/"
  }'