# lscs

AWS Glue jobs and helper scripts for LSC.

## Glue job: `glue/list_import_and_match.py`

Single Glue job that **imports a delimited list file from S3** (header-driven, all columns as strings) and then **matches each row to an existing Glue table** using fuzzy/phonetic matching. It writes a **final matched Parquet dataset** to S3 and registers a Glue table that includes the match results.

### What it does

- **Ingest**: reads `INPUT_S3_PATH` via `spark.read.text()`, parses header → builds a DDL schema (all `STRING`), parses rows via `from_csv`, trims whitespace, converts blanks to `NULL`.
- **Match**: loads `MATCH_TABLE` from the Glue Catalog, registers fuzzy UDFs (rapidfuzz + jellyfish), runs a ranked match query (best match per input row).
- **Output**: writes Parquet to `OUTPUT_PATH` (overwrite) and creates/ensures `OUTPUT_TABLE` points at that location.

### Output columns

The output table includes:

- **All imported columns** from the input file
- Match result columns:
  - `match_id`, `match_first_name`, `match_last_name`, `match_address`, `match_zip`, `match_zip4`
  - `match_first_name_score`, `match_last_name_score`, `match_address_score`
  - `match_type`, `match_overall_score`

### Arguments

Required:

- `--JOB_NAME`
- `--INPUT_S3_PATH` (S3 path to delimited text file; first row must be header)
- `--MATCH_TABLE` (Glue table, e.g. `source_a.consumer_data`)
- `--OUTPUT_PATH` (S3 prefix for output Parquet)
- `--OUTPUT_TABLE` (Glue table name including database, e.g. `clients.telebrands_list_202511_matched`)
- `--MATCH_THRESHOLD` (0–100, e.g. `92`)
- `--INPUT_COLUMN_MAPPING` (JSON mapping from standard names to input-file column names)
  - required standard keys: `first_name`, `last_name`, `address`, `city`, `state`, `zip`, `zip4`

Optional:

- `--INPUT_DELIMITER` (defaults to `,`; supports `tab`, `pipe`, or escape sequences like `\\t`)
- `--STATE_FILTER` (2-letter code; filters both input + match datasets for testing)

### Glue dependencies

This job requires additional python modules (same as `consumer_match.py`):

- `nicknames==0.1.0`
- `jellyfish==0.9.0`
- `rapidfuzz==3.6.1`

In Glue, set:

- `--additional-python-modules "nicknames==0.1.0,jellyfish==0.9.0,rapidfuzz==3.6.1"`

### Usage examples

#### Example 1: Basic run (comma-delimited)

```bash
aws glue start-job-run \
  --job-name list-import-and-match \
  --arguments '{
    "--INPUT_S3_PATH": "s3://lsc-databases/clients/telebrands_list_202511/input/list.csv",
    "--MATCH_TABLE": "source_a.consumer_data",
    "--OUTPUT_PATH": "s3://lsc-databases/clients/telebrands_list_202511_matched/",
    "--OUTPUT_TABLE": "clients.telebrands_list_202511_matched",
    "--MATCH_THRESHOLD": "92",
    "--INPUT_COLUMN_MAPPING": "{\"first_name\":\"firstname\",\"last_name\":\"lastname\",\"address\":\"street\",\"city\":\"city\",\"state\":\"state\",\"zip\":\"zipcode\",\"zip4\":\"plus4\"}"
  }'
```

#### Example 2: Pipe-delimited + filter to a single state for testing

```bash
aws glue start-job-run \
  --job-name list-import-and-match \
  --arguments '{
    "--INPUT_S3_PATH": "s3://lsc-databases/clients/some_list/input/list.psv",
    "--INPUT_DELIMITER": "pipe",
    "--STATE_FILTER": "CA",
    "--MATCH_TABLE": "source_a.consumer_data",
    "--OUTPUT_PATH": "s3://lsc-databases/clients/some_list_matched/",
    "--OUTPUT_TABLE": "clients.some_list_matched",
    "--MATCH_THRESHOLD": "92",
    "--INPUT_COLUMN_MAPPING": "{\"first_name\":\"firstname\",\"last_name\":\"lastname\",\"address\":\"street\",\"city\":\"city\",\"state\":\"state\",\"zip\":\"zipcode\",\"zip4\":\"plus4\"}"
  }'
```

### Notes / assumptions

- The match table (`MATCH_TABLE`) is expected to have at least: `id`, `first_name`, `last_name`, `address`, `state`, `zip`, `zip4`.
- Matching currently blocks on `zip_norm` + `zip4_norm` (same as the original matcher logic).
