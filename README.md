# LSCS

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
- Optional match-table columns when `MATCH_APPEND_COLUMNS` is set (original column names)

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
- `--MATCH_APPEND_COLUMNS` (default: match columns only; use `ALL` or a comma list like `age,gender`)
  - `ALL` appends all `MATCH_TABLE` columns (including `first_name`, `last_name`, `address`, `zip`, `zip4`)
  - Columns that would duplicate existing output names are prefixed with `matched_`
  - If `matched_` still collides, the job suffixes `_2`, `_3`, etc.

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
    "--OUTPUT_PATH": "s3://lsc-databases/clients/telebrands_list/",
    "--OUTPUT_TABLE": "clients.telebrands_list",
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

## Glue job: `glue/list_import_and_match_keys.py`

Single Glue job that **imports a delimited list file from S3** and then **matches each row to a consumer key table** using exact key matching (email/phone/address). Name validation uses the same fuzzy name matching UDFs as `list_import_and_match.py`.

### What it does

- **Ingest**: identical to `list_import_and_match.py` (header-driven CSV parsing, all STRING columns).
- **Key match**: matches on normalized `key` + `key_type` (email/phone/address).
- **Name validation**:
  - `INDIVIDUAL_MATCH`: first + last name scores >= `MATCH_THRESHOLD`
  - `HOUSEHOLD_MATCH`: last name score >= `MATCH_THRESHOLD`, first name below
  - `KEY_MATCH`: key matched but name threshold not met
- **Output**: writes Parquet to `OUTPUT_PATH` and creates/ensures `OUTPUT_TABLE` at that location.

### Output columns

The output table includes:

- **All imported columns** from the input file
- Match result columns:
  - `match_consumer_id`, `match_uuid`, `match_key`, `match_key_type`
  - `match_first`, `match_last`
  - `match_first_name_score`, `match_last_name_score`
  - `match_type`, `match_date`
- Optional match-table columns when `MATCH_APPEND_COLUMNS` is set (original column names)

### Arguments

Required:

- `--JOB_NAME`
- `--INPUT_S3_PATH` (S3 path to delimited text file; first row must be header)
- `--MATCH_TABLE` (Glue table with `key` + `key_type`, e.g. `source_a.consumer_key`)
- `--OUTPUT_PATH` (S3 prefix for output Parquet)
- `--OUTPUT_TABLE` (Glue table name including database)
- `--MATCH_THRESHOLD` (0–100, e.g. `92`)
- `--INPUT_COLUMN_MAPPING` (JSON mapping from standard names to input-file column names)
  - required standard keys: `first_name`, `last_name`
  - key sources:
    - `email` (comma list or JSON array)
    - `phone` (comma list or JSON array)
    - `address` + `zip` (address key is `"<street> <zip5>"`)
  - at least one of `email`, `phone`, or `address`+`zip` is required

Optional:

- `--INPUT_DELIMITER` (defaults to `,`; supports `tab`, `pipe`, or escape sequences like `\\t`)
- `--STATE_FILTER` (2-letter code; filters both input + match datasets for testing)
- `--MATCH_APPEND_COLUMNS` (default: match columns only; use `ALL` or a comma list like `age,gender`)

### Glue dependencies

This job requires the same additional python modules as `list_import_and_match.py`:

- `nicknames==0.1.0`
- `jellyfish==0.9.0`
- `rapidfuzz==3.6.1`

### Usage example

```bash
aws glue start-job-run \
  --job-name list-import-and-match-keys \
  --arguments '{
    "--INPUT_S3_PATH": "s3://lsc-databases/clients/my_list/input/list.csv",
    "--MATCH_TABLE": "source_a.consumer_key",
    "--OUTPUT_PATH": "s3://lsc-databases/clients/my_list_matched/",
    "--OUTPUT_TABLE": "clients.my_list_matched",
    "--MATCH_THRESHOLD": "92",
    "--INPUT_COLUMN_MAPPING": "{\"first_name\":\"first\",\"last_name\":\"last\",\"email\":\"email,alt_email\",\"phone\":[\"phone\",\"mobile\"],\"address\":\"street\",\"zip\":\"zip\"}"
  }'
```

### CSV header naming best practices (Glue / Athena)

Glue Data Catalog + Athena are happiest when CSV headers are already “catalog-safe”. If headers include spaces or punctuation, Glue may auto-sanitize and (depending on the situation) append `#<n>` to produce deterministic names (e.g. `finder_number#20`).

- **Do**: use lowercase `snake_case` with only `[a-z0-9_]`
  - Examples: `first_name`, `last_name`, `primary_address`, `zip_4_code`, `finder_number`
- **Do**: ensure headers are unique after normalization
  - Example: `zip` and `ZIP` collide once lowercased
- **Don’t**: use spaces or punctuation in header names
  - Avoid: `LAST NAME`, `ZIP+4 CODE`, `DELIVERY POINT BARCODE`
- **Don’t**: start names with a digit (prefix with `_` instead)
  - Avoid: `1st_address` → prefer `_1st_address` or `address_1`

**Header “before → after” examples**

- `LAST NAME` → `last_name`
- `ZIP+4 CODE` → `zip_4_code`
- `DELIVERY POINT BARCODE` → `delivery_point_barcode`
- `FINDER NUMBER` → `finder_number`

**Note:** The import jobs in this repo now sanitize headers to safe, unique names during ingest, but providing catalog-safe headers up front keeps the schema stable across tools and avoids surprises when querying in Athena.
