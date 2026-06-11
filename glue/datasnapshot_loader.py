import sys
import re
import time
import boto3
import pyarrow as pa
from typing import Optional
from botocore.exceptions import ClientError
import pyarrow.parquet as pq
import pyarrow.fs as pafs
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job


def _normalize_type(t):
    """Normalize type strings for comparison (Glue uses Hive types, Parquet uses Arrow types)."""
    t = t.lower().strip()
    mapping = {
        'int32': 'int', 'int64': 'bigint', 'float': 'float', 'double': 'double',
        'string': 'string', 'bool': 'boolean', 'boolean': 'boolean',
    }
    return mapping.get(t, t)


def _arrow_type_to_glue(t: pa.DataType) -> str:
    if pa.types.is_boolean(t):
        return 'boolean'
    if pa.types.is_integer(t):
        if pa.types.is_int64(t) or pa.types.is_uint64(t):
            return 'bigint'
        return 'int'
    if pa.types.is_float32(t):
        return 'float'
    if pa.types.is_float64(t):
        return 'double'
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return 'string'
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return 'binary'
    if pa.types.is_date32(t) or pa.types.is_date64(t):
        return 'date'
    if pa.types.is_timestamp(t):
        return 'timestamp'
    if pa.types.is_decimal(t):
        return f'decimal({t.precision},{t.scale})'
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return f'array<{_arrow_type_to_glue(t.value_type)}>'
    if pa.types.is_struct(t):
        inner = ','.join(f'{f.name}:{_arrow_type_to_glue(f.type)}' for f in t)
        return f'struct<{inner}>'
    if pa.types.is_map(t):
        return f'map<{_arrow_type_to_glue(t.key_type)},{_arrow_type_to_glue(t.item_type)}>'
    print(f"Warning: unsupported Arrow type {t}, mapping Glue column type to string")
    return 'string'


def _parquet_schema_to_glue_columns(arrow_schema: pa.Schema, partition_names: set) -> list:
    cols = []
    for field in arrow_schema:
        if field.name in partition_names:
            continue
        cols.append({'Name': field.name, 'Type': _arrow_type_to_glue(field.type)})
    return cols


def _create_glue_parquet_snapshot_table(
    glue_client,
    *,
    database_name: str,
    table_name: str,
    target_bucket: str,
    target_prefix_base: str,
    arrow_schema: pa.Schema,
) -> None:
    partition_names = {'snapshot_date'}
    columns = _parquet_schema_to_glue_columns(arrow_schema, partition_names)
    if not columns:
        raise ValueError(
            'Parquet schema has no data columns after excluding partition column snapshot_date.'
        )
    table_root = f"s3://{target_bucket}/{target_prefix_base.rstrip('/')}/"
    glue_client.create_table(
        DatabaseName=database_name,
        TableInput={
            'Name': table_name,
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {'classification': 'parquet', 'EXTERNAL': 'TRUE'},
            'PartitionKeys': [{'Name': 'snapshot_date', 'Type': 'string'}],
            'StorageDescriptor': {
                'Columns': columns,
                'Location': table_root,
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                    'Parameters': {'serialization.format': '1'},
                },
            },
        },
    )


def _verify_glue_table_visible(glue_client, database_name: str, table_name: str) -> None:
    """Poll GetTable after CreateTable in case of catalog propagation lag."""
    interval_sec = 2.0
    attempts = 15
    last_err: Optional[BaseException] = None
    for i in range(attempts):
        try:
            glue_client.get_table(DatabaseName=database_name, Name=table_name)
            if i:
                print(
                    f"Glue table {database_name}.{table_name} visible after "
                    f"{i + 1} GetTable attempts."
                )
            return
        except glue_client.exceptions.EntityNotFoundException as err:
            last_err = err
            time.sleep(interval_sec)
    raise RuntimeError(
        f"glue.create_table finished but GetTable still returns NOT FOUND for "
        f"{database_name}.{table_name} after ~{attempts * interval_sec:.0f}s. "
        "Confirm region and account match the Glue Data Catalog, Lake Formation allows access, "
        "and IAM permits glue:GetTable."
    ) from last_err


args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_bucket',
    'source_prefix',
    'target_bucket',
    'target_prefix',
    'snapshot_date',
    'database_name',
    'table_name',
    'schema_check_mode'  # 'strict' | 'warn' | 'skip'
])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3 = boto3.client('s3')
glue = boto3.client('glue')

source_bucket = args['source_bucket']
source_prefix = f"{args['source_prefix'].rstrip('/')}/{args['snapshot_date']}/"
target_bucket = args['target_bucket']
target_prefix = f"{args['target_prefix'].rstrip('/')}/snapshot_date={args['snapshot_date']}/"

# --- Step 0: Validate snapshot_date format ---
if not re.match(r'^\d{4}-\d{2}-\d{2}$', args['snapshot_date']):
    raise ValueError(f"snapshot_date must be YYYY-MM-DD, got: {args['snapshot_date']}")

# --- Step 1: Idempotency check ---
try:
    glue.get_partition(
        DatabaseName=args['database_name'],
        TableName=args['table_name'],
        PartitionValues=[args['snapshot_date']]
    )
    print(f"Partition snapshot_date={args['snapshot_date']} already exists. Exiting.")
    job.commit()
    sys.exit(0)
except glue.exceptions.EntityNotFoundException:
    pass

existing = s3.list_objects_v2(Bucket=target_bucket, Prefix=target_prefix, MaxKeys=1)
if existing.get('KeyCount', 0) > 0:
    raise Exception(
        f"Target path s3://{target_bucket}/{target_prefix} already has objects "
        f"but no Glue partition exists. Investigate before proceeding."
    )

# --- Step 2: List source Parquet files ---
paginator = s3.get_paginator('list_objects_v2')
parquet_keys = []
other_files = []
for page in paginator.paginate(Bucket=source_bucket, Prefix=source_prefix):
    for obj in page.get('Contents', []):
        if obj['Key'].lower().endswith('.parquet'):
            parquet_keys.append(obj['Key'])
        else:
            other_files.append(obj['Key'])

if not parquet_keys:
    raise Exception(f"No Parquet files found in s3://{source_bucket}/{source_prefix}")

print(f"Found {len(parquet_keys)} Parquet files to copy")
if other_files:
    print(f"Skipping {len(other_files)} non-Parquet files: {other_files[:5]}{'...' if len(other_files) > 5 else ''}")

try:
    glue.get_table(DatabaseName=args['database_name'], Name=args['table_name'])
    table_exists = True
except glue.exceptions.EntityNotFoundException:
    table_exists = False

need_source_schema = (not table_exists) or (args['schema_check_mode'] != 'skip')
if need_source_schema:
    s3_fs = pafs.S3FileSystem()
    sample_file = f"{source_bucket}/{parquet_keys[0]}"
    source_arrow_schema = pq.read_schema(sample_file, filesystem=s3_fs)
    source_cols = {field.name: str(field.type) for field in source_arrow_schema}
else:
    source_arrow_schema = None
    source_cols = None

# --- Step 3: Schema validation ---
if args['schema_check_mode'] != 'skip':
    print("Running schema validation...")

    # Get existing table schema from Glue
    try:
        table = glue.get_table(DatabaseName=args['database_name'], Name=args['table_name'])['Table']
        existing_cols = {col['Name']: col['Type'] for col in table['StorageDescriptor']['Columns']}

        added = set(source_cols) - set(existing_cols)
        removed = set(existing_cols) - set(source_cols)
        type_changed = {
            c: (existing_cols[c], source_cols[c])
            for c in set(source_cols) & set(existing_cols)
            if _normalize_type(existing_cols[c]) != _normalize_type(source_cols[c])
        }

        if added or removed or type_changed:
            msg_parts = ["Schema differences detected:"]
            if added:
                msg_parts.append(f"  Added columns ({len(added)}): {sorted(added)[:10]}")
            if removed:
                msg_parts.append(f"  Removed columns ({len(removed)}): {sorted(removed)[:10]}")
            if type_changed:
                msg_parts.append(f"  Type changes ({len(type_changed)}): {dict(list(type_changed.items())[:5])}")
            msg = "\n".join(msg_parts)
            print(msg)

            if args['schema_check_mode'] == 'strict':
                raise Exception(
                    f"Schema validation failed in strict mode. "
                    f"Re-run with schema_check_mode=warn to proceed anyway, "
                    f"or update the table schema first."
                )
        else:
            print(f"Schema validation passed ({len(source_cols)} columns match)")

    except glue.exceptions.EntityNotFoundException:
        print(f"Table {args['table_name']} doesn't exist yet — skipping schema check (first load)")

if not table_exists:
    print(
        f"Creating Glue table {args['database_name']}.{args['table_name']} "
        "(external Parquet, partition snapshot_date)..."
    )
    try:
        _create_glue_parquet_snapshot_table(
            glue,
            database_name=args['database_name'],
            table_name=args['table_name'],
            target_bucket=target_bucket,
            target_prefix_base=args['target_prefix'],
            arrow_schema=source_arrow_schema,
        )
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code', '')
        if code == 'AlreadyExistsException':
            print(
                f"Table {args['database_name']}.{args['table_name']} already exists "
                "(concurrent or prior run); continuing."
            )
        else:
            raise
    _verify_glue_table_visible(glue, args['database_name'], args['table_name'])

# --- Step 4: Copy Parquet files (server-side) ---
copied_keys = []
try:
    for key in parquet_keys:
        filename = key.split('/')[-1]
        target_key = f"{target_prefix}{filename}"
        s3.copy_object(
            Bucket=target_bucket,
            Key=target_key,
            CopySource={'Bucket': source_bucket, 'Key': key}
        )
        copied_keys.append(target_key)
    print(f"Copied {len(copied_keys)} files to s3://{target_bucket}/{target_prefix}")
except Exception as e:
    # Clean up partial copies on failure
    print(f"Copy failed after {len(copied_keys)} files, rolling back...")
    for key in copied_keys:
        try:
            s3.delete_object(Bucket=target_bucket, Key=key)
        except Exception as cleanup_err:
            print(f"  Cleanup failed for {key}: {cleanup_err}")
    raise e

# --- Step 5: Register partition ---
try:
    table = glue.get_table(DatabaseName=args['database_name'], Name=args['table_name'])['Table']
except glue.exceptions.EntityNotFoundException as e:
    raise RuntimeError(
        f"Glue GetTable failed for {args['database_name']}.{args['table_name']} while registering "
        f"partition snapshot_date={args['snapshot_date']}. "
        "If CloudWatch logs never printed 'Creating Glue table', this Glue job is probably still "
        "running an older script without auto-create — redeploy the latest datasnapshot_loader.py. "
        "Otherwise confirm the Glue database exists, names match job arguments, region is correct, "
        "and IAM allows glue:CreateTable and glue:GetTable."
    ) from e
storage_descriptor = table['StorageDescriptor'].copy()
storage_descriptor['Location'] = f"s3://{target_bucket}/{target_prefix}"

glue.create_partition(
    DatabaseName=args['database_name'],
    TableName=args['table_name'],
    PartitionInput={
        'Values': [args['snapshot_date']],
        'StorageDescriptor': storage_descriptor
    }
)
print(f"Registered partition snapshot_date={args['snapshot_date']}")

# --- Step 6: Write success marker ---
s3.put_object(
    Bucket=target_bucket,
    Key=f"{target_prefix}_SUCCESS",
    Body=f"snapshot_date={args['snapshot_date']}\nfile_count={len(copied_keys)}\n"
)

job.commit()