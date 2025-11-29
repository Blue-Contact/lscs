import sys
import csv
from io import StringIO

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality

from pyspark.sql import functions as F

# -----------------------------------------------------------------------------------
# Read job parameters
# -----------------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "INPUT_S3_PATH",
        "OUTPUT_S3_PATH",
        "OUTPUT_DATABASE",
        "OUTPUT_TABLE",
    ],
)

INPUT_S3_PATH   = args["INPUT_S3_PATH"]
OUTPUT_S3_PATH  = args["OUTPUT_S3_PATH"]
OUTPUT_DATABASE = args["OUTPUT_DATABASE"]
OUTPUT_TABLE    = args["OUTPUT_TABLE"]

# -----------------------------------------------------------------------------------
# Glue / Spark Context
# -----------------------------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()
logger.info(f"Starting job: {args['JOB_NAME']}")
logger.info(f"INPUT_S3_PATH   = {INPUT_S3_PATH}")
logger.info(f"OUTPUT_S3_PATH  = {OUTPUT_S3_PATH}")
logger.info(f"OUTPUT_DATABASE = {OUTPUT_DATABASE}")
logger.info(f"OUTPUT_TABLE    = {OUTPUT_TABLE}")

try:
    # -----------------------------------------------------------------------------------
    # 1) Read file(s) as plain text (NO spark.read.csv, NO schema inference)
    # -----------------------------------------------------------------------------------
    logger.info("Step 1: Reading input as plain text (spark.read.text)...")

    lines_df = spark.read.text(INPUT_S3_PATH)

    if lines_df.rdd.isEmpty():
        logger.error(f"No data found at INPUT_S3_PATH={INPUT_S3_PATH}")
        raise Exception(f"No data found at INPUT_S3_PATH={INPUT_S3_PATH}")

    total_lines = lines_df.count()
    logger.info(f"Step 1 complete: Read {total_lines} total line(s) from input path.")

    logger.info("Sample of first 5 raw lines:")
    lines_df.show(5, truncate=False)

    # -----------------------------------------------------------------------------------
    # 2) Extract header row and build DDL schema string (all STRING)
    # -----------------------------------------------------------------------------------
    logger.info("Step 2: Extracting header row and building dynamic schema DDL...")

    header_row = lines_df.first()[0]
    logger.info(f"Header row (truncated to 200 chars): {header_row[:200]}")

    reader = csv.reader(StringIO(header_row))
    header_cols = next(reader)

    header_cols = [col.strip().strip('"') for col in header_cols]
    logger.info(f"Detected {len(header_cols)} column(s) from header.")
    logger.info(f"First 10 column names: {header_cols[:10]}")

    schema_ddl = ", ".join(f"`{name}` STRING" for name in header_cols)
    logger.info(f"Schema DDL (truncated to 300 chars): {schema_ddl[:300]}")

    # -----------------------------------------------------------------------------------
    # 3) Drop header lines and parse rows with from_csv()
    # -----------------------------------------------------------------------------------
    logger.info("Step 3: Filtering out header row(s) and parsing CSV lines with from_csv...")

    # Drop all rows that exactly match the header row string
    data_df = lines_df.filter(F.col("value") != header_row)
    data_line_count = data_df.count()
    logger.info(f"Data lines after removing header: {data_line_count}")

    if data_line_count == 0:
        logger.error("No data rows found after removing header.")
        raise Exception("No data rows found after removing header.")

    parsed = data_df.select(
        F.from_csv(
            F.col("value"),
            schema_ddl,  # DDL string, not StructType
            {
                "delimiter": ",",
                "quote": "\"",
                "escape": "\"",
                # header option is ignored by from_csv; we handled header manually
            },
        ).alias("parsed")
    )

    df_raw = parsed.select("parsed.*")

    raw_count = df_raw.count()
    logger.info(f"Step 3 complete: Parsed {raw_count} row(s) into columns from CSV.")
    logger.info("Raw parsed schema:")
    df_raw.printSchema()

    logger.info("Sample of first 5 parsed rows:")
    df_raw.show(5, truncate=False)

    # -----------------------------------------------------------------------------------
    # 4) Trim whitespace AND convert blanks -> NULL
    # -----------------------------------------------------------------------------------
    logger.info("Step 4: Trimming whitespace and converting blank strings to NULL...")

    df_clean = df_raw
    for c in df_clean.columns:
        logger.info(f"Cleaning column: {c}")
        df_clean = df_clean.withColumn(
            c,
            F.when(F.length(F.trim(F.col(c))) == 0, F.lit(None)).otherwise(
                F.trim(F.col(c))
            ),
        )

    clean_count = df_clean.count()
    logger.info(f"Step 4 complete: Cleaned {clean_count} row(s).")

    logger.info("Sample of first 5 cleaned rows:")
    df_clean.show(5, truncate=False)

    # -----------------------------------------------------------------------------------
    # 5) Convert to DynamicFrame for DQ + sink
    # -----------------------------------------------------------------------------------
    logger.info("Step 5: Converting cleaned DataFrame to DynamicFrame...")
    dyf_clean = DynamicFrame.fromDF(df_clean, glueContext, "dyf_clean")

    logger.info("DynamicFrame schema:")
    dyf_clean.printSchema()

    # -----------------------------------------------------------------------------------
    # 6) Data Quality Rules (simple, supported rule type)
    # -----------------------------------------------------------------------------------
    logger.info("Step 6: Running Glue Data Quality rules...")

    # Use RowCount (safe, widely supported) instead of ColumnCount
    DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        RowCount > 0
    ]
    """

    logger.info("DQ ruleset being applied:")
    logger.info(DEFAULT_DATA_QUALITY_RULESET)

    # Recommended API per docs: EvaluateDataQuality.apply(...)
    dq_frame = EvaluateDataQuality.apply(
        frame=dyf_clean,
        ruleset=DEFAULT_DATA_QUALITY_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "DQ_Waverly",
            "enableDataQualityCloudWatchMetrics": False,
            "enableDataQualityResultsPublishing": True,
        },
    )

    logger.info("DQ results sample (first 5 rows):")
    dq_frame.toDF().show(5, truncate=False)

    # -----------------------------------------------------------------------------------
    # 7) Write out as Parquet and update Glue catalog
    # -----------------------------------------------------------------------------------
    logger.info("Step 7: Writing cleaned data to Parquet and updating Glue Catalog...")

    sink = glueContext.getSink(
        path=OUTPUT_S3_PATH,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],  # add partitions later if desired, e.g. ["State"]
        enableUpdateCatalog=True,
        transformation_ctx="GlueSink",
    )

    sink.setCatalogInfo(
        catalogDatabase=OUTPUT_DATABASE,
        catalogTableName=OUTPUT_TABLE,
    )

    sink.setFormat("glueparquet", compression="snappy")

    sink.writeFrame(dyf_clean)

    logger.info("Parquet write and catalog update complete.")

    job.commit()
    logger.info("Job commit successful. Job completed without errors.")

except Exception as e:
    logger.error(f"Job failed with exception: {repr(e)}")
    # Re-raise so Glue marks the job as FAILED
    raise