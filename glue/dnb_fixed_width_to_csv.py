import sys
import logging

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DNB fixed-width column layout
# Each tuple: (column_name, 1-based start position, length)
# Spark's F.substring is 1-indexed, so positions map directly.
# ---------------------------------------------------------------------------
DNB_LAYOUT = [
    ("duns_number",                  1,   9),
    ("sequence_number_of_ddm",      10,   3),
    ("gender_code",                 13,   1),
    ("ddm_prefix",                  14,  10),
    ("ddm_last_name",               24,  16),
    ("ddm_first_name",              40,  13),
    ("ddm_middle_initial",          53,   1),
    ("ddm_suffix",                  54,   3),
    ("ddm_name",                    57,  35),
    ("ddm_title",                   92,  30),
    ("mrc1",                       122,   4),
    ("mrc2",                       126,   4),
    ("mrc3",                       130,   4),
    ("mrc4",                       134,   4),
    ("mrc5",                       138,   4),
    ("mrc6",                       142,   4),
    ("legal_business_name",        146,  30),
    ("tradestyle_name",            176,  30),
    ("physical_street_address",    206,  25),
    ("physical_city",              231,  20),
    ("physical_state",             251,   2),
    ("physical_zip5",              253,   5),
    ("physical_zip4",              258,   4),
    ("misc_address_field",         262,  15),
    ("carrier_route_code",         277,   4),
    ("delivery_point_bar_code",    281,   3),
    ("dnb_state_code",             284,   2),
    ("dnb_county_code",            286,   3),
    ("dnb_city_code",              289,   4),
    ("dnb_smsa_code",              293,   3),
    ("fips_state_code",            296,   2),
    ("fips_county_code",           298,   3),
    ("fips_msa_code",              301,   4),
    ("mailing_address",            305,  25),
    ("mailing_city",               330,  20),
    ("mailing_state",              350,   2),
    ("mailing_zip5",               352,   5),
    ("mailing_zip4",               357,   4),
    ("year_business_started",      361,   4),
    ("business_status_code",       365,   1),
    ("employee_range_code",        366,   1),
    ("sales_volume_range_code",    367,   1),
    ("cottage_indicator",          368,   1),
    ("key",                        369,   9),
    ("ceo_gender_code",            378,   1),
    ("ceo_name_prefix",            379,  10),
    ("ceo_name",                   389,  30),
    ("ceo_name_suffix",            419,   3),
    ("ceo_title",                  422,  30),
    ("area_code",                  452,   3),
    ("telephone_number",           455,   7),
    ("fax_number",                 462,  10),
    ("blank",                      472,  10),
    ("minority_ind",               482,   1),
    ("minority_codes",             483,   3),
    ("small_business_ind",         486,   1),
    ("women_owned_ind",            487,   1),
    ("primary_4_digit_sic",        488,   4),
    ("secondary_4_digit_sic",      492,   4),
    ("tertiary_4_digit_sic",       496,   4),
    ("sic_4digit_4th",             500,   4),
    ("sic_4digit_5th",             504,   4),
    ("sic_4digit_6th",             508,   4),
    ("sic_8digit_1st",             512,   8),
    ("sic_8digit_2nd",             520,   8),
    ("sic_8digit_3rd",             528,   8),
    ("sic_8digit_4th",             536,   8),
    ("sic_8digit_secondary_1st",   544,   8),
    ("sic_8digit_secondary_2nd",   552,   8),
    ("sic_8digit_secondary_3rd",   560,   8),
    ("sic_8digit_secondary_4th",   568,   8),
    ("sic_8digit_tertiary_1st",    576,   8),
    ("sic_8digit_tertiary_2nd",    584,   8),
    ("sic_8digit_tertiary_3rd",    592,   8),
    ("sic_8digit_tertiary_4th",    600,   8),
    ("sic_8digit_fourth_1st",      608,   8),
    ("sic_8digit_fourth_2nd",      616,   8),
    ("sic_8digit_fourth_3rd",      624,   8),
    ("sic_8digit_fourth_4th",      632,   8),
    ("sic_8digit_fifth_1st",       640,   8),
    ("sic_8digit_fifth_2nd",       648,   8),
    ("sic_8digit_fifth_3rd",       656,   8),
    ("sic_8digit_fifth_4th",       664,   8),
    ("sic_8digit_sixth_1st",       672,   8),
    ("sic_8digit_sixth_2nd",       680,   8),
    ("sic_8digit_sixth_3rd",       688,   8),
    ("sic_8digit_sixth_4th",       696,   8),
    ("report_date",                704,   6),
    ("franchise_code_1",           710,   8),
    ("franchise_code_2",           718,   8),
    ("franchise_code_3",           726,   8),
    ("franchise_code_4",           734,   8),
    ("franchise_code_5",           742,   8),
    ("franchise_code_6",           750,   8),
    ("franchise_code_indicator",   758,   1),
    ("latitude",                   759,   7),
    ("longitude",                  766,   8),
    ("accuracy_level_code",        774,   1),
    ("class_2_keycode",            775,   1),
    ("number_of_urls",             776,   5),
    ("record_class_type",          781,   1),
]

# ---------------------------------------------------------------------------
# Job arguments
# ---------------------------------------------------------------------------
required_args = [
    "JOB_NAME",
    "INPUT_S3_PATH",
    "OUTPUT_S3_PATH",
]
optional_args = []
if any(arg.startswith("--STATE_FILTER") for arg in sys.argv):
    optional_args.append("STATE_FILTER")
if any(arg.startswith("--COALESCE") for arg in sys.argv):
    optional_args.append("COALESCE")

args = getResolvedOptions(sys.argv, required_args + optional_args)

INPUT_S3_PATH  = args["INPUT_S3_PATH"]
OUTPUT_S3_PATH = args["OUTPUT_S3_PATH"]
STATE_FILTER   = args.get("STATE_FILTER")
COALESCE       = int(args.get("COALESCE", "1"))

if STATE_FILTER and (len(STATE_FILTER) != 2 or not STATE_FILTER.isalpha()):
    raise ValueError(f"STATE_FILTER must be a 2-letter state code, got: {STATE_FILTER!r}")

# ---------------------------------------------------------------------------
# Glue / Spark context
# ---------------------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info("[INIT] Job name:       %s", args["JOB_NAME"])
logger.info("[INIT] INPUT_S3_PATH:  %s", INPUT_S3_PATH)
logger.info("[INIT] OUTPUT_S3_PATH: %s", OUTPUT_S3_PATH)
logger.info("[INIT] STATE_FILTER:   %s", STATE_FILTER or "(none)")
logger.info("[INIT] COALESCE:       %d", COALESCE)
logger.info("[INIT] Layout columns: %d", len(DNB_LAYOUT))

try:
    # ------------------------------------------------------------------
    # STEP 1 - Read raw lines from S3
    # ------------------------------------------------------------------
    logger.info("[STEP 1] Reading fixed-width input from %s", INPUT_S3_PATH)

    lines_df = spark.read.text(INPUT_S3_PATH)

    if lines_df.rdd.isEmpty():
        raise RuntimeError(f"No data found at INPUT_S3_PATH={INPUT_S3_PATH}")

    total_lines = lines_df.count()
    logger.info("[STEP 1] Read %d line(s)", total_lines)

    # ------------------------------------------------------------------
    # STEP 2 - Parse fixed-width fields via F.substring
    # ------------------------------------------------------------------
    logger.info("[STEP 2] Parsing %d fixed-width columns", len(DNB_LAYOUT))

    select_exprs = [
        F.substring(F.col("value"), start, length).alias(name)
        for name, start, length in DNB_LAYOUT
    ]
    df_parsed = lines_df.select(select_exprs)

    logger.info("[STEP 2] Parsed schema:")
    df_parsed.printSchema()

    # ------------------------------------------------------------------
    # STEP 3 - Trim whitespace and convert blanks to NULL
    # ------------------------------------------------------------------
    logger.info("[STEP 3] Trimming whitespace and converting blanks to NULL")

    for col_name in df_parsed.columns:
        df_parsed = df_parsed.withColumn(
            col_name,
            F.when(
                F.length(F.trim(F.col(col_name))) == 0,
                F.lit(None),
            ).otherwise(F.trim(F.col(col_name))),
        )

    # Drop the "blank" placeholder column (positions 472-481)
    df_clean = df_parsed.drop("blank")

    clean_count = df_clean.count()
    logger.info("[STEP 3] Cleaned %d row(s), %d columns", clean_count, len(df_clean.columns))

    # ------------------------------------------------------------------
    # STEP 4 - Optional state filter
    # ------------------------------------------------------------------
    if STATE_FILTER:
        logger.info("[STEP 4] Filtering to physical_state = %s", STATE_FILTER)
        df_clean = df_clean.filter(
            F.upper(F.col("physical_state")) == STATE_FILTER.upper()
        )
        filtered_count = df_clean.count()
        logger.info("[STEP 4] %d row(s) after state filter", filtered_count)
    else:
        logger.info("[STEP 4] No STATE_FILTER — skipping")

    # ------------------------------------------------------------------
    # STEP 5 - Write CSV to S3
    # ------------------------------------------------------------------
    logger.info("[STEP 5] Writing CSV to %s (coalesce=%d)", OUTPUT_S3_PATH, COALESCE)

    df_clean.coalesce(COALESCE).write.csv(
        OUTPUT_S3_PATH,
        header=True,
        mode="overwrite",
        quote='"',
        escape='"',
    )

    logger.info("[STEP 5] CSV write complete")

    job.commit()
    logger.info("[DONE] Job committed successfully")

except Exception as e:
    logger.error("[FATAL] Job failed: %s", repr(e))
    raise
