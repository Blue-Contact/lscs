"""
Combined Glue Job: List Import (S3 delimited text) + Consumer Match (fuzzy person matching)

This script merges the ingest pattern from glue/list_import.py with the matching logic from
glue/consumer_match.py.

Contract (consumer_match style output):
  Required:
    - JOB_NAME (Glue)
    - INPUT_S3_PATH
    - MATCH_TABLE
    - OUTPUT_PATH
    - OUTPUT_TABLE  (database.table)
    - MATCH_THRESHOLD
    - INPUT_COLUMN_MAPPING (JSON mapping from standard names to input file columns)
  Optional:
    - INPUT_DELIMITER (defaults to ",", supports "tab", "pipe", or escape sequences like "\\t")
    - STATE_FILTER (2-letter code; filters both input and match datasets for testing)

Output:
  Writes Parquet to OUTPUT_PATH and creates/ensures Glue table OUTPUT_TABLE at that location.
  Output includes all imported columns plus match result columns.
"""

import sys
import csv
import json
import re
import time
import logging
from io import StringIO

from rapidfuzz import fuzz
import jellyfish
from nicknames import NickNamer

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def normalize_delimiter(raw_value: str) -> str:
    """
    Convert user input (e.g., pipe, tab, \\t) to the actual delimiter character.
    Defaults to the raw string if no special handling is required.
    """
    if raw_value is None:
        return ","

    token = raw_value.lower()
    if token == "tab":
        return "\t"
    if token == "pipe":
        return "|"

    # Allow escape sequences like "\\t" to be supplied via job parameters.
    try:
        return raw_value.encode("utf-8").decode("unicode_escape")
    except UnicodeDecodeError:
        return raw_value


def sql_ident(name: str) -> str:
    """Safely quote a Spark SQL identifier with backticks."""
    if name is None:
        raise ValueError("Identifier cannot be None")
    # Escape any backticks by doubling them
    escaped = str(name).replace("`", "``")
    return f"`{escaped}`"


def _catalog_safe_name(raw: str) -> str:
    """
    Approximate Glue/Athena-friendly column names:
      - lowercase
      - replace non [a-z0-9_] with _
      - collapse underscores
      - trim underscores
      - prefix leading digits with _
    """
    if raw is None:
        return ""
    s = str(raw).strip().lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if s and s[0].isdigit():
        s = "_" + s
    return s


def _make_unique(names: list[str]) -> list[str]:
    """
    Make names unique by suffixing _2, _3, ... (never uses '#').
    Empty names become col_<index>.
    """
    seen: dict[str, int] = {}
    out: list[str] = []
    for i, n in enumerate(names):
        base = (n or "").strip()
        if not base:
            base = f"col_{i+1}"
        k = base
        if k in seen:
            seen[k] += 1
            k = f"{base}_{seen[base]}"
        else:
            seen[k] = 1
        out.append(k)
    return out


class NameMatcher:
    """Class to handle advanced name matching logic with multiple algorithms."""

    def __init__(self):
        self.common_variations = {
            "jr": "junior",
            "sr": "senior",
            "ii": "2",
            "iii": "3",
            "iv": "4",
            "v": "5",
            "vi": "6",
            "vii": "7",
            "viii": "8",
            "ix": "9",
            "x": "10",
            "st": "saint",
            "mt": "mount",
            "dr": "doctor",
            "rev": "reverend",
            "hon": "honorable",
            "prof": "professor",
            "mr": "mister",
            "mrs": "missus",
            "ms": "miss",
            "miss": "miss",
            "mstr": "master",
        }
        self.suffixes = ["jr", "sr", "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x"]
        self.nicknamer = NickNamer()

    def normalize_string(self, s):
        """Normalize string by removing extra whitespace and converting to lowercase."""
        if s is None:
            return ""

        s = str(s)
        s = re.sub(r"\s+", " ", s)
        s = s.strip()
        s = s.lower()

        for short, long in self.common_variations.items():
            s = re.sub(r"\b" + short + r"\b", long, s)

        return s

    def get_rapidfuzz_scores(self, s1, s2):
        """Get scores from various rapidfuzz algorithms."""
        if not s1 or not s2:
            return 0

        if s1 == s2:
            return 100

        best_score = 0
        try:
            ratio = int(fuzz.ratio(s1, s2))
            if ratio > best_score:
                best_score = ratio

            partial = int(fuzz.partial_ratio(s1, s2))
            if partial > best_score:
                best_score = partial

            token_sort = int(fuzz.token_sort_ratio(s1, s2))
            if token_sort > best_score:
                best_score = token_sort

            token_set = int(fuzz.token_set_ratio(s1, s2))
            if token_set > best_score:
                best_score = token_set
        except Exception:
            pass

        return best_score

    def get_phonetic_scores(self, s1, s2):
        """Get scores from various phonetic matching algorithms."""
        if not s1 or not s2:
            return 0

        if s1 == s2:
            return 100

        best_score = 0
        try:
            if jellyfish.soundex(s1) == jellyfish.soundex(s2):
                best_score = max(best_score, 90)
            if jellyfish.metaphone(s1) == jellyfish.metaphone(s2):
                best_score = max(best_score, 90)
            if jellyfish.nysiis(s1) == jellyfish.nysiis(s2):
                best_score = max(best_score, 90)
            if jellyfish.match_rating_codex(s1) == jellyfish.match_rating_codex(s2):
                best_score = max(best_score, 90)
        except Exception:
            pass

        return best_score

    def get_combined_scores(self, s1, s2):
        if not s1 or not s2:
            return 0
        if s1 == s2:
            return 100

        rapidfuzz_score = self.get_rapidfuzz_scores(s1, s2)
        phonetic_score = self.get_phonetic_scores(s1, s2)

        rapidfuzz_int = int(rapidfuzz_score) if rapidfuzz_score is not None else 0
        phonetic_int = int(phonetic_score) if phonetic_score is not None else 0
        return rapidfuzz_int if rapidfuzz_int > phonetic_int else phonetic_int

    def get_best_match_score(self, s1, s2, is_last_name=False, first_name_score=0):
        if not s1 or not s2:
            return 0

        try:
            if s1 == s2:
                return 100

            all_scores = []

            if is_last_name:
                s1_parts = []
                for part in s1.split():
                    s1_parts.extend([p for p in part.split("-") if p])
                s2_parts = []
                for part in s2.split():
                    s2_parts.extend([p for p in part.split("-") if p])

                score = self.get_combined_scores(s1, s2)
                if score > 0:
                    all_scores.append(score)

                for part1 in s1_parts:
                    if not part1:
                        continue
                    score = self.get_combined_scores(part1, s2)
                    if score > 0:
                        all_scores.append(score)
                    for part2 in s2_parts:
                        if not part2:
                            continue
                        score = self.get_combined_scores(part1, part2)
                        if score > 0:
                            all_scores.append(score)

                for part2 in s2_parts:
                    if not part2:
                        continue
                    score = self.get_combined_scores(s1, part2)
                    if score > 0:
                        all_scores.append(score)
            else:
                score = self.get_combined_scores(s1, s2)
                if score > 0:
                    all_scores.append(score)

            if all_scores:
                import builtins

                return builtins.max(all_scores)
            return 0
        except Exception:
            return 0


name_matcher = NameMatcher()


def fuzzy_match_name_with_first_name(name1, name2, is_last_name, first_name_score):
    try:
        if name1 is None or name2 is None:
            return 0

        name1_norm = name_matcher.normalize_string(name1)
        name2_norm = name_matcher.normalize_string(name2)
        score = name_matcher.get_best_match_score(
            name1_norm, name2_norm, is_last_name, first_name_score
        )
        return int(score)
    except Exception:
        return 0


def fuzzy_match_address(addr1, addr2, zip1, zip2):
    if addr1 is None or addr2 is None or zip1 is None or zip2 is None:
        return 0

    if zip1 != zip2:
        return 0

    addr1_norm = name_matcher.normalize_string(addr1)
    addr2_norm = name_matcher.normalize_string(addr2)
    score = name_matcher.get_best_match_score(addr1_norm, addr2_norm)
    return int(score)


# -----------------------------------------------------------------------------------
# Read job parameters
# -----------------------------------------------------------------------------------
required_args = [
    "JOB_NAME",
    "INPUT_S3_PATH",
    "MATCH_TABLE",
    "OUTPUT_PATH",
    "OUTPUT_TABLE",
    "MATCH_THRESHOLD",
    "INPUT_COLUMN_MAPPING",
]

optional_args = []
if any(arg.startswith("--INPUT_DELIMITER") for arg in sys.argv):
    optional_args.append("INPUT_DELIMITER")
if any(arg.startswith("--STATE_FILTER") for arg in sys.argv):
    optional_args.append("STATE_FILTER")

args = getResolvedOptions(sys.argv, required_args + optional_args)

INPUT_S3_PATH = args["INPUT_S3_PATH"]
MATCH_TABLE = args["MATCH_TABLE"]
OUTPUT_PATH = args["OUTPUT_PATH"]
OUTPUT_TABLE = args["OUTPUT_TABLE"]
MATCH_THRESHOLD = int(args["MATCH_THRESHOLD"])
INPUT_COLUMN_MAPPING_RAW = args["INPUT_COLUMN_MAPPING"]
INPUT_DELIMITER = normalize_delimiter(args.get("INPUT_DELIMITER"))
STATE_FILTER = args.get("STATE_FILTER")
if STATE_FILTER:
    STATE_FILTER = STATE_FILTER.upper().strip()
    if len(STATE_FILTER) != 2:
        raise ValueError("STATE_FILTER must be a 2-character state code (e.g., 'WY', 'CA')")

try:
    input_column_mapping = json.loads(INPUT_COLUMN_MAPPING_RAW)
except json.JSONDecodeError as e:
    raise ValueError(f"Invalid JSON in INPUT_COLUMN_MAPPING parameter: {e}")

logger.info(f"[INIT] Job name: {args['JOB_NAME']}")
logger.info(f"[INIT] INPUT_S3_PATH      = {INPUT_S3_PATH}")
logger.info(f"[INIT] INPUT_DELIMITER    = {repr(INPUT_DELIMITER)}")
logger.info(f"[INIT] MATCH_TABLE        = {MATCH_TABLE}")
logger.info(f"[INIT] OUTPUT_PATH        = {OUTPUT_PATH}")
logger.info(f"[INIT] OUTPUT_TABLE       = {OUTPUT_TABLE}")
logger.info(f"[INIT] MATCH_THRESHOLD    = {MATCH_THRESHOLD}")
logger.info(f"[INIT] STATE_FILTER       = {STATE_FILTER}")
logger.info(f"[INIT] INPUT_COLUMN_MAPPING = {input_column_mapping}")


# -----------------------------------------------------------------------------------
# Glue / Spark Context
# -----------------------------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Filled after header parsing so INPUT_COLUMN_MAPPING can refer to either original
# header names (e.g. "FINDER NUMBER") or sanitized names (e.g. "finder_number").
HEADER_ORIG_TO_SAFE: dict[str, str] = {}
HEADER_SAFE_SET: set[str] = set()


def get_input_column_name(standard_col_name: str) -> str:
    mapped = input_column_mapping.get(standard_col_name, standard_col_name)
    # If mapping refers to original header, translate to safe name.
    if mapped in HEADER_ORIG_TO_SAFE:
        return HEADER_ORIG_TO_SAFE[mapped]
    # If mapping already safe, keep it.
    if mapped in HEADER_SAFE_SET:
        return mapped
    # Best-effort: if they passed something close, try sanitizing.
    candidate = _catalog_safe_name(mapped)
    if candidate in HEADER_SAFE_SET:
        return candidate
    return mapped


metrics_start = time.time()

try:
    # -----------------------------------------------------------------------------------
    # 1) Read file(s) as plain text (NO spark.read.csv, NO schema inference)
    # -----------------------------------------------------------------------------------
    logger.info("[STEP 1] Reading input as plain text with spark.read.text...")
    lines_df = spark.read.text(INPUT_S3_PATH)

    if lines_df.rdd.isEmpty():
        raise Exception(f"No data found at INPUT_S3_PATH={INPUT_S3_PATH}")

    total_lines = lines_df.count()
    logger.info(f"[STEP 1] Read {total_lines} total line(s) from input path.")
    logger.info("[STEP 1] Sample of first 5 raw lines:")
    lines_df.show(5, truncate=False)

    # -----------------------------------------------------------------------------------
    # 2) Extract header row and build DDL schema string (all STRING)
    # -----------------------------------------------------------------------------------
    logger.info("[STEP 2] Extracting header row and building dynamic schema DDL...")
    header_row = lines_df.first()[0]
    logger.info(f"[STEP 2] Header row (truncated to 200 chars): {header_row[:200]}")

    reader = csv.reader(StringIO(header_row), delimiter=INPUT_DELIMITER)
    header_cols = next(reader)
    header_cols = [col.strip().strip('"') for col in header_cols]
    logger.info(f"[STEP 2] Detected {len(header_cols)} column(s) from header.")
    logger.info(f"[STEP 2] First 10 column names: {header_cols[:10]}")

    # Build catalog-safe column names to prevent Glue from appending `#<n>` during catalog updates.
    safe_cols = _make_unique([_catalog_safe_name(c) for c in header_cols])
    HEADER_ORIG_TO_SAFE = dict(zip(header_cols, safe_cols))
    HEADER_SAFE_SET = set(safe_cols)

    mapping_preview = list(zip(header_cols[:20], safe_cols[:20]))
    logger.info(f"[STEP 2] Column name mapping (original -> safe), first 20: {mapping_preview}")

    schema_ddl = ", ".join(f"{sql_ident(name)} STRING" for name in safe_cols)
    logger.info(f"[STEP 2] Schema DDL (truncated to 300 chars): {schema_ddl[:300]}")

    # -----------------------------------------------------------------------------------
    # 3) Drop header lines and parse rows with from_csv()
    # -----------------------------------------------------------------------------------
    logger.info("[STEP 3] Filtering out header row(s) and parsing CSV lines with from_csv...")
    data_df = lines_df.filter(F.col("value") != header_row)
    data_line_count = data_df.count()
    logger.info(f"[STEP 3] Data lines after removing header: {data_line_count}")
    if data_line_count == 0:
        raise Exception("No data rows found after removing header.")

    parsed = data_df.select(
        F.from_csv(
            F.col("value"),
            schema_ddl,
            {
                "delimiter": INPUT_DELIMITER,
                "quote": '"',
                "escape": '"',
            },
        ).alias("parsed")
    )
    df_raw = parsed.select("parsed.*")
    raw_count = df_raw.count()
    logger.info(f"[STEP 3] Parsed {raw_count} row(s) into columns from CSV.")
    logger.info("[STEP 3] Raw parsed schema:")
    df_raw.printSchema()

    # -----------------------------------------------------------------------------------
    # 4) Trim whitespace AND convert blanks -> NULL
    # -----------------------------------------------------------------------------------
    logger.info("[STEP 4] Trimming whitespace and converting blank strings to NULL...")
    df_clean = df_raw
    for c in df_clean.columns:
        df_clean = df_clean.withColumn(
            c, F.when(F.length(F.trim(F.col(c))) == 0, F.lit(None)).otherwise(F.trim(F.col(c)))
        )

    clean_count = df_clean.count()
    logger.info(f"[STEP 4] Cleaned {clean_count} row(s).")
    logger.info("[STEP 4] Sample of first 5 cleaned rows:")
    df_clean.show(5, truncate=False)

    # -----------------------------------------------------------------------------------
    # 5) Validate mapping columns exist
    # -----------------------------------------------------------------------------------
    required_standard = ["first_name", "last_name", "address", "city", "state", "zip", "zip4"]
    missing = []
    for std in required_standard:
        actual = get_input_column_name(std)
        if actual not in df_clean.columns:
            missing.append(f"{std}->{actual}")
    if missing:
        raise ValueError(
            "Imported file is missing required columns for matching (standard->input): "
            + ", ".join(missing)
        )

    # Optional: filter input by state for test runs
    if STATE_FILTER:
        input_state_col = get_input_column_name("state")
        logger.info(f"[STEP 5] Filtering imported input to state={STATE_FILTER} using column={input_state_col}")
        df_clean = df_clean.filter(F.upper(F.col(input_state_col)) == F.lit(STATE_FILTER))

    # -----------------------------------------------------------------------------------
    # 6) Prepare input_df and match_df (normalized columns)
    # -----------------------------------------------------------------------------------
    logger.info("[STEP 6] Preparing normalized columns and loading match table...")

    input_first_col = get_input_column_name("first_name")
    input_last_col = get_input_column_name("last_name")
    input_address_col = get_input_column_name("address")
    input_zip_col = get_input_column_name("zip")
    input_zip4_col = get_input_column_name("zip4")

    input_zip_clean = F.trim(F.coalesce(F.col(input_zip_col).cast("string"), F.lit("")))
    input_zip4_clean = F.trim(F.coalesce(F.col(input_zip4_col).cast("string"), F.lit("")))
    input_zip_5 = F.substring(input_zip_clean, 1, 5)
    input_zip4_4 = F.substring(input_zip4_clean, 1, 4)

    input_df = df_clean.select(
        "*",
        F.coalesce(F.col(input_first_col), F.lit("")).alias("first_name_norm"),
        F.coalesce(F.col(input_last_col), F.lit("")).alias("last_name_norm"),
        F.coalesce(F.col(input_address_col), F.lit("")).alias("address_norm"),
        F.when(F.length(input_zip_5) == 0, F.lit(""))
        .otherwise(F.lpad(input_zip_5, 5, "0"))
        .alias("zip_norm"),
        F.when(F.length(input_zip4_4) == 0, F.lit(""))
        .otherwise(F.lpad(input_zip4_4, 4, "0"))
        .alias("zip4_norm"),
    )

    if STATE_FILTER:
        match_df = spark.sql(
            f"SELECT * FROM {MATCH_TABLE} WHERE state = '{STATE_FILTER}'"
        )
    else:
        match_df = spark.sql(f"SELECT * FROM {MATCH_TABLE}")

    # Normalize match dataset
    match_zip_clean = F.trim(F.coalesce(F.col("zip").cast("string"), F.lit("")))
    match_zip4_clean = F.trim(F.coalesce(F.col("zip4").cast("string"), F.lit("")))
    match_zip_5 = F.substring(match_zip_clean, 1, 5)
    match_zip4_4 = F.substring(match_zip4_clean, 1, 4)

    match_df = match_df.select(
        "*",
        F.coalesce(F.col("first_name"), F.lit("")).alias("first_name_norm"),
        F.coalesce(F.col("last_name"), F.lit("")).alias("last_name_norm"),
        F.coalesce(F.col("address"), F.lit("")).alias("address_norm"),
        F.when(F.length(match_zip_5) == 0, F.lit(""))
        .otherwise(F.lpad(match_zip_5, 5, "0"))
        .alias("zip_norm"),
        F.when(F.length(match_zip4_4) == 0, F.lit(""))
        .otherwise(F.lpad(match_zip4_4, 4, "0"))
        .alias("zip4_norm"),
    )

    input_df.createOrReplaceTempView("input_data")
    match_df.createOrReplaceTempView("match_data")

    input_count = input_df.count()
    match_count = match_df.count()
    logger.info(f"[STEP 6] Input count: {input_count:,}")
    logger.info(f"[STEP 6] Match table count: {match_count:,}")

    # -----------------------------------------------------------------------------------
    # 7) Register UDFs and run matching SQL
    # -----------------------------------------------------------------------------------
    logger.info("[STEP 7] Registering UDFs and performing fuzzy matching...")
    spark.udf.register("fuzzy_name_match_udf", fuzzy_match_name_with_first_name, IntegerType())
    spark.udf.register("fuzzy_address_match_udf", fuzzy_match_address, IntegerType())

    match_threshold = int(MATCH_THRESHOLD)
    household_threshold = match_threshold

    input_columns = [c for c in input_df.columns if not c.endswith("_norm")]
    select_input_cols = ", ".join([f"r.{sql_ident(c)}" for c in input_columns])

    part_cols = ", ".join(
        [
            sql_ident(input_first_col),
            sql_ident(input_last_col),
            sql_ident(input_address_col),
            sql_ident(input_zip_col),
            sql_ident(input_zip4_col),
        ]
    )

    result_df = spark.sql(
        f"""
        WITH first_name_matches AS (
            SELECT
                i.*,
                c.id                         AS match_id,
                c.first_name                 AS match_first_name,
                c.first_name_norm            AS match_first_name_norm,
                c.last_name                  AS match_last_name,
                c.last_name_norm             AS match_last_name_norm,
                c.address                    AS match_address,
                c.address_norm               AS match_address_norm,
                c.zip                        AS match_zip,
                c.zip4                       AS match_zip4,
                c.zip_norm                   AS match_zip_norm,
                c.zip4_norm                  AS match_zip4_norm,
                CASE
                    WHEN i.first_name_norm = '' OR c.first_name_norm = '' THEN 0
                    ELSE fuzzy_name_match_udf(i.first_name_norm, c.first_name_norm, false, 0)
                END                          AS raw_first_name_score
            FROM input_data i
            LEFT JOIN match_data c
              ON i.zip_norm = c.zip_norm
             AND i.zip4_norm = c.zip4_norm
        ),
        address_matches AS (
            SELECT
                f.*,
                CASE
                    WHEN f.last_name_norm = '' OR f.match_last_name_norm = '' THEN 0
                    ELSE fuzzy_name_match_udf(
                           f.last_name_norm,
                           f.match_last_name_norm,
                           true,
                           f.raw_first_name_score
                         )
                END                          AS raw_last_name_score,
                CASE
                    WHEN f.address_norm = ''
                      OR f.match_address_norm = ''
                      OR f.zip_norm = ''
                      OR f.match_zip_norm = ''
                    THEN 0
                    ELSE fuzzy_address_match_udf(
                           f.address_norm,
                           f.match_address_norm,
                           f.zip_norm,
                           f.match_zip_norm
                         )
                END                          AS raw_address_score
            FROM first_name_matches f
        ),
        match_types AS (
            SELECT
                *,
                CASE
                    WHEN raw_first_name_score >= {match_threshold}
                      AND raw_last_name_score >= {match_threshold}
                      AND raw_address_score >= {match_threshold}
                    THEN 'INDIVIDUAL_MATCH'
                    WHEN raw_last_name_score >= {household_threshold}
                      AND raw_address_score >= {match_threshold}
                    THEN 'HOUSEHOLD_MATCH'
                    WHEN raw_address_score >= {match_threshold}
                    THEN 'ADDRESS_MATCH'
                    ELSE 'NO_MATCH'
                END                          AS match_type,
                CASE
                    WHEN raw_first_name_score >= {match_threshold}
                      AND raw_last_name_score >= {match_threshold}
                      AND raw_address_score >= {match_threshold}
                    THEN (raw_first_name_score + raw_last_name_score + raw_address_score) / 3
                    WHEN raw_last_name_score >= {household_threshold}
                      AND raw_address_score >= {match_threshold}
                    THEN (raw_last_name_score + raw_address_score) / 2
                    ELSE raw_address_score
                END                          AS match_overall_score
            FROM address_matches
        ),
        ranked_matches AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY {part_cols}
                    ORDER BY
                        CASE match_type
                            WHEN 'INDIVIDUAL_MATCH' THEN 1
                            WHEN 'HOUSEHOLD_MATCH' THEN 2
                            WHEN 'ADDRESS_MATCH' THEN 3
                            ELSE 4
                        END,
                        match_overall_score DESC
                ) AS match_rank
            FROM match_types
            WHERE match_type != 'NO_MATCH'
        )
        SELECT
            {select_input_cols},
            r.match_id,
            r.match_first_name,
            r.match_last_name,
            r.match_address,
            r.match_zip,
            r.match_zip4,
            r.raw_first_name_score    AS match_first_name_score,
            r.raw_last_name_score     AS match_last_name_score,
            r.raw_address_score       AS match_address_score,
            r.match_type,
            r.match_overall_score
        FROM ranked_matches r
        WHERE r.match_rank = 1
        """
    )

    result_count = result_df.count()
    logger.info(f"[STEP 7] Result count: {result_count:,}")

    # -----------------------------------------------------------------------------------
    # 8) Write output and register Glue table
    # -----------------------------------------------------------------------------------
    logger.info("[STEP 8] Writing results to Parquet and registering output table...")
    result_df.write.format("parquet").option("compression", "snappy").mode("overwrite").save(
        OUTPUT_PATH
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {OUTPUT_TABLE}
        USING parquet
        LOCATION '{OUTPUT_PATH}'
        """
    )

    elapsed = time.time() - metrics_start
    logger.info(f"[DONE] Job completed successfully in {elapsed:.2f}s")
    job.commit()

except Exception as e:
    logger.error(f"[FATAL] Job failed with error: {str(e)}")
    raise


