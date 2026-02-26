"""
Combined Glue Job: List Import (S3 delimited text) + Consumer Key Match

This script merges the ingest pattern from glue/list_import_and_match.py with
key-based matching against a consumer key table (email/phone/address).

Contract (key match output):
  Required:
    - JOB_NAME (Glue)
    - INPUT_S3_PATH
    - MATCH_TABLE
    - OUTPUT_PATH
    - OUTPUT_TABLE (datasource.database.table)
    - MATCH_THRESHOLD
    - INPUT_COLUMN_MAPPING (JSON mapping from standard names to input file columns)
  Optional:
    - INPUT_DELIMITER (defaults to ",", supports "tab", "pipe", or escape sequences like "\\t")
    - STATE_FILTER (2-letter code; filters both input and match datasets for testing)
    - MATCH_APPEND_COLUMNS (default: match columns only; use "ALL" or comma list)

INPUT_COLUMN_MAPPING:
  Required standard keys: first_name, last_name
  Key sources:
    - email: may be a comma-separated list or JSON array of column names
    - phone: may be a comma-separated list or JSON array of column names
    - address + zip: used to build address key "<street> <zip5>"
  At least one of email / phone / address+zip must be present.
  Example:
    {
      "first_name": "first",
      "last_name": "last",
      "email": "email,alt_email",
      "phone": ["phone", "mobile"],
      "address": "street",
      "zip": "zip"
    }

Matching:
  - Exact key match on normalized key value and key_type (email/phone/address)
  - Name validation uses fuzzy match UDFs from list_import_and_match.py
  - Match types:
      INDIVIDUAL_MATCH: first + last name scores >= MATCH_THRESHOLD
      HOUSEHOLD_MATCH: last name score >= MATCH_THRESHOLD, first name below
      KEY_MATCH: key matched but name threshold not met
  - Adds match_date (run date) to output
"""

import sys
import csv
import json
import re
import time
import logging
from io import StringIO
from typing import Optional

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


def parse_match_append_columns(raw_value: Optional[str]) -> tuple[str, list[str]]:
    """
    Parse MATCH_APPEND_COLUMNS.
    Returns (mode, columns) where mode is: default | all | list.
    """
    if raw_value is None:
        return "default", []
    token = str(raw_value).strip()
    if not token:
        return "default", []
    if token.upper() == "ALL":
        return "all", []
    cols = [c.strip() for c in token.split(",") if c.strip()]
    if not cols:
        return "default", []
    return "list", cols


def make_unique_name(base: str, taken: set[str]) -> str:
    """Return a unique name, suffixing _2, _3, ... if needed (case-insensitive)."""
    if base.lower() not in taken:
        return base
    suffix = 2
    candidate = f"{base}_{suffix}"
    while candidate.lower() in taken:
        suffix += 1
        candidate = f"{base}_{suffix}"
    return candidate


def parse_column_list(raw_value) -> list[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        items = raw_value
    else:
        items = str(raw_value).split(",")
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        name = str(item).strip()
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def normalize_email(col):
    cleaned = F.lower(F.trim(F.coalesce(col, F.lit(""))))
    return F.when(F.length(cleaned) == 0, F.lit(None)).otherwise(cleaned)


def normalize_phone(col):
    digits = F.regexp_replace(F.coalesce(col, F.lit("")), r"\D+", "")
    digits = F.when(
        (F.length(digits) == 11) & (F.substring(digits, 1, 1) == F.lit("1")),
        F.substring(digits, 2, 10),
    ).otherwise(digits)
    return F.when(F.length(digits) == 0, F.lit(None)).otherwise(digits)


def normalize_zip5(col):
    digits = F.regexp_replace(F.coalesce(col, F.lit("")), r"\D+", "")
    zip5 = F.substring(digits, 1, 5)
    zip5 = F.when(F.length(zip5) == 0, F.lit(None)).otherwise(F.lpad(zip5, 5, "0"))
    return zip5


def normalize_text(col):
    cleaned = F.regexp_replace(F.lower(F.trim(F.coalesce(col, F.lit("")))), r"\s+", " ")
    return F.when(F.length(cleaned) == 0, F.lit(None)).otherwise(cleaned)


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
if any(arg.startswith("--MATCH_APPEND_COLUMNS") for arg in sys.argv):
    optional_args.append("MATCH_APPEND_COLUMNS")

args = getResolvedOptions(sys.argv, required_args + optional_args)

INPUT_S3_PATH = args["INPUT_S3_PATH"]
MATCH_TABLE = args["MATCH_TABLE"]
OUTPUT_PATH = args["OUTPUT_PATH"]
OUTPUT_TABLE = args["OUTPUT_TABLE"]
MATCH_THRESHOLD = int(args["MATCH_THRESHOLD"])
INPUT_COLUMN_MAPPING_RAW = args["INPUT_COLUMN_MAPPING"]
INPUT_DELIMITER = normalize_delimiter(args.get("INPUT_DELIMITER"))
STATE_FILTER = args.get("STATE_FILTER")
MATCH_APPEND_COLUMNS_RAW = args.get("MATCH_APPEND_COLUMNS")
match_append_mode, match_append_requested = parse_match_append_columns(MATCH_APPEND_COLUMNS_RAW)

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
if match_append_mode == "default":
    logger.info("[INIT] MATCH_APPEND_COLUMNS = default (match columns only)")
elif match_append_mode == "all":
    logger.info("[INIT] MATCH_APPEND_COLUMNS = ALL (append all match-table columns)")
else:
    logger.info(
        f"[INIT] MATCH_APPEND_COLUMNS = list ({len(match_append_requested)}): "
        f"{match_append_requested}"
    )


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
    if mapped in HEADER_ORIG_TO_SAFE:
        return HEADER_ORIG_TO_SAFE[mapped]
    if mapped in HEADER_SAFE_SET:
        return mapped
    candidate = _catalog_safe_name(mapped)
    if candidate in HEADER_SAFE_SET:
        return candidate
    return mapped


def resolve_input_column(raw_name: str) -> str:
    if raw_name is None:
        return ""
    if raw_name in HEADER_ORIG_TO_SAFE:
        return HEADER_ORIG_TO_SAFE[raw_name]
    if raw_name in HEADER_SAFE_SET:
        return raw_name
    candidate = _catalog_safe_name(raw_name)
    if candidate in HEADER_SAFE_SET:
        return candidate
    return raw_name


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
    # 5) Validate mapping columns and build key source lists
    # -----------------------------------------------------------------------------------
    logger.info("[STEP 5] Validating mapping columns and building key sources...")
    required_standard = ["first_name", "last_name"]
    missing_required = []
    for std in required_standard:
        actual = get_input_column_name(std)
        if actual not in df_clean.columns:
            missing_required.append(f"{std}->{actual}")
    if missing_required:
        raise ValueError(
            "Imported file is missing required columns (standard->input): "
            + ", ".join(missing_required)
        )

    input_first_col = get_input_column_name("first_name")
    input_last_col = get_input_column_name("last_name")

    email_raw = input_column_mapping.get("email")
    if email_raw is None and "email" in df_clean.columns:
        email_raw = "email"
    email_cols = [resolve_input_column(c) for c in parse_column_list(email_raw)]

    phone_raw = input_column_mapping.get("phone")
    if phone_raw is None and "phone" in df_clean.columns:
        phone_raw = "phone"
    phone_cols = [resolve_input_column(c) for c in parse_column_list(phone_raw)]

    address_col = None
    zip_col = None
    if "address" in input_column_mapping or "address" in df_clean.columns:
        address_col = get_input_column_name("address")
    if "zip" in input_column_mapping or "zip" in df_clean.columns:
        zip_col = get_input_column_name("zip")

    if (address_col and not zip_col) or (zip_col and not address_col):
        raise ValueError("Address key requires BOTH address and zip columns in INPUT_COLUMN_MAPPING.")

    missing_keys = []
    missing_keys.extend([c for c in email_cols if c not in df_clean.columns])
    missing_keys.extend([c for c in phone_cols if c not in df_clean.columns])
    if address_col and address_col not in df_clean.columns:
        missing_keys.append(address_col)
    if zip_col and zip_col not in df_clean.columns:
        missing_keys.append(zip_col)
    if missing_keys:
        raise ValueError(
            "Input file is missing key columns referenced in INPUT_COLUMN_MAPPING: "
            + ", ".join(sorted(set(missing_keys)))
        )

    if not email_cols and not phone_cols and not (address_col and zip_col):
        raise ValueError(
            "At least one key source is required: email, phone, or address+zip."
        )

    logger.info(
        f"[STEP 5] Key sources: email={email_cols}, phone={phone_cols}, "
        f"address={address_col}, zip={zip_col}"
    )

    # Optional: filter input by state for test runs
    if STATE_FILTER:
        input_state_col = get_input_column_name("state")
        if input_state_col not in df_clean.columns:
            raise ValueError(
                f"STATE_FILTER provided but state column not found in input: {input_state_col}"
            )
        logger.info(
            f"[STEP 5] Filtering imported input to state={STATE_FILTER} using column={input_state_col}"
        )
        df_clean = df_clean.filter(F.upper(F.col(input_state_col)) == F.lit(STATE_FILTER))

    # -----------------------------------------------------------------------------------
    # 6) Prepare input keys and load match table
    # -----------------------------------------------------------------------------------
    logger.info("[STEP 6] Building input keys and loading match table...")

    existing_cols_lower = {c.lower() for c in df_clean.columns}
    input_row_id_col = make_unique_name("__input_row_id", existing_cols_lower)
    input_with_id = df_clean.withColumn(input_row_id_col, F.monotonically_increasing_id())

    key_structs = []
    for idx, col_name in enumerate(email_cols):
        key_structs.append(
            F.struct(
                F.lit("email").alias("key_type"),
                normalize_email(F.col(col_name)).alias("key_value"),
                F.lit(1).alias("key_priority"),
                F.lit(idx).alias("key_order"),
            )
        )
    for idx, col_name in enumerate(phone_cols):
        key_structs.append(
            F.struct(
                F.lit("phone").alias("key_type"),
                normalize_phone(F.col(col_name)).alias("key_value"),
                F.lit(2).alias("key_priority"),
                F.lit(idx).alias("key_order"),
            )
        )
    if address_col and zip_col:
        address_norm = normalize_text(F.col(address_col))
        zip5 = normalize_zip5(F.col(zip_col))
        address_key = F.when(
            address_norm.isNotNull() & zip5.isNotNull(),
            F.concat_ws(" ", address_norm, zip5),
        )
        key_structs.append(
            F.struct(
                F.lit("address").alias("key_type"),
                address_key.alias("key_value"),
                F.lit(3).alias("key_priority"),
                F.lit(0).alias("key_order"),
            )
        )

    input_keys_df = (
        input_with_id
        .withColumn("input_keys", F.array(*key_structs))
        .withColumn("input_key", F.explode("input_keys"))
        .select(
            F.col(input_row_id_col).alias("input_row_id"),
            F.col("input_key.key_type").alias("key_type"),
            F.col("input_key.key_value").alias("key_value"),
            F.col("input_key.key_priority").alias("key_priority"),
            F.col("input_key.key_order").alias("key_order"),
            F.col(input_first_col).alias("input_first_name"),
            F.col(input_last_col).alias("input_last_name"),
        )
        .filter(F.col("key_value").isNotNull())
    )

    match_base_df = spark.sql(f"SELECT * FROM {MATCH_TABLE}")
    if STATE_FILTER:
        if "std_state" in match_base_df.columns:
            match_base_df = match_base_df.filter(F.upper(F.col("std_state")) == F.lit(STATE_FILTER))
        elif "state" in match_base_df.columns:
            match_base_df = match_base_df.filter(F.upper(F.col("state")) == F.lit(STATE_FILTER))
        else:
            raise ValueError(
                "STATE_FILTER provided but match table lacks std_state or state column."
            )

    match_base_columns = match_base_df.columns
    match_col_map = {c.lower(): c for c in match_base_columns}

    def pick_match_col(*candidates):
        for cand in candidates:
            actual = match_col_map.get(cand)
            if actual:
                return actual
        return None

    match_key_col = pick_match_col("key")
    match_key_type_col = pick_match_col("key_type")
    match_consumer_id_col = pick_match_col("consumer_id")
    match_uuid_col = pick_match_col("uuid")
    match_first_col = pick_match_col("first", "first_name")
    match_last_col = pick_match_col("last", "last_name")

    missing_match_cols = []
    if not match_key_col:
        missing_match_cols.append("key")
    if not match_key_type_col:
        missing_match_cols.append("key_type")
    if not match_consumer_id_col:
        missing_match_cols.append("consumer_id")
    if not match_uuid_col:
        missing_match_cols.append("uuid")
    if not match_first_col:
        missing_match_cols.append("first/first_name")
    if not match_last_col:
        missing_match_cols.append("last/last_name")
    if missing_match_cols:
        raise ValueError(
            "Match table is missing required columns: " + ", ".join(missing_match_cols)
        )

    match_df = match_base_df.withColumn("match_key_type", F.lower(F.col(match_key_type_col)))
    match_df = match_df.withColumn(
        "match_key_norm",
        F.when(F.col("match_key_type") == F.lit("email"), normalize_email(F.col(match_key_col)))
        .when(F.col("match_key_type") == F.lit("phone"), normalize_phone(F.col(match_key_col)))
        .when(F.col("match_key_type") == F.lit("address"), normalize_text(F.col(match_key_col)))
        .otherwise(F.lit(None)),
    ).filter(F.col("match_key_type").isin("email", "phone", "address"))
    match_df = match_df.filter(F.col("match_key_norm").isNotNull())

    input_count = input_with_id.count()
    match_count = match_df.count()
    logger.info(f"[STEP 6] Input count: {input_count:,}")
    logger.info(f"[STEP 6] Match table count (key rows): {match_count:,}")

    input_with_id.createOrReplaceTempView("input_base")
    input_keys_df.createOrReplaceTempView("input_keys")
    match_df.createOrReplaceTempView("match_data")

    # -----------------------------------------------------------------------------------
    # 7) Register UDFs and perform key matching with name validation
    # -----------------------------------------------------------------------------------
    logger.info("[STEP 7] Registering UDFs and performing key matching (SQL)...")
    spark.udf.register("fuzzy_name_match_udf", fuzzy_match_name_with_first_name, IntegerType())

    input_columns = [c for c in df_clean.columns]
    select_input_cols = ", ".join([f"i.{sql_ident(c)}" for c in input_columns])

    default_match_output_cols = [
        "match_consumer_id",
        "match_uuid",
        "match_key",
        "match_key_type",
        "match_first",
        "match_last",
        "match_first_name_score",
        "match_last_name_score",
        "match_type",
        "match_date",
    ]
    reserved_output_cols = {c.lower() for c in input_columns}
    reserved_output_cols.update(c.lower() for c in default_match_output_cols)

    extra_match_cols: list[tuple[str, str]] = []
    skipped_match_cols: list[str] = []
    renamed_match_cols: list[str] = []
    existing_output_cols = set(reserved_output_cols)

    if match_append_mode == "all":
        candidates = list(match_base_columns)
    elif match_append_mode == "list":
        candidates = []
        for raw_col in match_append_requested:
            actual = match_col_map.get(raw_col.lower())
            if not actual:
                skipped_match_cols.append(f"{raw_col} (not in match table)")
                continue
            candidates.append(actual)
    else:
        candidates = []

    seen_lower: set[str] = set()
    for col_name in candidates:
        col_key = col_name.lower()
        if col_key in seen_lower:
            continue
        seen_lower.add(col_key)
        output_name = col_name
        if output_name.lower() in existing_output_cols:
            output_name = f"matched_{col_name}"
        output_name = make_unique_name(output_name, existing_output_cols)
        if output_name.lower() != col_name.lower():
            renamed_match_cols.append(f"{col_name} -> {output_name}")
        existing_output_cols.add(output_name.lower())
        extra_match_cols.append((col_name, output_name))

    if match_append_mode != "default":
        if extra_match_cols:
            logger.info(
                "[STEP 7] Appending match table columns: "
                f"{[output for _, output in extra_match_cols]}"
            )
        else:
            logger.info("[STEP 7] No extra match table columns appended.")
    if renamed_match_cols:
        logger.info(
            "[STEP 7] Renamed match table columns due to collisions: "
            f"{renamed_match_cols}"
        )
    if skipped_match_cols:
        logger.warning(f"[STEP 7] Skipped match table columns: {skipped_match_cols}")

    extra_match_select = ""
    if extra_match_cols:
        extra_match_select = ",\n                " + ",\n                ".join(
            [
                f"m.{sql_ident(source)} AS {sql_ident(output)}"
                for source, output in extra_match_cols
            ]
        )

    extra_final_select = ""
    if extra_match_cols:
        extra_final_select = ",\n            " + ",\n            ".join(
            [f"r.{sql_ident(output)}" for _, output in extra_match_cols]
        )

    match_threshold = int(MATCH_THRESHOLD)
    input_row_id_ident = sql_ident(input_row_id_col)
    match_consumer_id_ident = sql_ident(match_consumer_id_col)
    match_uuid_ident = sql_ident(match_uuid_col)
    match_key_ident = sql_ident(match_key_col)
    match_first_ident = sql_ident(match_first_col)
    match_last_ident = sql_ident(match_last_col)

    result_df = spark.sql(
        f"""
        WITH key_matches AS (
            SELECT
                k.input_row_id,
                k.key_type,
                k.key_value,
                k.key_priority,
                k.key_order,
                k.input_first_name,
                k.input_last_name,
                m.{match_consumer_id_ident} AS match_consumer_id,
                m.{match_uuid_ident} AS match_uuid,
                m.{match_key_ident} AS match_key,
                m.match_key_type AS match_key_type,
                m.{match_first_ident} AS match_first,
                m.{match_last_ident} AS match_last{extra_match_select},
                CASE
                    WHEN k.input_first_name IS NULL OR m.{match_first_ident} IS NULL THEN 0
                    ELSE fuzzy_name_match_udf(k.input_first_name, m.{match_first_ident}, false, 0)
                END AS raw_first_name_score
            FROM input_keys k
            JOIN match_data m
              ON k.key_value = m.match_key_norm
             AND k.key_type = m.match_key_type
        ),
        name_matches AS (
            SELECT
                km.*,
                CASE
                    WHEN km.input_last_name IS NULL OR km.match_last IS NULL THEN 0
                    ELSE fuzzy_name_match_udf(
                        km.input_last_name,
                        km.match_last,
                        true,
                        km.raw_first_name_score
                    )
                END AS raw_last_name_score
            FROM key_matches km
        ),
        match_types AS (
            SELECT
                *,
                (raw_first_name_score + raw_last_name_score) / 2.0 AS match_overall_score,
                CASE
                    WHEN raw_first_name_score >= {match_threshold}
                      AND raw_last_name_score >= {match_threshold}
                    THEN 'INDIVIDUAL_MATCH'
                    WHEN raw_last_name_score >= {match_threshold}
                    THEN 'HOUSEHOLD_MATCH'
                    ELSE 'KEY_MATCH'
                END AS match_type
            FROM name_matches
        ),
        ranked_matches AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY input_row_id
                    ORDER BY
                        key_priority,
                        key_order,
                        match_overall_score DESC,
                        COALESCE(CAST(match_consumer_id AS STRING), ''),
                        COALESCE(CAST(match_uuid AS STRING), '')
                ) AS match_rank
            FROM match_types
        )
        SELECT
            {select_input_cols},
            r.match_consumer_id,
            r.match_uuid,
            r.match_key,
            r.match_key_type,
            r.match_first,
            r.match_last,
            r.raw_first_name_score AS match_first_name_score,
            r.raw_last_name_score AS match_last_name_score,
            r.match_type,
            current_date() AS match_date{extra_final_select}
        FROM input_base i
        LEFT JOIN ranked_matches r
          ON i.{input_row_id_ident} = r.input_row_id
         AND r.match_rank = 1
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
