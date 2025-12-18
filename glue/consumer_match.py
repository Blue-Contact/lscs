"""
Advanced Person-to-Person Matching Job with Sophisticated Algorithms

This job combines the best features from both old_consumer_match.py and large_consumer_match_job.py:
- Advanced fuzzy matching algorithms (rapidfuzz + phonetic)
- Flexible column mapping for different table schemas
- State-based filtering for efficient testing
- Multiple similarity algorithms for better match rates

The job performs fuzzy matching between large datasets and classifies matches into:
- INDIVIDUAL_MATCH: High confidence match on first name, last name, and address
- HOUSEHOLD_MATCH: Match on last name and address (family members at same address)  
- ADDRESS_MATCH: Match on address only (different people at same address)
"""

import sys
import re
import time
import logging
import boto3
from rapidfuzz import fuzz
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import jellyfish  # For phonetic matching algorithms
from nicknames import NickNamer  # For handling name variations

# Note: This job requires the following additional Python modules:
# --additional-python-modules "nicknames==0.1.0,jellyfish==0.9.0,rapidfuzz==3.6.1"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==========================================
# Performance Metrics Class
# ==========================================
class PerformanceMetrics:
    def __init__(self):
        self.start_time = time.time()
        self.metrics = {}
        self.logger = logging.getLogger(__name__)

    def record_step(self, name, start):
        elapsed = time.time() - start
        self.metrics[name] = elapsed
        self.logger.info(f"Step '{name}' took {elapsed:.2f}s")

    def log_summary(self):
        total = time.time() - self.start_time
        self.logger.info(f"Total job time: {total:.2f}s")
        for step, dur in self.metrics.items():
            pct = (dur / total) * 100 if total > 0 else 0
            self.logger.info(f"  {step}: {dur:.2f}s ({pct:.1f}%)")

# ==========================================
# Advanced Name Matcher Class
# ==========================================
class NameMatcher:
    """Class to handle advanced name matching logic with multiple algorithms."""
    
    def __init__(self):
        self.common_variations = {
            'jr': 'junior',
            'sr': 'senior',
            'ii': '2',
            'iii': '3',
            'iv': '4',
            'v': '5',
            'vi': '6',
            'vii': '7',
            'viii': '8',
            'ix': '9',
            'x': '10',
            'st': 'saint',
            'mt': 'mount',
            'dr': 'doctor',
            'rev': 'reverend',
            'hon': 'honorable',
            'prof': 'professor',
            'mr': 'mister',
            'mrs': 'missus',
            'ms': 'miss',
            'miss': 'miss',
            'mstr': 'master'
        }
        self.suffixes = ['jr', 'sr', 'ii', 'iii', 'iv', 'v', 'vi', 'vii', 'viii', 'ix', 'x']
        self.nicknamer = NickNamer()
    
    def normalize_string(self, s):
        """Normalize string by removing extra whitespace and converting to lowercase."""
        if s is None:
            return ""
        
        # Convert to string and handle all types of whitespace
        s = str(s)
        # Replace all whitespace characters with a single space
        s = re.sub(r'\s+', ' ', s)
        # Strip leading/trailing whitespace
        s = s.strip()
        # Convert to lowercase
        s = s.lower()
        
        # Replace variations
        for short, long in self.common_variations.items():
            s = re.sub(r'\b' + short + r'\b', long, s)
        
        return s
    
    def get_rapidfuzz_scores(self, s1, s2):
        """Get scores from various rapidfuzz algorithms."""
        if not s1 or not s2:
            return 0
            
        best_score = 0
        
        try:
            # Check for exact match first (strings are already normalized)
            if s1 == s2:
                return 100
            
            # Get ratio score
            ratio = int(fuzz.ratio(s1, s2))
            if ratio > best_score:
                best_score = ratio
            
            # Get partial ratio score
            partial = int(fuzz.partial_ratio(s1, s2))
            if partial > best_score:
                best_score = partial
            
            # Get token sort ratio score
            token_sort = int(fuzz.token_sort_ratio(s1, s2))
            if token_sort > best_score:
                best_score = token_sort
            
            # Get token set ratio score
            token_set = int(fuzz.token_set_ratio(s1, s2))
            if token_set > best_score:
                best_score = token_set
            
        except Exception as e:
            # Note: Logging disabled in UDFs to prevent CloudWatch cost explosion
            pass
        
        return best_score
    
    def get_phonetic_scores(self, s1, s2):
        """Get scores from various phonetic matching algorithms."""
        if not s1 or not s2:
            return 0
            
        # Check for exact match first (strings are already normalized)
        if s1 == s2:
            return 100
        
        best_score = 0
        
        try:
            # Get Soundex score
            soundex1 = jellyfish.soundex(s1)
            soundex2 = jellyfish.soundex(s2)
            if soundex1 == soundex2:
                score = 90  # High but not perfect match for phonetic similarity
                if score > best_score:
                    best_score = score
            
            # Get Metaphone score
            metaphone1 = jellyfish.metaphone(s1)
            metaphone2 = jellyfish.metaphone(s2)
            if metaphone1 == metaphone2:
                score = 90  # High but not perfect match for phonetic similarity
                if score > best_score:
                    best_score = score
            
            # Get NYSIIS score
            nysiis1 = jellyfish.nysiis(s1)
            nysiis2 = jellyfish.nysiis(s2)
            if nysiis1 == nysiis2:
                score = 90  # High but not perfect match for phonetic similarity
                if score > best_score:
                    best_score = score
            
            # Get Match Rating Approach score
            mra1 = jellyfish.match_rating_codex(s1)
            mra2 = jellyfish.match_rating_codex(s2)
            if mra1 == mra2:
                score = 90  # High but not perfect match for phonetic similarity
                if score > best_score:
                    best_score = score
            
        except Exception as e:
            # Note: Logging disabled in UDFs to prevent CloudWatch cost explosion
            pass
        
        return best_score
    
    def get_combined_scores(self, s1, s2):
        """Get all possible scores between two strings."""
        if not s1 or not s2:
            return 0
        
        # Check for exact match first
        if s1 == s2:
            return 100
        
        # Get rapidfuzz scores
        rapidfuzz_score = self.get_rapidfuzz_scores(s1, s2)
        
        # Get phonetic scores
        phonetic_score = self.get_phonetic_scores(s1, s2)
        
        # Ensure both scores are integers and return the best score
        rapidfuzz_int = int(rapidfuzz_score) if rapidfuzz_score is not None else 0
        phonetic_int = int(phonetic_score) if phonetic_score is not None else 0
        best_score = rapidfuzz_int if rapidfuzz_int > phonetic_int else phonetic_int
        
        return best_score

    def get_best_match_score(self, s1, s2, is_last_name=False, first_name_score=0):
        """Get the best match score using both rapidfuzz and phonetic matching."""
        if not s1 or not s2:
            return 0
            
        try:
            # Strings should already be normalized when passed in
            s1_norm = s1
            s2_norm = s2
            
            # Check for exact match first
            if s1_norm == s2_norm:
                return 100
                
            # Get all possible scores
            all_scores = []
            
            # For last names, handle hyphenated, spaced, and concatenated names
            if is_last_name:
                # Split names by both hyphen and space
                s1_parts = []
                for part in s1_norm.split():
                    s1_parts.extend([p for p in part.split('-') if p])
                s2_parts = []
                for part in s2_norm.split():
                    s2_parts.extend([p for p in part.split('-') if p])
                
                # Try fuzzy matching on the full names
                score = self.get_combined_scores(s1_norm, s2_norm)
                if score > 0:
                    all_scores.append(score)
                
                # Try fuzzy matching between parts
                for part1 in s1_parts:
                    if part1:
                        # Try matching each part of s1 against full s2
                        score = self.get_combined_scores(part1, s2_norm)
                        if score > 0:
                            all_scores.append(score)
                        
                        # Try matching each part of s1 against each part of s2
                        for part2 in s2_parts:
                            if part2:
                                score = self.get_combined_scores(part1, part2)
                                if score > 0:
                                    all_scores.append(score)
                
                # Try matching full s1 against each part of s2
                for part2 in s2_parts:
                    if part2:
                        score = self.get_combined_scores(s1_norm, part2)
                        if score > 0:
                            all_scores.append(score)
            else:
                # For first names, just use fuzzy matching
                score = self.get_combined_scores(s1_norm, s2_norm)
                if score > 0:
                    all_scores.append(score)
            
            # Return the best score found, or 0 if no matches found
            if all_scores:
                # Use Python's built-in max() explicitly to avoid Spark's max() function
                import builtins
                best_score = builtins.max(all_scores)
                
                return best_score
            else:
                return 0
        except Exception as e:
            # Note: Logging disabled in UDFs to prevent CloudWatch cost explosion
            return 0

# Initialize the name matcher
name_matcher = NameMatcher()

# Debug function to test scoring (temporary)
def debug_name_scoring():
    """Debug function to test name scoring logic."""
    test_cases = [
        ("STRAITOR", "Straiton", True),  # Should be household match
        ("SMITH", "Smith", True),        # Exact match after normalization
        ("JOHNSON", "Johnsson", True),   # Similar names
        ("BARBARA", "Beverly", False),   # Different first names
    ]
    
    for name1, name2, is_last_name in test_cases:
        norm1 = name_matcher.normalize_string(name1)
        norm2 = name_matcher.normalize_string(name2)
        
        # Test rapidfuzz directly
        rapidfuzz_scores = name_matcher.get_rapidfuzz_scores(norm1, norm2)
        phonetic_scores = name_matcher.get_phonetic_scores(norm1, norm2)
        combined_scores = name_matcher.get_combined_scores(norm1, norm2)
        final_score = name_matcher.get_best_match_score(norm1, norm2, is_last_name)
        
        print(f"'{name1}' vs '{name2}' (last_name={is_last_name}): {final_score}")
        print(f"  Normalized: '{norm1}' vs '{norm2}'")
        print(f"  Rapidfuzz scores: {rapidfuzz_scores}")
        print(f"  Phonetic scores: {phonetic_scores}")
        print(f"  Combined scores: {combined_scores}")
        print()

# Uncomment to run debug scoring
# debug_name_scoring()

# ==========================================
# Define Advanced Fuzzy Matching UDFs
# ==========================================
def fuzzy_match_name_with_first_name(name1, name2, is_last_name, first_name_score):
    """Fuzzy match two names using advanced algorithms."""
    try:
        if name1 is None or name2 is None:
            return 0
            
        # Normalize strings once at the UDF level
        name1_norm = name_matcher.normalize_string(name1)
        name2_norm = name_matcher.normalize_string(name2)
        
        score = name_matcher.get_best_match_score(name1_norm, name2_norm, is_last_name, first_name_score)
        
        return int(score)
    except Exception as e:
        # Note: Logging disabled in UDFs to prevent CloudWatch cost explosion
        return 0

def fuzzy_match_address(addr1, addr2, zip1, zip2):
    """Fuzzy match two addresses using advanced algorithms."""
    if addr1 is None or addr2 is None or zip1 is None or zip2 is None:
        return 0
        
    # First check if zips match exactly
    if zip1 != zip2:
        return 0
        
    # Normalize strings once at the UDF level
    addr1_norm = name_matcher.normalize_string(addr1)
    addr2_norm = name_matcher.normalize_string(addr2)
    
    score = name_matcher.get_best_match_score(addr1_norm, addr2_norm)
    return int(score)

# ==========================================
# Parse Command Line Arguments
# ==========================================
args = getResolvedOptions(
    sys.argv,
    [
        'INPUT_TABLE',      # e.g. source_b.b2c_tci_202507
        'MATCH_TABLE',      # e.g. source_a.consumer_data_202507
        'OUTPUT_PATH',      # e.g. s3://bucket/matched/
        'OUTPUT_TABLE',     # e.g. output_db.matched_consumers
        'MATCH_THRESHOLD',  # fuzzy threshold (0-100)
        'INPUT_COLUMN_MAPPING'  # JSON mapping from MATCH_TABLE columns to INPUT_TABLE columns
    ]
)

# Handle optional STATE_FILTER parameter (preferred for testing)
try:
    optional_args = getResolvedOptions(sys.argv, ['STATE_FILTER'])
    args['STATE_FILTER'] = optional_args['STATE_FILTER'].upper().strip()
    print(f"STATE_FILTER provided: {args['STATE_FILTER']}")
    if args['STATE_FILTER'] and len(args['STATE_FILTER']) != 2:
        raise ValueError("STATE_FILTER must be a 2-character state code (e.g., 'WY', 'CA')")
except:
    args['STATE_FILTER'] = None
    print("No STATE_FILTER provided")

# Convert MATCH_THRESHOLD to int
args['MATCH_THRESHOLD'] = int(args['MATCH_THRESHOLD'])

# Parse INPUT_COLUMN_MAPPING JSON
import json
try:
    input_column_mapping = json.loads(args['INPUT_COLUMN_MAPPING'])
    print(f"Column mapping provided: {input_column_mapping}")
except json.JSONDecodeError as e:
    raise ValueError(f"Invalid JSON in INPUT_COLUMN_MAPPING parameter: {e}")

# Validate required columns are mapped
required_columns = ['first_name', 'last_name', 'address', 'city', 'state', 'zip4', 'zip']
missing_columns = [col for col in required_columns if col not in input_column_mapping]
if missing_columns:
    raise ValueError(f"Missing required consumer table columns in INPUT_COLUMN_MAPPING: {missing_columns}")

print(f"Parsed arguments: {args}")

# ==========================================
# Initialize Spark and Glue Contexts
# ==========================================
try:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init('person-match-job', args)
    metrics = PerformanceMetrics()
    logging.getLogger().setLevel(logging.INFO)

    logger.info(f"Starting job with parameters: {args}")

    # Helper functions for column mapping
    def get_input_column_name(standard_col_name):
        """Get the actual input table column name using column mapping."""
        return input_column_mapping.get(standard_col_name, standard_col_name)

    def get_match_column_name(col_name):
        """Get the match table column name (no mapping needed).""" 
        return col_name

    # ==========================================
    # Clean Previous Output from S3
    # ==========================================
    start = time.time()
    if args['OUTPUT_PATH'].startswith('s3://'):
        s3 = boto3.client('s3')
        _, _, bucket, *prefix = args['OUTPUT_PATH'].split('/', 3)
        prefix = prefix[0] if prefix else ''
        try:
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            for obj in resp.get('Contents', []):
                s3.delete_object(Bucket=bucket, Key=obj['Key'])
        except Exception as e:
            logging.warning(f"Cleanup warning: {e}")
    metrics.record_step('Cleanup', start)

    # ==========================================
    # Calculate Match Thresholds
    # ==========================================
    match_threshold = int(args.get('MATCH_THRESHOLD', 92))
    # Household threshold is 70% of match threshold (more sensitive for household matching)
    household_threshold = match_threshold #int(match_threshold * 0.7)
    logger.info(f"Match threshold: {match_threshold}, Household threshold: {household_threshold}")

    # ==========================================
    # Register UDFs with Spark
    # ==========================================
    spark.udf.register("fuzzy_name_match_udf", fuzzy_match_name_with_first_name, IntegerType())
    spark.udf.register("fuzzy_address_match_udf", fuzzy_match_address, IntegerType())
    
    # Test UDF registration with a simple test (disabled for production to reduce CloudWatch costs)
    # Uncomment for debugging:
    # logger.info("Testing UDF registration...")
    # test_df = spark.createDataFrame([("STRAITOR", "Straiton", True, 0)], ["name1", "name2", "is_last_name", "first_name_score"])
    # test_df.createOrReplaceTempView("test_udf")
    # result = spark.sql("SELECT fuzzy_name_match_udf(name1, name2, is_last_name, first_name_score) as score FROM test_udf").collect()
    # logger.info(f"UDF test result: {result}")

    # ==========================================
    # Load Data with Optional State Filtering
    # ==========================================
    start = time.time()
    
    logger.info("Reading input table...")
    # Read input table with optional state filtering
    if args.get('STATE_FILTER'):
        state_filter = args['STATE_FILTER']
        logger.info(f"Loading input data filtered by state: {state_filter}")
        input_column_for_state = get_input_column_name('state')
        input_df = spark.sql(f"""
            SELECT * FROM {args['INPUT_TABLE']} 
            WHERE {input_column_for_state} = '{state_filter}'
        """)
    else:
        logger.info("Loading full input dataset")
        input_df = spark.sql(f"SELECT * FROM {args['INPUT_TABLE']}")
    
    input_count = input_df.count()
    logger.info(f"Input table count: {input_count:,}")

    logger.info("Reading match table...")
    # Read match table with optional state filtering
    if args.get('STATE_FILTER'):
        state_filter = args['STATE_FILTER']
        logger.info(f"Loading match data filtered by state: {state_filter}")
        match_df = spark.sql(f"""
            SELECT * FROM {args['MATCH_TABLE']} 
            WHERE state = '{state_filter}'
        """)
    else:
        logger.info("Loading full match dataset")
        match_df = spark.sql(f"SELECT * FROM {args['MATCH_TABLE']}")
        
    match_count = match_df.count()
    logger.info(f"Match table count: {match_count:,}")
    
    if args.get('STATE_FILTER'):
        logger.info(f"State-filtered records ({args['STATE_FILTER']}) - Input: {input_count:,}, Match: {match_count:,}")
    else:
        logger.info(f"Full dataset records - Input: {input_count:,}, Match: {match_count:,}")

    # Log potential comparison pairs
    logger.info(f"Potential comparison pairs (without blocking): {input_count * match_count:,}")

    # ==========================================
    # Normalize Data Using Column Mapping
    # ==========================================
    # Normalize case for input data and ensure no nulls (using column mapping)
    input_first_col = get_input_column_name('first_name')
    input_last_col = get_input_column_name('last_name')
    input_address_col = get_input_column_name('address')
    input_zip_col = get_input_column_name('zip')
    input_zip4_col = get_input_column_name('zip4')

    input_zip_clean = trim(coalesce(col(input_zip_col).cast("string"), lit("")))
    input_zip4_clean = trim(coalesce(col(input_zip4_col).cast("string"), lit("")))
    input_zip_5 = substring(input_zip_clean, 1, 5)
    input_zip4_4 = substring(input_zip4_clean, 1, 4)
    
    input_df = input_df.select(
        "*",
        coalesce(col(input_first_col), lit("")).alias("first_name_norm"),
        coalesce(col(input_last_col), lit("")).alias("last_name_norm"),
        coalesce(col(input_address_col), lit("")).alias("address_norm"),
        when(length(input_zip_5) == 0, lit("")).otherwise(lpad(input_zip_5, 5, "0")).alias("zip_norm"),
        when(length(input_zip4_4) == 0, lit("")).otherwise(lpad(input_zip4_4, 4, "0")).alias("zip4_norm")
    )

    match_zip_clean = trim(coalesce(col("zip").cast("string"), lit("")))
    match_zip4_clean = trim(coalesce(col("zip4").cast("string"), lit("")))
    match_zip_5 = substring(match_zip_clean, 1, 5)
    match_zip4_4 = substring(match_zip4_clean, 1, 4)

    # Normalize case for match data and ensure no nulls
    match_df = match_df.select(
        "*",
        coalesce(col("first_name"), lit("")).alias("first_name_norm"),
        coalesce(col("last_name"), lit("")).alias("last_name_norm"),
        coalesce(col("address"), lit("")).alias("address_norm"),
        when(length(match_zip_5) == 0, lit("")).otherwise(lpad(match_zip_5, 5, "0")).alias("zip_norm"),
        when(length(match_zip4_4) == 0, lit("")).otherwise(lpad(match_zip4_4, 4, "0")).alias("zip4_norm")
    )

    # Create temporary views
    input_df.createOrReplaceTempView("input_data")
    match_df.createOrReplaceTempView("match_data")
    
    metrics.record_step('LoadData', start)

    # ==========================================
    # Perform Advanced Fuzzy Matching
    # ==========================================
    start = time.time()
    
    # Get all column names from input table except the normalized ones
    input_columns = [col for col in input_df.columns if not col.endswith('_norm')]
    
    logger.info("Performing advanced fuzzy matching...")
    # Perform fuzzy matching with optimized join using column mapping
    
    result_df = spark.sql(f"""
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
                    PARTITION BY 
                        {input_first_col}, {input_last_col}, {input_address_col}, {input_zip_col}, {input_zip4_col}
                    ORDER BY 
                        CASE match_type
                            WHEN 'INDIVIDUAL_MATCH' THEN 1
                            WHEN 'HOUSEHOLD_MATCH' THEN 2
                            WHEN 'ADDRESS_MATCH' THEN 3
                            ELSE 4
                        END,
                        match_overall_score DESC
                )                         AS match_rank
            FROM match_types
            WHERE match_type != 'NO_MATCH'
        )
        SELECT 
            {', '.join([f"r.{col}" for col in input_columns])},
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
    """)
    
    result_count = result_df.count()
    logger.info(f"Result count: {result_count:,}")
    
    metrics.record_step('FuzzyMatch', start)

    # ==========================================
    # Log Match Type Distribution
    # ==========================================
    logger.info("Match type distribution:")
    try:
        result_df.createOrReplaceTempView("temp_results")
        match_type_dist = spark.sql("""
            SELECT match_type, COUNT(*) as count, 
                   ROUND(AVG(match_overall_score), 2) as avg_score
            FROM temp_results
            GROUP BY match_type
            ORDER BY count DESC
        """)
        match_type_dist.show()
        
        # Add score distribution analysis
        logger.info("Score distribution analysis:")
        score_dist = spark.sql("""
            SELECT 
                match_type,
                COUNT(*) as count,
                ROUND(AVG(match_first_name_score), 2) as avg_first_name_score,
                ROUND(AVG(match_last_name_score), 2) as avg_last_name_score,
                ROUND(AVG(match_address_score), 2) as avg_address_score,
                ROUND(MIN(match_last_name_score), 2) as min_last_name_score,
                ROUND(MAX(match_last_name_score), 2) as max_last_name_score
            FROM temp_results
            GROUP BY match_type
            ORDER BY count DESC
        """)
        score_dist.show()
    except Exception as e:
        logger.warning(f"Could not show match type distribution: {e}")

    # ==========================================
    # Write Output
    # ==========================================
    start = time.time()
    logger.info("Writing results to Parquet...")
    result_df.write \
        .format("parquet") \
        .option("compression", "snappy") \
        .mode("overwrite") \
        .save(args['OUTPUT_PATH'])
    metrics.record_step('WriteOutput', start)

    # ==========================================
    # Register Output Table in Glue Catalog
    # ==========================================
    start = time.time()
    logger.info("Creating output table...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {args['OUTPUT_TABLE']}
        USING parquet
        LOCATION '{args['OUTPUT_PATH']}'
    """)
    metrics.record_step('RegisterTable', start)

    # ==========================================
    # Finalize Job
    # ==========================================
    metrics.log_summary()
    logger.info("Job completed successfully")
    job.commit()

except Exception as e:
    logger.error(f"Job failed with error: {str(e)}")
    raise 
