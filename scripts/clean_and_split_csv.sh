#!/usr/bin/env bash
# chmod +x clean_and_split_csv.sh
# ./clean_and_split_csv.sh example.csv 200000

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 INPUT_FILE [ROWS_PER_CHUNK]"
  exit 1
fi

INPUT_FILE="$1"
ROWS_PER_CHUNK="${2:-100000}"  # default 100k data rows per chunk

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "Input file not found: $INPUT_FILE"
  exit 1
fi

# Derive some names
BASENAME="$(basename "$INPUT_FILE")"              # e.g. Waverly_Export_1125.txt
NAME_NO_EXT="${BASENAME%.*}"                     # e.g. Waverly_Export_1125
EXT="${BASENAME##*.}"                            # e.g. txt

CLEAN_FILE="${NAME_NO_EXT}_clean.${EXT}"         # e.g. Waverly_Export_1125_clean.txt
BAD_ROWS_FILE="${NAME_NO_EXT}_bad_rows.log"      # e.g. Waverly_Export_1125_bad_rows.log
OUT_DIR="${NAME_NO_EXT}_parts"                   # folder for chunked files

echo "Input file:          $INPUT_FILE"
echo "Cleaned output:      $CLEAN_FILE"
echo "Bad rows log:        $BAD_ROWS_FILE"
echo "Rows per chunk:      $ROWS_PER_CHUNK"
echo "Output directory:    $OUT_DIR"
echo

# Extract header and expected column count (by number of fields)
HEADER="$(head -n 1 "$INPUT_FILE")"
EXPECTED_FIELDS="$(awk -F',' 'NR==1 {print NF; exit}' "$INPUT_FILE")"

echo "Detected header:"
echo "$HEADER"
echo "Expected number of fields per row: $EXPECTED_FIELDS"
echo

# Remove any old outputs
rm -f "$CLEAN_FILE" "$BAD_ROWS_FILE"
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "Scanning file and separating good/bad rows..."

# Use awk to separate good and bad rows based on field count
awk -F',' -v expected="$EXPECTED_FIELDS" -v good="$CLEAN_FILE" -v bad="$BAD_ROWS_FILE" '
NR==1 { next }  # skip header, we will re-add later
{
  if (NF == expected) {
    print > good
  } else {
    # Log line number and content for inspection
    print "Line " NR ": " $0 > bad
  }
}
' "$INPUT_FILE"

echo "Finished scanning."
if [[ -f "$BAD_ROWS_FILE" ]]; then
  echo "Bad rows (if any) logged to: $BAD_ROWS_FILE"
fi

# Prepend header to cleaned data
TMP_CLEAN="${CLEAN_FILE}.tmp"
{
  echo "$HEADER"
  cat "$CLEAN_FILE"
} > "$TMP_CLEAN"
mv "$TMP_CLEAN" "$CLEAN_FILE"

echo "Cleaned file with header: $CLEAN_FILE"
echo

# Split cleaned file into chunks (excluding header for splitting)
echo "Splitting cleaned file into chunks of $ROWS_PER_CHUNK data rows..."

# We skip the header during split, then add it back to each part
tail -n +2 "$CLEAN_FILE" | split -l "$ROWS_PER_CHUNK" - "$OUT_DIR/${NAME_NO_EXT}_part_"

# Add header and extension to each part
for PART in "$OUT_DIR"/"${NAME_NO_EXT}_part_"*; do
  PART_WITH_HEADER="${PART}.${EXT}"
  {
    echo "$HEADER"
    cat "$PART"
  } > "$PART_WITH_HEADER"
  rm "$PART"
done

echo "Chunked files created in: $OUT_DIR"
ls -1 "$OUT_DIR"
echo
echo "Done. You can now upload these part files to S3."