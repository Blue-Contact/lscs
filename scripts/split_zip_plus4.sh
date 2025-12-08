#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 input.csv output.csv" >&2
  exit 1
fi

infile="$1"
outfile="$2"

awk -F',' -v OFS=',' '
BEGIN {
  zip_col = 0
}
NR == 1 {
  # Strip CR (in case of Windows line endings)
  gsub(/\r/, "", $0)

  # Find the ZIP column
  for (i = 1; i <= NF; i++) {
    if ($i == "ZIP" || $i == "zip") {
      zip_col = i
      break
    }
  }
  if (zip_col == 0) {
    print "Error: ZIP column not found in header" > "/dev/stderr"
    exit 1
  }

  # Print header: replace ZIP with zip,plus4
  for (i = 1; i <= NF; i++) {
    if (i == zip_col) {
      printf "%s", "zip"
      printf OFS "plus4"
    } else {
      printf "%s", $i
    }
    if (i < NF) printf OFS
  }
  printf ORS
  next
}
NR > 1 {
  # Strip CR (in case of Windows line endings)
  gsub(/\r/, "", $0)

  for (i = 1; i <= NF; i++) {
    if (i == zip_col) {
      # Split ZIP field on "-"
      split($i, arr, "-")
      zip = arr[1]
      plus4 = (length(arr) > 1 ? arr[2] : "")

      printf "%s", zip
      printf OFS plus4
    } else {
      printf "%s", $i
    }
    if (i < NF) printf OFS
  }
  printf ORS
}
' "$infile" > "$outfile"