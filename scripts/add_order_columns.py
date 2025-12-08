#!/usr/bin/env python3
"""
add_order_columns.py

This script processes CSV files by adding two columns:
  • OrderCode  - inferred automatically from the filename prefix (before first underscore)
  • OrderDate  - provided via --order_date OR defaults to today's date

It writes updated files as <original_name>_with_orders.csv in the same directory.

Example filename → inferred OrderCode:
    QB127_OmitFile_Export_1125.csv → OrderCode = "QB127"

--------------------------------------
Example usage:

# 1. Auto-use today's date (default)
    ./add_order_columns.py --input_dir "/path/to/csvs"

# 2. Provide a specific OrderDate
    ./add_order_columns.py --input_dir "/path/to/csvs" --order_date 2025-11-25

# 3. Use a custom matching pattern
    ./add_order_columns.py --input_dir "/path/to/csvs" --pattern "*.csv"

--------------------------------------
"""

import os
import sys
import argparse
import pandas as pd
from glob import glob
from datetime import date


def process_file(input_path: str, order_date: str, output_suffix: str = "_with_orders"):
    """
    Processes a single CSV file:
      - Reads CSV
      - Infers OrderCode from filename prefix
      - Adds OrderCode and OrderDate columns
      - Writes a new CSV with '_with_orders' appended to filename
    """

    base = os.path.basename(input_path)
    dirname = os.path.dirname(input_path)

    # Infer OrderCode from the filename prefix before first underscore
    # Example: "QB127_OmitFile_Export_1125.csv" → "QB127"
    order_code = base.split("_")[0]

    print(f"Processing: {base}  (OrderCode={order_code}, OrderDate={order_date})")

    # Load CSV (auto-detects header)
    df = pd.read_csv(input_path)

    # Add new columns to every row
    df["OrderCode"] = order_code
    df["OrderDate"] = order_date

    # Build output filename
    name, ext = os.path.splitext(base)
    output_name = f"{name}{output_suffix}{ext}"
    output_path = os.path.join(dirname, output_name)

    # Save the updated CSV
    df.to_csv(output_path, index=False)

    print(f"  → Wrote: {output_path}\n")


def main():
    """Main entry point for argument parsing and batch file processing."""
    
    parser = argparse.ArgumentParser(
        description="Add OrderCode and OrderDate columns to CSV files in a directory."
    )

    # Required input directory argument
    parser.add_argument(
        "--input_dir",
        required=True,
        help="Directory containing the CSV files."
    )

    # Optional file pattern
    parser.add_argument(
        "--pattern",
        default="*_OmitFile_Export_*.csv",
        help="Glob pattern for matching CSV files (default: *_OmitFile_Export_*.csv)"
    )

    # Optional order date argument. If omitted, today's date is used.
    parser.add_argument(
        "--order_date",
        help="OrderDate value (YYYY-MM-DD). If omitted, today's date will be used."
    )

    args = parser.parse_args()

    # Use provided order_date or default to today's date
    order_date = args.order_date or date.today().isoformat()

    # Build full glob search path
    pattern = os.path.join(args.input_dir, args.pattern)

    # Find all matching files
    files = glob(pattern)
    if not files:
        print(f"No files found matching pattern: {pattern}")
        sys.exit(1)

    print(f"Found {len(files)} file(s) in {args.input_dir}\n")

    # Process each file found
    for file_path in files:
        process_file(file_path, order_date)


# Run script
if __name__ == "__main__":
    main()