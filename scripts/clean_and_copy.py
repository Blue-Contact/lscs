#!/usr/bin/env python3
"""For each database, verify dated source data exists, clear destination, then sync.

Destructive S3 commands only target lsc-databases. The blue-glue-tables bucket is used
for list-objects reads and as the read-only sync source — nothing is ever deleted there.
"""

import argparse
import json
import subprocess
import sys
from datetime import date

SRC_BUCKET = "blue-glue-tables"
DST_BUCKET = "lsc-databases"
SOURCE_BASE = "source_b"

# Basenames under source_b/ — source is {name}_{YYYYMM}/, destination is {name}/
DATABASES = [
    "bankruptcy",
    "business",
    "deceased",
    "master_telco",
    "property",
    "primary_assessor",
]


def current_yyyy_mm() -> str:
    d = date.today()
    return f"{d.year}{d.month:02d}"


def parse_ym(s: str) -> str:
    if len(s) != 6 or not s.isdigit():
        raise argparse.ArgumentTypeError("must be six digits YYYYMM, e.g. 202604")
    return s


def s3_uri(bucket: str, prefix: str) -> str:
    p = prefix.rstrip("/") + "/"
    return f"s3://{bucket}/{p}"


def bucket_from_s3_uri(uri: str) -> str:
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3 URI: {uri!r}")
    rest = uri[len("s3://") :]
    return rest.split("/", 1)[0]


def assert_sync_direction(src: str, dst: str) -> None:
    """Refuse if source/destination buckets are swapped or wrong for safety."""
    if bucket_from_s3_uri(src) != SRC_BUCKET:
        raise AssertionError(f"sync source must be {SRC_BUCKET}, got {src!r}")
    if bucket_from_s3_uri(dst) != DST_BUCKET:
        raise AssertionError(f"sync destination must be {DST_BUCKET}, got {dst!r}")


def assert_rm_destination_only(dst: str) -> None:
    if bucket_from_s3_uri(dst) != DST_BUCKET:
        raise AssertionError(f"rm only allowed on {DST_BUCKET}, got {dst!r}")


def source_has_files(prefix: str) -> bool:
    """True if SRC_BUCKET has at least one object under prefix."""
    proc = subprocess.run(
        [
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            SRC_BUCKET,
            "--prefix",
            prefix,
            "--max-keys",
            "1",
            "--output",
            "json",
        ],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr, end="")
        return False
    data = json.loads(proc.stdout)
    contents = data.get("Contents") or []
    return len(contents) > 0


def run(cmd: list[str]) -> int:
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(cmd).returncode


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "For each database, check blue-glue-tables …/{db}_{YYYYMM}/ has files, "
            "then remove lsc-databases …/{db}/ and sync."
        ),
    )
    parser.add_argument(
        "ym_suffix",
        nargs="?",
        type=parse_ym,
        metavar="YYYYMM",
        help="Source folder suffix {db}_YYYYMM (default: current year-month).",
    )
    args = parser.parse_args()
    ym = args.ym_suffix if args.ym_suffix is not None else current_yyyy_mm()

    any_ok = False
    for db in DATABASES:
        src_key_prefix = f"{SOURCE_BASE}/{db}_{ym}/"
        dst_uri = s3_uri(DST_BUCKET, f"{SOURCE_BASE}/{db}")

        print(
            f"\n--- {db}: "
            f"check s3://{SRC_BUCKET}/{src_key_prefix} "
            f"→ {dst_uri}",
            flush=True,
        )

        if not source_has_files(src_key_prefix):
            print(
                f"skip {db}: source missing or has no files under s3://{SRC_BUCKET}/{src_key_prefix}",
                file=sys.stderr,
                flush=True,
            )
            continue

        src_uri = s3_uri(SRC_BUCKET, src_key_prefix)
        assert_rm_destination_only(dst_uri)
        assert_sync_direction(src_uri, dst_uri)
        rc = run(["aws", "s3", "rm", dst_uri, "--recursive"])
        if rc != 0:
            return rc
        rc = run(["aws", "s3", "sync", src_uri, dst_uri])
        if rc != 0:
            return rc
        any_ok = True

    if not any_ok:
        print("error: no database had a non-empty source; nothing copied.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
