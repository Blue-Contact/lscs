#!/usr/bin/env python3
"""
Flatten Excel tier sheets into two CSVs:
  1) <stem>_flattened.csv: geo_value, geo_type, tier
  2) <stem>_tier_summary.csv: tier + counts + states

Key behaviors:
- Narrative sub-tier detection:
    Cells like "Tier 1 OH - Non-Lucas Co. Ohio" switch the current tier context.
- IMPORTANT: States mentioned in tier labels are NOT treated as audience geography.
    They are recorded in the summary column `label_states` only.
- Geo type detection:
    * zip  = 5 digits
    * scf  = 3 digits
    * state = explicit rule phrases like "all zips in Rhode Island" / "balance of ..."
- SCF de-noising:
    If a row contains a ZIP, 3-digit SCFs on that same row are ignored (often informational).

Usage:
  python3 flatten_geo_tiers_v2.py --input SW1295.xlsx --out-dir ./output

Dependencies:
  pip install pandas openpyxl
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd


STATE_MAP = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD", "massachusetts": "MA",
    "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new_hampshire": "NH", "new_jersey": "NJ", "new_mexico": "NM",
    "new_york": "NY", "north_carolina": "NC", "north_dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode_island": "RI", "south_carolina": "SC",
    "south_dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west_virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
}
ABBR_SET = set(STATE_MAP.values())


def slug(s: object) -> str:
    s = "" if s is None else str(s)
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def detect_state_in_text(text: object) -> str | None:
    if text is None:
        return None
    t = str(text).strip().lower()
    t_slug = slug(t)

    # Explicit 2-letter token (e.g., "OH")
    tokens = re.findall(r"\b[a-z]{2}\b", t)
    for tok in tokens:
        up = tok.upper()
        if up in ABBR_SET:
            return up

    # Full state names embedded in text
    for name, abbr in STATE_MAP.items():
        if name in t_slug:
            return abbr

    return None


def detect_geo_value(val: object) -> tuple[str | None, str | None]:
    """
    Returns (geo_type, geo_value) where geo_type is one of:
      zip, scf, state
    """
    if val is None:
        return None, None
    v = str(val).strip()
    if v == "" or v.lower() == "nan":
        return None, None

    v_low = v.lower().strip()

    # ZIP
    if re.fullmatch(r"\d{5}", v_low):
        return "zip", v_low

    # SCF
    if re.fullmatch(r"\d{3}", v_low):
        return "scf", v_low

    # State: treat as audience geo ONLY if it's clearly an inclusion rule
    st = detect_state_in_text(v)

    # Exact token
    if st and (v_low == st.lower() or re.fullmatch(r"[a-z]{2}", v_low)):
        return "state", st

    # Narrative inclusion rules
    if st and ("all zips" in v_low or "balance of" in v_low or "zips in" in v_low):
        return "state", st

    return None, None


def is_narrative_tier_label(val: object) -> bool:
    """
    Detects labels like:
      "Tier 1 OH - Non-Lucas Co. Ohio"
      "Tier 1 MI- Non-Lucas Co. Michigan"
    """
    if val is None:
        return False
    s = str(val).strip()
    if s == "" or s.lower() == "nan":
        return False

    low = s.lower()
    if "tier" not in low:
        return False

    has_state = detect_state_in_text(s) is not None
    has_geo_words = any(
        w in low for w in ["balance", "county", "co.", "co ", "non-", "non ", "all zips", "zips in"]
    )
    has_dash = any(d in s for d in ["-", "–", "—"])

    return has_state or has_geo_words or has_dash


def row_has_zip(df: pd.DataFrame, r: int) -> bool:
    row = df.iloc[r, :].tolist()
    for v in row:
        gt, _ = detect_geo_value(v)
        if gt == "zip":
            return True
    return False


def flatten_workbook(input_path: Path) -> tuple[pd.DataFrame, dict[str, set[str]]]:
    """
    Returns:
      flat_df: geo_value, geo_type, tier (audience geography only)
      tier_label_states: {tier: set(states mentioned in tier label text)}
    """
    xl = pd.ExcelFile(input_path)
    rows: list[dict] = []
    tier_label_states: dict[str, set[str]] = {}

    for sheet in xl.sheet_names:
        df = xl.parse(sheet, dtype=str)  # keep blanks for layout
        base_tier = slug(sheet)
        current_subtier: str | None = None

        for r in range(df.shape[0]):
            zip_present = row_has_zip(df, r)

            for c in range(df.shape[1]):
                val = df.iat[r, c]

                # Narrative tier label: switch tier context; record label states (metadata only)
                if is_narrative_tier_label(val):
                    label_slug = slug(val)
                    current_subtier = f"{base_tier}_{label_slug}" if label_slug else base_tier
                    st = detect_state_in_text(val)
                    if st:
                        tier_label_states.setdefault(current_subtier, set()).add(st)
                    continue

                geo_type, geo_val = detect_geo_value(val)
                if not geo_type:
                    continue

                # Ignore SCFs on rows that also contain ZIPs (usually informational)
                if geo_type == "scf" and zip_present:
                    continue

                tier_out = current_subtier if current_subtier else base_tier
                rows.append(
                    {
                        "geo_value": geo_val.upper() if geo_type == "state" else geo_val,
                        "geo_type": geo_type,
                        "tier": tier_out,
                    }
                )

    flat = pd.DataFrame(rows).drop_duplicates()
    flat = flat.sort_values(["tier", "geo_type", "geo_value"]).reset_index(drop=True)
    return flat, tier_label_states


def build_tier_summary(flat: pd.DataFrame, tier_label_states: dict[str, set[str]]) -> pd.DataFrame:
    totals = flat.groupby("tier").size().rename("row_count").reset_index()

    counts = (
        flat.groupby(["tier", "geo_type"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )
    for col in ["zip", "scf", "state"]:
        if col not in counts.columns:
            counts[col] = 0

    audience_states = (
        flat[flat["geo_type"] == "state"]
        .groupby("tier")["geo_value"]
        .apply(lambda s: ",".join(sorted(set(s))))
        .rename("audience_states")
        .reset_index()
    )

    label_states_df = pd.DataFrame(
        [{"tier": t, "label_states": ",".join(sorted(v))} for t, v in tier_label_states.items()]
    )
    # FIX: if there were no narrative labels, ensure the DataFrame still has the merge key/columns
    # so the merge doesn't raise KeyError: 'tier'.
    if label_states_df.empty:
        label_states_df = pd.DataFrame(columns=["tier", "label_states"])

    summary = (
        totals
        .merge(counts[["tier", "zip", "scf", "state"]], on="tier", how="left")
        .merge(audience_states, on="tier", how="left")
        .merge(label_states_df, on="tier", how="left")
        .fillna({"audience_states": "", "label_states": ""})
        .rename(columns={"zip": "zip_count", "scf": "scf_count", "state": "state_count"})
        .sort_values(["row_count", "tier"], ascending=[False, True])
        .reset_index(drop=True)
    )
    return summary


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to input .xlsx")
    ap.add_argument("--out-dir", required=True, help="Directory for output CSVs")
    args = ap.parse_args()

    in_path = Path(args.input).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()

    if not in_path.exists():
        print(f"ERROR: input file not found: {in_path}", file=sys.stderr)
        return 2

    out_dir.mkdir(parents=True, exist_ok=True)
    stem = in_path.stem

    flat_path = out_dir / f"{stem}_flattened.csv"
    summary_path = out_dir / f"{stem}_tier_summary.csv"

    flat, tier_label_states = flatten_workbook(in_path)
    summary = build_tier_summary(flat, tier_label_states)

    flat.to_csv(flat_path, index=False)
    summary.to_csv(summary_path, index=False)

    print(f"Wrote: {flat_path}")
    print(f"Wrote: {summary_path}")
    print(f"Distinct tiers: {flat['tier'].nunique()}")
    print("Geo type counts:")
    print(flat["geo_type"].value_counts().to_string())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
