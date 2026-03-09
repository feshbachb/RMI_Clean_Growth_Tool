#!/usr/bin/env python3
"""
split_release_data.py
=====================
Reads the large industry_complexity_pairs files (from the GitHub release
and from the repo data folder) and splits them into small, dashboard-friendly
CSV files organised by industry and by geography.

Output structure
----------------
  {output_dir}/
    by_industry/{geo_level_name}/{industry_code}.csv.gz
    by_geography/{geo_level_name}/{geoid}.csv.gz
    meta/
      geography_specific.csv        (all levels combined)
      industry_titles.csv
      industry_specific.csv         (all levels combined)
      industry_space_nodes.csv      (all levels combined)
      industry_space_edges.csv      (all levels combined)
      crosswalk.csv
      geo_aggregation_levels.csv
      state_aggregated_eci.csv

Usage
-----
    python3 split_release_data.py

The script auto-detects the data folder (lightcast_*) in the repo root.
"""

import os
import sys
import glob
import time
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))

# Auto-detect the data folder
data_folders = sorted(
    glob.glob(os.path.join(REPO_ROOT, "lightcast_*_complexity_naics6d_*")),
    reverse=True,
)
if not data_folders:
    sys.exit("ERROR: No lightcast data folder found in repo root.")

DATA_DIR = data_folders[0]
FOLDER_NAME = os.path.basename(DATA_DIR)

# Extract timestamp from folder name for the output directory
# e.g. lightcast_2024_complexity_naics6d_v2_4_20260308_204924
TIMESTAMP = FOLDER_NAME.split("_")[-2] + "_" + FOLDER_NAME.split("_")[-1]
OUTPUT_DIR = os.path.join(REPO_ROOT, f"dashboard_data_{TIMESTAMP}")

GEO_LEVEL_NAMES = {
    1: "county",
    2: "state",
    3: "cbsa",
    4: "csa",
    5: "cz",
}

# Pairs files: keyed by geo_level name, maps to filename in DATA_DIR
PAIRS_FILES = {
    "county": "county_industry_complexity_pairs",
    "state": "state_industry_complexity_pairs",
    "cbsa": "cbsa_industry_complexity_pairs",
    "csa": "csa_industry_complexity_pairs",
    "cz": "cz_industry_complexity_pairs",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def read_data(basename: str, directory: str = DATA_DIR) -> pd.DataFrame | None:
    """Try parquet first, then csv.gz, then csv."""
    for ext in [".parquet", ".csv.gz", ".csv"]:
        path = os.path.join(directory, basename + ext)
        if os.path.exists(path):
            print(f"  Reading {os.path.basename(path)} ...")
            if ext == ".parquet":
                return pd.read_parquet(path)
            else:
                return pd.read_csv(path)
    print(f"  WARNING: {basename} not found in {directory}")
    return None


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    t0 = time.time()
    print(f"Data folder: {DATA_DIR}")
    print(f"Output dir:  {OUTPUT_DIR}")
    print()

    ensure_dir(OUTPUT_DIR)

    # ---- 1. Copy meta / reference tables ----
    print("=== Copying meta/reference tables ===")
    meta_dir = os.path.join(OUTPUT_DIR, "meta")
    ensure_dir(meta_dir)

    meta_files = {
        "geography_specific_data": "geography_specific.csv",
        "industry_titles": "industry_titles.csv",
        "industry_specific_data": "industry_specific.csv",
        "industry_space_nodes": "industry_space_nodes.csv",
        "industry_space_edges": "industry_space_edges.csv",
        "county_state_cbsa_csa_cz_crosswalk": "crosswalk.csv",
        "geo_aggregation_levels": "geo_aggregation_levels.csv",
        "state_aggregated_eci": "state_aggregated_eci.csv",
    }

    # Also copy per-level geography-specific files
    for level_name in GEO_LEVEL_NAMES.values():
        key = f"{level_name}_geography_specific"
        meta_files[key] = f"{level_name}_geography_specific.csv"

    # Also copy per-level industry-specific files
    for level_name in GEO_LEVEL_NAMES.values():
        key = f"{level_name}_industry_specific"
        meta_files[key] = f"{level_name}_industry_specific.csv"

    for src_base, dst_name in meta_files.items():
        df = read_data(src_base)
        if df is not None:
            dst = os.path.join(meta_dir, dst_name)
            df.to_csv(dst, index=False)
            print(f"    -> {dst_name} ({len(df)} rows)")

    # ---- 2. Split pairs by industry and by geography ----
    print()
    print("=== Splitting complexity pairs ===")

    for level_name, pairs_basename in PAIRS_FILES.items():
        print(f"\n--- {level_name.upper()} level ---")
        df = read_data(pairs_basename)
        if df is None:
            print(f"  Skipping {level_name}: no data found")
            continue

        print(f"  Total rows: {len(df):,}")

        # Ensure string types for codes
        df["industry_code"] = df["industry_code"].astype(str)
        df["geoid"] = df["geoid"].astype(str)

        # Round floats to reduce file size
        float_cols = df.select_dtypes(include=["float64", "float32"]).columns
        for col in float_cols:
            df[col] = df[col].round(6)

        # Drop geo_aggregation_level (redundant - encoded in folder path)
        df = df.drop(columns=["geo_aggregation_level"], errors="ignore")

        # -- Split by industry --
        ind_dir = os.path.join(OUTPUT_DIR, "by_industry", level_name)
        ensure_dir(ind_dir)

        industries = df["industry_code"].unique()
        print(f"  Splitting by industry ({len(industries)} industries) ...")
        for i, ind_code in enumerate(industries):
            subset = df[df["industry_code"] == ind_code]
            subset.to_csv(
                os.path.join(ind_dir, f"{ind_code}.csv.gz"),
                index=False,
                compression="gzip",
            )
            if (i + 1) % 200 == 0:
                print(f"    {i + 1}/{len(industries)} industries written")
        print(f"    Done: {len(industries)} files -> {ind_dir}")

        # -- Split by geography --
        geo_dir = os.path.join(OUTPUT_DIR, "by_geography", level_name)
        ensure_dir(geo_dir)

        geoids = df["geoid"].unique()
        print(f"  Splitting by geography ({len(geoids)} geographies) ...")
        for i, geoid in enumerate(geoids):
            subset = df[df["geoid"] == geoid]
            subset.to_csv(
                os.path.join(geo_dir, f"{geoid}.csv.gz"),
                index=False,
                compression="gzip",
            )
            if (i + 1) % 500 == 0:
                print(f"    {i + 1}/{len(geoids)} geographies written")
        print(f"    Done: {len(geoids)} files -> {geo_dir}")

    # ---- 3. Summary ----
    elapsed = time.time() - t0
    print(f"\n=== Done in {elapsed:.1f}s ===")

    # Count output files
    total_files = 0
    total_size = 0
    for root, dirs, files in os.walk(OUTPUT_DIR):
        for f in files:
            total_files += 1
            total_size += os.path.getsize(os.path.join(root, f))
    print(f"Total files: {total_files:,}")
    print(f"Total size:  {total_size / 1e6:.1f} MB")
    print(f"Output dir:  {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
