import argparse
import datetime as dt
import os
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd


SCHEMA_RULES = {
    "probability_range": (0.0, 1.0),
    "ev_range": (-100.0, 100.0),
}


def collect_files(base_dir: Path, patterns: List[str], start_date: str = None, end_date: str = None) -> List[Path]:
    files: List[Path] = []
    for p in patterns:
        files += list(base_dir.glob(f"{p}_*.csv"))
    files = sorted(files)
    # Optional date filtering
    if start_date or end_date:
        s = dt.datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else dt.date.min
        e = dt.datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else dt.date.max
        filtered: List[Path] = []
        for f in files:
            try:
                dstr = f.stem.split("_")[-1]
                d = dt.datetime.strptime(dstr, "%Y-%m-%d").date()
                if s <= d <= e:
                    filtered.append(f)
            except Exception:
                continue
        files = filtered
    return files


def check_schema_consistency(files: List[Path]) -> Dict:
    col_sets: List[set] = []
    dtypes_map: Dict[str, List[str]] = {}
    issues: List[str] = []

    for f in files:
        try:
            df = pd.read_csv(f)
        except Exception as ex:
            issues.append(f"Failed to read {f}: {ex}")
            continue
        cols = set(df.columns)
        col_sets.append(cols)
        for c in df.columns:
            dtypes_map.setdefault(c, []).append(str(df[c].dtype))
        # Range checks
        for c in df.columns:
            if c.lower().startswith("prob") or c.lower().endswith("_prob"):
                if pd.api.types.is_numeric_dtype(df[c]):
                    bad = df[(df[c] < SCHEMA_RULES["probability_range"][0]) | (df[c] > SCHEMA_RULES["probability_range"][1])]
                    if not bad.empty:
                        issues.append(f"Probability out of range in {f} column {c}: {len(bad)} rows")
            if c.lower() in {"ev", "edge", "expected_value"}:
                if pd.api.types.is_numeric_dtype(df[c]):
                    bad = df[(df[c] < SCHEMA_RULES["ev_range"][0]) | (df[c] > SCHEMA_RULES["ev_range"][1])]
                    if not bad.empty:
                        issues.append(f"EV out of plausible range in {f} column {c}: {len(bad)} rows")
        # Missing checks
        missing = df.isna().sum()
        for c, n in missing.items():
            if n > 0:
                issues.append(f"Missing values in {f} column {c}: {n}")
    # Column consistency summary
    common_cols = set.intersection(*col_sets) if col_sets else set()
    union_cols = set.union(*col_sets) if col_sets else set()
    dtype_variations = {c: sorted(set(v)) for c, v in dtypes_map.items() if len(set(v)) > 1}

    return {
        "files_checked": len(files),
        "common_columns": sorted(common_cols),
        "union_columns": sorted(union_cols),
        "dtype_variations": dtype_variations,
        "issues": issues,
    }


def main():
    parser = argparse.ArgumentParser(description="Validate simulated data schemas and ranges")
    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--outdir", type=str, default=str(Path("data/processed/metrics")))
    args = parser.parse_args()

    base_dir = Path("data/processed")
    os.makedirs(args.outdir, exist_ok=True)

    files = collect_files(base_dir, ["props_predictions", "recon_props"], args.start_date, args.end_date)
    report = check_schema_consistency(files)

    out_json = Path(args.outdir) / f"sim_data_validation_{dt.date.today().strftime('%Y-%m-%d')}.json"
    with open(out_json, "w", encoding="utf-8") as f:
        import json
        json.dump(report, f, indent=2)

    print(f"Validated {report['files_checked']} files; wrote report to {out_json}")


if __name__ == "__main__":
    main()
