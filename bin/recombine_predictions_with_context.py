#!/usr/bin/env python3
import argparse
from pathlib import Path
import pandas as pd


def normalize_cols(df):
    mapping = {c: str(c).strip() for c in df.columns}
    return df.rename(columns=mapping)


def find_key_cols(df):
    candidates = ["Image", "Centroid X µm", "Centroid Y µm", "Centroid X um", "Centroid Y um"]
    cols = []
    for c in candidates:
        if c in df.columns:
            cols.append(c)
    if "Centroid X um" in cols and "Centroid X µm" not in cols:
        df = df.rename(columns={"Centroid X um": "Centroid X µm"})
        cols = ["Image" if x == "Image" else ("Centroid X µm" if x == "Centroid X um" else x) for x in cols]
    if "Centroid Y um" in cols and "Centroid Y µm" not in cols:
        df = df.rename(columns={"Centroid Y um": "Centroid Y µm"})
        cols = ["Image" if x == "Image" else ("Centroid Y µm" if x == "Centroid Y um" else x) for x in cols]
    key_cols = [k for k in ["Image", "Centroid X µm", "Centroid Y µm"] if k in df.columns]
    return df, key_cols


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("merged_file")
    ap.add_argument("--context-columns", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("context_tables", nargs="+")
    args = ap.parse_args()

    kept_cols = [c.strip() for c in args.context_columns.split(",") if c.strip()]

    merged = pd.read_csv(args.merged_file, sep='\t', low_memory=False)
    merged = normalize_cols(merged)
    merged, key_cols = find_key_cols(merged)
    if not key_cols:
        merged.to_csv(args.output, sep='\t', index=False)
        return

    context_frames = []
    for f in args.context_tables:
        p = Path(f)
        if not p.exists():
            continue
        try:
            df = pd.read_csv(p, sep='\t', low_memory=False)
        except Exception:
            continue
        df = normalize_cols(df)
        df, df_keys = find_key_cols(df)
        if not all(k in df.columns for k in key_cols):
            continue
        select = [c for c in kept_cols if c in df.columns]
        use_cols = list(dict.fromkeys(key_cols + select))
        context_frames.append(df[use_cols])

    if context_frames:
        context = pd.concat(context_frames, ignore_index=True, sort=False)
        context = context.drop_duplicates(subset=key_cols)
        merged = merged.merge(context, on=key_cols, how='left')

    ordered = key_cols + [c for c in kept_cols if c in merged.columns and c not in key_cols] + [c for c in merged.columns if c not in key_cols and c not in kept_cols]
    merged = merged[ordered]
    merged.to_csv(args.output, sep='\t', index=False)


if __name__ == '__main__':
    main()
