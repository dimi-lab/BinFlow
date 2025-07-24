#!/usr/bin/env python3

import os
import re
import sys
from functools import reduce
import pandas as pd

image_id = sys.argv[1]
pred_files = sys.argv[2:]

key_cols = ["Image", "Centroid X µm", "Centroid Y µm"]
dfs = []
for i, f in enumerate(pred_files):
    df = pd.read_csv(f, sep='\t')
    # Extract marker name from filename
    marker_match = re.search(r'predictions_([A-Za-z0-9\-]+)\.pkl', os.path.basename(f))
    marker = marker_match.group(1) if marker_match else f"Unknown{i}"
    # Rename the prediction column
    if df.shape[1] < 4:
        pred_col_name = f'Prediction_{marker}_{i}'
        df[pred_col_name] = "NA"
    else:
        pred_col = df.columns[3]
        pred_col_name = f'Prediction_{marker}_{i}'
        df.rename(columns={pred_col: pred_col_name}, inplace=True)
    # Keep only key columns and prediction column
    keep_cols = [col for col in df.columns if col in key_cols] + [pred_col_name]
    df = df[keep_cols]
    dfs.append(df)

# Drop duplicate key rows
for i, df in enumerate(dfs):
    dupes = df.duplicated(subset=key_cols).sum()
    if dupes > 0:
        print(f"Warning: DF {i} has {dupes} duplicate key rows, dropping duplicates.")
        df = df.drop_duplicates(subset=key_cols)
    dfs[i] = df

# Try inner merge first
merged = reduce(lambda left, right: pd.merge(left, right, on=key_cols, how='inner'), dfs)
print(f"Merged shape: {merged.shape}")

# If you must use outer, check for mismatched keys
# merged = reduce(lambda left, right: pd.merge(left, right, on=key_cols, how='outer'), dfs)

# Save merged output
merged.to_csv(f'{image_id}_MERGED.tsv', sep='\t', index=False)