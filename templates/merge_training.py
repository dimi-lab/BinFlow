#!/usr/bin/env python3

import glob
import pandas as pd
import os

# Find all .tsv files in the current directory
tsv_files = glob.glob("*.tsv")
if not tsv_files:
    print("No .tsv files found in work directory!", file=sys.stderr)
    exit(1)

# Read and merge all tables
dfs = [pd.read_csv(f, sep='\\t') for f in tsv_files]
merged = pd.concat(dfs, axis=0, join='outer', ignore_index=True, sort=False)
merged.fillna('NA', inplace=True)

# Write output
outname = "${mark}_all.tsv"
merged.to_csv(outname, sep='\\t', index=False)
print(f"Merged {len(tsv_files)} files into {outname}")

