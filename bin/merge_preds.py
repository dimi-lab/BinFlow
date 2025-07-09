#!/usr/bin/env python3

import pandas as pd
import re
import sys
from functools import reduce

image_id = sys.argv[1]
pred_files = sys.argv[2:]

# Read all prediction files
dfs = [pd.read_csv(f, sep='\t') for f in pred_files]

# Rename the 4th column in each DataFrame to a unique prediction column
for df in dfs:
    base = re.sub(r'[+-]', '', str(df.iloc[0, 3]))
    new_col = f'Prediction_{base}'
    df.rename(columns={df.columns[3]: new_col}, inplace=True)

# Merge all DataFrames on the first 3 columns
merged = reduce(lambda left, right: pd.merge(left, right, on=list(left.columns[:3]), how='outer'), dfs)

# Save merged output
merged.to_csv(f'{image_id}_MERGED.tsv', sep='\t', index=False)