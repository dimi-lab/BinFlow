#!/usr/bin/env python3
import os
import sys
import pandas as pd

# --- Configuration ---
if len(sys.argv) < 4:
    print("Usage: binary_counter.py <output_table> <label_column> <file1> [<file2> ...]")
    sys.exit(1)

summary_table = sys.argv[1]
label_column = sys.argv[2]
file_list = sys.argv[3:]

summary_data = []  # List to store the summary counts

for file_path in file_list:
    try:
        df = pd.read_csv(file_path, sep="\t", usecols=[label_column], low_memory=True)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        summary_data.append({"file": os.path.basename(file_path), "label_count": 0})
        continue

    if label_column not in df.columns:
        print(f"Skipping {file_path}: '{label_column}' column not found.")
        summary_data.append({"file": os.path.basename(file_path), "label_count": 0})
        continue

    # Split each cell by "|" and remove any extra whitespace, then explode the list into individual rows
    all_values = df[label_column].dropna().apply(lambda x: [val.strip() for val in str(x).split("|")]).explode()
    summary_data.append({"file": os.path.basename(file_path), "label_count": int(all_values.shape[0])})

# --- Create a summary DataFrame for total label counts ---
summary_df = pd.DataFrame(summary_data)
summary_df.to_csv(summary_table, sep="\t", index=False)
print(f"Total label counts saved to '{summary_table}'")

