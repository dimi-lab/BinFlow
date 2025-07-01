#!/usr/bin/env python3
import os
import pandas as pd

# --- Configuration ---
file_list = "${tables_collected}".split(' ')
summary_table = "label_counts.tsv"   # Output TSV file with total label count per file
label_column = "${params.singleLabelColumn}"  # The column name containing the labels

summary_data = []  # List to store the summary counts

for file_path in file_list:
    # Read the file into a DataFrame
    try:
        df = pd.read_csv(file_path, sep="\t", low_memory=False)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        continue

    # Check if the label column exists; skip file if not found
    if label_column not in df.columns:
        print(f"Skipping {file_path}: '{label_column}' column not found.")
        summary_data.append({"file": os.path.basename(file_path), "label_count": 0})
        continue

    # Split each cell by "|" and remove any extra whitespace, then explode the list into individual rows
    all_values = df[label_column].dropna().apply(lambda x: [val.strip() for val in str(x).split("|")]).explode()
    # Add summary count for this file
    summary_data.append({"file": os.path.basename(file_path), "label_count": int(all_values.shape[0])})

# --- Create a summary DataFrame for total label counts ---
summary_df = pd.DataFrame(summary_data)
summary_df.to_csv(summary_table, sep="\t", index=False)
print(f"Total label counts saved to '{summary_table}'")

